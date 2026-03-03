use anyhow::{bail, Result};

use crate::query::ast::DurationUnit;

/// Tokens produced by the PulseQL lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    GroupBy,
    OrderBy,
    By,
    Fill,
    Limit,
    Offset,
    And,
    Or,
    As,
    Between,
    In,
    Time,
    Asc,
    Desc,
    Now,
    Group,
    Order,

    // Identifiers & literals
    Ident(String),
    StringLit(String),
    NumberLit(f64),
    IntLit(i64),
    DurationLit(u64, DurationUnit),
    RegexLit(String),

    // Punctuation & operators
    Comma,
    LParen,
    RParen,
    Star,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    RegexMatch,
    RegexNotMatch,
    Minus,

    // Special
    Eof,
}

/// Tokenizer for PulseQL queries.
pub struct Lexer {
    input: Vec<char>,
    pos: usize,
    peeked: Option<Token>,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Lexer {
            input: input.chars().collect(),
            pos: 0,
            peeked: None,
        }
    }

    /// Returns a reference to the next token without consuming it.
    pub fn peek(&mut self) -> Result<&Token> {
        if self.peeked.is_none() {
            self.peeked = Some(self.read_token()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }

    /// Consumes and returns the next token.
    pub fn next_token(&mut self) -> Result<Token> {
        if let Some(tok) = self.peeked.take() {
            return Ok(tok);
        }
        self.read_token()
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
            self.pos += 1;
        }
    }

    fn current(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.input.get(self.pos).copied();
        if ch.is_some() {
            self.pos += 1;
        }
        ch
    }

    fn read_token(&mut self) -> Result<Token> {
        self.skip_whitespace();

        let ch = match self.current() {
            Some(c) => c,
            None => return Ok(Token::Eof),
        };

        match ch {
            ',' => {
                self.advance();
                Ok(Token::Comma)
            }
            '(' => {
                self.advance();
                Ok(Token::LParen)
            }
            ')' => {
                self.advance();
                Ok(Token::RParen)
            }
            '*' => {
                self.advance();
                Ok(Token::Star)
            }
            '-' => {
                self.advance();
                Ok(Token::Minus)
            }
            '=' => {
                self.advance();
                if self.current() == Some('~') {
                    self.advance();
                    Ok(Token::RegexMatch)
                } else {
                    Ok(Token::Eq)
                }
            }
            '!' => {
                self.advance();
                match self.current() {
                    Some('=') => {
                        self.advance();
                        Ok(Token::Neq)
                    }
                    Some('~') => {
                        self.advance();
                        Ok(Token::RegexNotMatch)
                    }
                    _ => bail!("unexpected character after '!', expected '=' or '~'"),
                }
            }
            '>' => {
                self.advance();
                if self.current() == Some('=') {
                    self.advance();
                    Ok(Token::Gte)
                } else {
                    Ok(Token::Gt)
                }
            }
            '<' => {
                self.advance();
                if self.current() == Some('=') {
                    self.advance();
                    Ok(Token::Lte)
                } else {
                    Ok(Token::Lt)
                }
            }
            '\'' => self.read_string(),
            '/' => self.read_regex(),
            c if c.is_ascii_digit() => self.read_number(),
            c if c.is_ascii_alphabetic() || c == '_' => self.read_ident_or_keyword(),
            other => bail!("unexpected character: '{other}'"),
        }
    }

    fn read_string(&mut self) -> Result<Token> {
        self.advance(); // consume opening quote
        let mut s = String::new();
        loop {
            match self.advance() {
                Some('\'') => break,
                Some('\\') => match self.advance() {
                    Some(c) => s.push(c),
                    None => bail!("unexpected end of input in string escape"),
                },
                Some(c) => s.push(c),
                None => bail!("unterminated string literal"),
            }
        }
        Ok(Token::StringLit(s))
    }

    fn read_regex(&mut self) -> Result<Token> {
        self.advance(); // consume opening /
        let mut pattern = String::new();
        loop {
            match self.advance() {
                Some('/') => break,
                Some('\\') => {
                    pattern.push('\\');
                    match self.advance() {
                        Some(c) => pattern.push(c),
                        None => bail!("unexpected end of input in regex escape"),
                    }
                }
                Some(c) => pattern.push(c),
                None => bail!("unterminated regex literal"),
            }
        }
        Ok(Token::RegexLit(pattern))
    }

    fn read_number(&mut self) -> Result<Token> {
        let start = self.pos;
        let mut has_dot = false;

        while let Some(c) = self.current() {
            if c.is_ascii_digit() {
                self.advance();
            } else if c == '.' && !has_dot {
                has_dot = true;
                self.advance();
            } else {
                break;
            }
        }

        // Check for duration suffix
        if let Some(c) = self.current() {
            if c.is_ascii_alphabetic() && !has_dot {
                let num_str: String = self.input[start..self.pos].iter().collect();
                let value: u64 = num_str.parse()?;
                let unit = self.read_duration_unit()?;
                return Ok(Token::DurationLit(value, unit));
            }
        }

        let num_str: String = self.input[start..self.pos].iter().collect();
        if has_dot {
            let value: f64 = num_str.parse()?;
            Ok(Token::NumberLit(value))
        } else {
            let value: i64 = num_str.parse()?;
            Ok(Token::IntLit(value))
        }
    }

    fn read_duration_unit(&mut self) -> Result<DurationUnit> {
        let start = self.pos;
        while let Some(c) = self.current() {
            if c.is_ascii_alphabetic() {
                self.advance();
            } else {
                break;
            }
        }
        let suffix: String = self.input[start..self.pos].iter().collect();
        match suffix.as_str() {
            "ns" => Ok(DurationUnit::Nanoseconds),
            "us" | "µs" => Ok(DurationUnit::Microseconds),
            "ms" => Ok(DurationUnit::Milliseconds),
            "s" => Ok(DurationUnit::Seconds),
            "m" => Ok(DurationUnit::Minutes),
            "h" => Ok(DurationUnit::Hours),
            "d" => Ok(DurationUnit::Days),
            "w" => Ok(DurationUnit::Weeks),
            _ => bail!("unknown duration unit: '{suffix}'"),
        }
    }

    fn read_ident_or_keyword(&mut self) -> Result<Token> {
        let start = self.pos;
        while let Some(c) = self.current() {
            if c.is_ascii_alphanumeric() || c == '_' {
                self.advance();
            } else {
                break;
            }
        }

        let word: String = self.input[start..self.pos].iter().collect();
        let lower = word.to_ascii_lowercase();

        let token = match lower.as_str() {
            "select" => Token::Select,
            "from" => Token::From,
            "where" => Token::Where,
            "group" => Token::Group,
            "order" => Token::Order,
            "by" => Token::By,
            "fill" => Token::Fill,
            "limit" => Token::Limit,
            "offset" => Token::Offset,
            "and" => Token::And,
            "or" => Token::Or,
            "as" => Token::As,
            "between" => Token::Between,
            "in" => Token::In,
            "time" => Token::Time,
            "asc" => Token::Asc,
            "desc" => Token::Desc,
            "now" => Token::Now,
            _ => Token::Ident(word),
        };
        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokenize(input: &str) -> Result<Vec<Token>> {
        let mut lexer = Lexer::new(input);
        let mut tokens = Vec::new();
        loop {
            let tok = lexer.next_token()?;
            if tok == Token::Eof {
                break;
            }
            tokens.push(tok);
        }
        Ok(tokens)
    }

    #[test]
    fn test_simple_select() {
        let tokens = tokenize("SELECT * FROM cpu").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Select, Token::Star, Token::From, Token::Ident("cpu".into())]
        );
    }

    #[test]
    fn test_case_insensitive_keywords() {
        let tokens = tokenize("select FROM Where").unwrap();
        assert_eq!(tokens, vec![Token::Select, Token::From, Token::Where]);
    }

    #[test]
    fn test_string_literal() {
        let tokens = tokenize("'server01'").unwrap();
        assert_eq!(tokens, vec![Token::StringLit("server01".into())]);
    }

    #[test]
    fn test_string_with_escape() {
        let tokens = tokenize("'it\\'s'").unwrap();
        assert_eq!(tokens, vec![Token::StringLit("it's".into())]);
    }

    #[test]
    fn test_number_literals() {
        let tokens = tokenize("42 3.14").unwrap();
        assert_eq!(tokens, vec![Token::IntLit(42), Token::NumberLit(3.14)]);
    }

    #[test]
    fn test_duration_literals() {
        let tokens = tokenize("5m 1h 30s 100ms 2d 1w 500us 10ns").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::DurationLit(5, DurationUnit::Minutes),
                Token::DurationLit(1, DurationUnit::Hours),
                Token::DurationLit(30, DurationUnit::Seconds),
                Token::DurationLit(100, DurationUnit::Milliseconds),
                Token::DurationLit(2, DurationUnit::Days),
                Token::DurationLit(1, DurationUnit::Weeks),
                Token::DurationLit(500, DurationUnit::Microseconds),
                Token::DurationLit(10, DurationUnit::Nanoseconds),
            ]
        );
    }

    #[test]
    fn test_operators() {
        let tokens = tokenize("= != > < >= <= =~ !~").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Eq,
                Token::Neq,
                Token::Gt,
                Token::Lt,
                Token::Gte,
                Token::Lte,
                Token::RegexMatch,
                Token::RegexNotMatch,
            ]
        );
    }

    #[test]
    fn test_punctuation() {
        let tokens = tokenize(", ( ) * -").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Comma, Token::LParen, Token::RParen, Token::Star, Token::Minus]
        );
    }

    #[test]
    fn test_regex_literal() {
        let tokens = tokenize("/web-\\d+/").unwrap();
        assert_eq!(tokens, vec![Token::RegexLit("web-\\d+".into())]);
    }

    #[test]
    fn test_identifiers() {
        let tokens = tokenize("usage_idle host_name cpu01").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("usage_idle".into()),
                Token::Ident("host_name".into()),
                Token::Ident("cpu01".into()),
            ]
        );
    }

    #[test]
    fn test_full_query() {
        let tokens =
            tokenize("SELECT mean(usage) FROM cpu WHERE host = 'a' AND time > now() - 1h")
                .unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Ident("mean".into()),
                Token::LParen,
                Token::Ident("usage".into()),
                Token::RParen,
                Token::From,
                Token::Ident("cpu".into()),
                Token::Where,
                Token::Ident("host".into()),
                Token::Eq,
                Token::StringLit("a".into()),
                Token::And,
                Token::Time,
                Token::Gt,
                Token::Now,
                Token::LParen,
                Token::RParen,
                Token::Minus,
                Token::DurationLit(1, DurationUnit::Hours),
            ]
        );
    }

    #[test]
    fn test_group_by_keywords() {
        let tokens = tokenize("GROUP BY time(5m)").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Group,
                Token::By,
                Token::Time,
                Token::LParen,
                Token::DurationLit(5, DurationUnit::Minutes),
                Token::RParen,
            ]
        );
    }

    #[test]
    fn test_order_by_keywords() {
        let tokens = tokenize("ORDER BY time DESC").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Order, Token::By, Token::Time, Token::Desc]
        );
    }

    #[test]
    fn test_peek_does_not_consume() {
        let mut lexer = Lexer::new("SELECT *");
        let peeked = lexer.peek().unwrap().clone();
        assert_eq!(peeked, Token::Select);
        let next = lexer.next_token().unwrap();
        assert_eq!(next, Token::Select);
        let next = lexer.next_token().unwrap();
        assert_eq!(next, Token::Star);
    }

    #[test]
    fn test_unterminated_string() {
        let result = tokenize("'unterminated");
        assert!(result.is_err());
    }

    #[test]
    fn test_unterminated_regex() {
        let result = tokenize("/unterminated");
        assert!(result.is_err());
    }

    #[test]
    fn test_unexpected_char_after_bang() {
        let result = tokenize("!x");
        assert!(result.is_err());
    }

    #[test]
    fn test_between_keyword() {
        let tokens = tokenize("BETWEEN").unwrap();
        assert_eq!(tokens, vec![Token::Between]);
    }

    #[test]
    fn test_limit_offset() {
        let tokens = tokenize("LIMIT 100 OFFSET 50").unwrap();
        assert_eq!(
            tokens,
            vec![Token::Limit, Token::IntLit(100), Token::Offset, Token::IntLit(50)]
        );
    }

    #[test]
    fn test_fill_keyword() {
        let tokens = tokenize("FILL(linear)").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Fill,
                Token::LParen,
                Token::Ident("linear".into()),
                Token::RParen,
            ]
        );
    }

    #[test]
    fn test_as_alias() {
        let tokens = tokenize("mean(usage) AS avg_usage").unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Ident("mean".into()),
                Token::LParen,
                Token::Ident("usage".into()),
                Token::RParen,
                Token::As,
                Token::Ident("avg_usage".into()),
            ]
        );
    }
}

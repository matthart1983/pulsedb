use anyhow::{bail, Result};

use crate::query::ast::DurationUnit;

/// Tokens produced by the PulseLang lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Literals
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    BoolVec(Vec<bool>),
    Symbol(String),
    Str(String),
    Timestamp(String),
    Duration(u64, DurationUnit),
    Null(Option<char>),

    // Identifiers
    Ident(String),

    // Operators
    Plus,
    Minus,
    Star,
    Percent,
    Caret,
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    Tilde,
    Amp,
    Pipe,
    Bang,
    Hash,
    Underscore,
    Question,
    At,
    Dot,
    Comma,
    Colon,
    Semi,

    // Iterators
    Quote,
    QuoteColon,
    Slash,
    Backslash,

    // Pipeline
    PipeArrow,

    // Brackets
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,

    // Conditional
    Dollar,

    Eof,
}

/// Source position for error reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub offset: usize,
    pub line: usize,   // 1-based
    pub col: usize,    // 1-based
}

/// Tokenizer for PulseLang expressions.
pub struct Lexer {
    input: Vec<char>,
    pos: usize,
    line: usize,
    col: usize,
    last_span: Span,
    peeked: Option<Token>,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Lexer {
            input: input.chars().collect(),
            pos: 0,
            line: 1,
            col: 1,
            last_span: Span { offset: 0, line: 1, col: 1 },
            peeked: None,
        }
    }

    pub fn span(&self) -> Span {
        Span {
            offset: self.pos,
            line: self.line,
            col: self.col,
        }
    }

    pub fn last_span(&self) -> Span {
        self.last_span
    }

    pub fn peek(&mut self) -> Result<&Token> {
        if self.peeked.is_none() {
            self.peeked = Some(self.read_token()?);
        }
        Ok(self.peeked.as_ref().unwrap())
    }

    pub fn next_token(&mut self) -> Result<Token> {
        if let Some(tok) = self.peeked.take() {
            return Ok(tok);
        }
        self.read_token()
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while self.pos < self.input.len() && self.input[self.pos].is_ascii_whitespace() {
                if self.input[self.pos] == '\n' {
                    self.line += 1;
                    self.col = 1;
                } else {
                    self.col += 1;
                }
                self.pos += 1;
            }
            // Check for line comment: `/` at start of line or after whitespace
            if self.pos < self.input.len() && self.input[self.pos] == '/' {
                let is_comment = self.pos == 0
                    || self.input[self.pos - 1].is_ascii_whitespace()
                    || self.input[self.pos - 1] == '\n';
                if is_comment {
                    while self.pos < self.input.len() && self.input[self.pos] != '\n' {
                        self.col += 1;
                        self.pos += 1;
                    }
                    continue;
                }
            }
            break;
        }
    }

    fn current(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn peek_char(&self, offset: usize) -> Option<char> {
        self.input.get(self.pos + offset).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.input.get(self.pos).copied();
        if let Some(c) = ch {
            self.pos += 1;
            if c == '\n' {
                self.line += 1;
                self.col = 1;
            } else {
                self.col += 1;
            }
        }
        ch
    }

    fn read_token(&mut self) -> Result<Token> {
        self.skip_whitespace_and_comments();
        self.last_span = self.span();

        let ch = match self.current() {
            Some(c) => c,
            None => return Ok(Token::Eof),
        };

        match ch {
            '(' => { self.advance(); Ok(Token::LParen) }
            ')' => { self.advance(); Ok(Token::RParen) }
            '[' => { self.advance(); Ok(Token::LBracket) }
            ']' => { self.advance(); Ok(Token::RBracket) }
            '{' => { self.advance(); Ok(Token::LBrace) }
            '}' => { self.advance(); Ok(Token::RBrace) }
            ';' => { self.advance(); Ok(Token::Semi) }
            '+' => { self.advance(); Ok(Token::Plus) }
            '*' => { self.advance(); Ok(Token::Star) }
            '%' => { self.advance(); Ok(Token::Percent) }
            '^' => { self.advance(); Ok(Token::Caret) }
            '~' => { self.advance(); Ok(Token::Tilde) }
            '#' => { self.advance(); Ok(Token::Hash) }
            '?' => { self.advance(); Ok(Token::Question) }
            '@' => { self.advance(); Ok(Token::At) }
            ',' => { self.advance(); Ok(Token::Comma) }
            ':' => { self.advance(); Ok(Token::Colon) }
            '$' => { self.advance(); Ok(Token::Dollar) }
            '_' => {
                // Check if this starts an identifier (e.g., `_foo`)
                if self.peek_char(1).map_or(false, |c| c.is_ascii_alphanumeric()) {
                    self.read_ident()
                } else {
                    self.advance();
                    Ok(Token::Underscore)
                }
            }
            '!' => { self.advance(); Ok(Token::Bang) }
            '\\' => { self.advance(); Ok(Token::Backslash) }
            '/' => {
                // Not a comment (handled in skip), so this is the over operator
                self.advance();
                Ok(Token::Slash)
            }
            '\'' => {
                self.advance();
                if self.current() == Some(':') {
                    self.advance();
                    Ok(Token::QuoteColon)
                } else {
                    Ok(Token::Quote)
                }
            }
            '|' => {
                self.advance();
                if self.current() == Some('>') {
                    self.advance();
                    Ok(Token::PipeArrow)
                } else {
                    Ok(Token::Pipe)
                }
            }
            '&' => { self.advance(); Ok(Token::Amp) }
            '<' => {
                self.advance();
                match self.current() {
                    Some('>') => { self.advance(); Ok(Token::Neq) }
                    Some('=') => { self.advance(); Ok(Token::Lte) }
                    _ => Ok(Token::Lt),
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
            '=' => { self.advance(); Ok(Token::Eq) }
            '-' => { self.advance(); Ok(Token::Minus) }
            '.' => {
                // Could be a float starting with `.` like `.5`, or member access
                if self.peek_char(1).map_or(false, |c| c.is_ascii_digit()) {
                    self.read_number_from(self.pos)
                } else {
                    self.advance();
                    Ok(Token::Dot)
                }
            }
            '"' => self.read_string(),
            '`' => self.read_symbol(),
            c if c.is_ascii_digit() => self.read_number(),
            c if c.is_ascii_alphabetic() || c == '_' => self.read_ident(),
            other => bail!("unexpected character: '{other}'"),
        }
    }

    fn read_string(&mut self) -> Result<Token> {
        self.advance(); // consume opening "
        let mut s = String::new();
        loop {
            match self.advance() {
                Some('"') => break,
                Some('\\') => match self.advance() {
                    Some('n') => s.push('\n'),
                    Some('t') => s.push('\t'),
                    Some('\\') => s.push('\\'),
                    Some('"') => s.push('"'),
                    Some(c) => { s.push('\\'); s.push(c); }
                    None => bail!("unexpected end of input in string escape"),
                },
                Some(c) => s.push(c),
                None => bail!("unterminated string literal"),
            }
        }
        Ok(Token::Str(s))
    }

    fn read_symbol(&mut self) -> Result<Token> {
        self.advance(); // consume `
        let mut s = String::new();
        while let Some(c) = self.current() {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.' || c == '/' {
                s.push(c);
                self.advance();
            } else {
                break;
            }
        }
        Ok(Token::Symbol(s))
    }

    fn read_number(&mut self) -> Result<Token> {
        let start = self.pos;

        // Check for 0N (null)
        if self.input[self.pos] == '0' && self.peek_char(1) == Some('N') {
            self.advance(); // 0
            self.advance(); // N
            let qualifier = match self.current() {
                Some(c @ ('i' | 'f' | 't' | 'u')) => {
                    self.advance();
                    Some(c)
                }
                _ => None,
            };
            return Ok(Token::Null(qualifier));
        }

        // Check for 0n (NaN) and 0w (Inf)
        if self.input[self.pos] == '0' {
            if self.peek_char(1) == Some('n')
                && !self.peek_char(2).map_or(false, |c| c.is_ascii_alphanumeric())
            {
                self.advance();
                self.advance();
                return Ok(Token::Float(f64::NAN));
            }
            if self.peek_char(1) == Some('w')
                && !self.peek_char(2).map_or(false, |c| c.is_ascii_alphanumeric())
            {
                self.advance();
                self.advance();
                return Ok(Token::Float(f64::INFINITY));
            }
        }

        self.read_number_from(start)
    }

    fn read_number_from(&mut self, start: usize) -> Result<Token> {
        let mut has_dot = false;
        let mut has_e = false;

        while let Some(c) = self.current() {
            if c.is_ascii_digit() {
                self.advance();
            } else if c == '.' && !has_dot && !has_e {
                // Check for timestamp pattern: digits.digits.digitsD
                // e.g., 2024.01.15D14:30:00
                if self.is_timestamp_ahead(start) {
                    return self.read_timestamp(start);
                }
                has_dot = true;
                self.advance();
            } else if (c == 'e' || c == 'E') && !has_e {
                has_e = true;
                self.advance();
                if self.current() == Some('+') || self.current() == Some('-') {
                    self.advance();
                }
            } else {
                break;
            }
        }

        let num_str: String = self.input[start..self.pos].iter().collect();

        // Check for suffix
        match self.current() {
            Some('b') if !has_dot && !has_e => {
                self.advance();
                // Boolean vector: 10010b
                let bools: Vec<bool> = num_str.chars().map(|c| c == '1').collect();
                if bools.len() == 1 {
                    return Ok(Token::Bool(bools[0]));
                }
                return Ok(Token::BoolVec(bools));
            }
            Some(c) if c.is_ascii_alphabetic() && !has_dot && !has_e => {
                // Try duration first (handles `us`, `u` suffix after)
                let save_pos = self.pos;
                let value: u64 = num_str.parse()?;
                if let Ok(unit) = self.read_duration_unit() {
                    return Ok(Token::Duration(value, unit));
                }
                // Duration parse failed, check for u/i suffix
                self.pos = save_pos;
                if c == 'u' && !self.peek_char(1).map_or(false, |c2| c2.is_ascii_alphabetic()) {
                    self.advance();
                    return Ok(Token::UInt(value));
                }
                if c == 'i' && !self.peek_char(1).map_or(false, |c2| c2.is_ascii_alphabetic()) {
                    self.advance();
                    return Ok(Token::Int(value as i64));
                }
            }
            _ => {}
        }

        if has_dot || has_e {
            let value: f64 = num_str.parse()?;
            Ok(Token::Float(value))
        } else {
            let value: i64 = num_str.parse()?;
            Ok(Token::Int(value))
        }
    }

    fn is_timestamp_ahead(&self, start: usize) -> bool {
        // Look for pattern: digits.digits.digitsD
        let chars = &self.input[start..];
        let s: String = chars.iter().take(20).collect();
        // Simple heuristic: 4 digits, dot, 2 digits, dot, 2 digits, D
        if s.len() >= 11 {
            let bytes = s.as_bytes();
            bytes[4] == b'.' && bytes[7] == b'.' && bytes[10] == b'D'
        } else {
            false
        }
    }

    fn read_timestamp(&mut self, start: usize) -> Result<Token> {
        // Consume: YYYY.MM.DDDhh:mm:ss[.nnnnnnnnn]
        while let Some(c) = self.current() {
            if c.is_ascii_digit() || c == '.' || c == 'D' || c == ':' {
                self.advance();
            } else {
                break;
            }
        }
        let ts_str: String = self.input[start..self.pos].iter().collect();
        Ok(Token::Timestamp(ts_str))
    }

    fn read_duration_unit(&mut self) -> Result<DurationUnit> {
        let start = self.pos;
        let mut unit_str = String::new();
        while let Some(c) = self.current() {
            if c.is_ascii_alphabetic() {
                unit_str.push(c);
                self.advance();
            } else {
                break;
            }
        }
        match unit_str.as_str() {
            "ns" => Ok(DurationUnit::Nanoseconds),
            "us" => Ok(DurationUnit::Microseconds),
            "ms" => Ok(DurationUnit::Milliseconds),
            "s" => Ok(DurationUnit::Seconds),
            "m" => Ok(DurationUnit::Minutes),
            "h" => Ok(DurationUnit::Hours),
            "d" => Ok(DurationUnit::Days),
            "w" => Ok(DurationUnit::Weeks),
            _ => {
                self.pos = start;
                bail!("unknown duration unit: '{unit_str}'")
            }
        }
    }

    fn read_ident(&mut self) -> Result<Token> {
        let start = self.pos;
        while let Some(c) = self.current() {
            if c.is_ascii_alphanumeric() || c == '_' {
                self.advance();
            } else {
                break;
            }
        }
        let word: String = self.input[start..self.pos].iter().collect();

        // Check for true/false as boolean literals
        match word.as_str() {
            "true" => Ok(Token::Bool(true)),
            "false" => Ok(Token::Bool(false)),
            _ => Ok(Token::Ident(word)),
        }
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
    fn test_integers() {
        let tokens = tokenize("42 0 100").unwrap();
        assert_eq!(tokens, vec![Token::Int(42), Token::Int(0), Token::Int(100)]);
    }

    #[test]
    fn test_unsigned() {
        let tokens = tokenize("42u").unwrap();
        assert_eq!(tokens, vec![Token::UInt(42)]);
    }

    #[test]
    fn test_floats() {
        let tokens = tokenize("3.14 1e-5").unwrap();
        assert_eq!(tokens, vec![Token::Float(3.14), Token::Float(1e-5)]);
    }

    #[test]
    fn test_nan_inf() {
        let tokens = tokenize("0n 0w").unwrap();
        assert_eq!(tokens.len(), 2);
        assert!(tokens[0] == Token::Float(f64::NAN) || {
            if let Token::Float(v) = tokens[0] { v.is_nan() } else { false }
        });
        assert_eq!(tokens[1], Token::Float(f64::INFINITY));
    }

    #[test]
    fn test_booleans() {
        let tokens = tokenize("true false 1b 0b").unwrap();
        assert_eq!(tokens, vec![
            Token::Bool(true),
            Token::Bool(false),
            Token::Bool(true),
            Token::Bool(false),
        ]);
    }

    #[test]
    fn test_bool_vector() {
        let tokens = tokenize("10010b").unwrap();
        assert_eq!(tokens, vec![Token::BoolVec(vec![true, false, false, true, false])]);
    }

    #[test]
    fn test_symbols() {
        let tokens = tokenize("`host `us-east `a.b").unwrap();
        assert_eq!(tokens, vec![
            Token::Symbol("host".into()),
            Token::Symbol("us-east".into()),
            Token::Symbol("a.b".into()),
        ]);
    }

    #[test]
    fn test_strings() {
        let tokens = tokenize(r#""hello" "world\n""#).unwrap();
        assert_eq!(tokens, vec![
            Token::Str("hello".into()),
            Token::Str("world\n".into()),
        ]);
    }

    #[test]
    fn test_null_literals() {
        let tokens = tokenize("0N 0Ni 0Nf 0Nt").unwrap();
        assert_eq!(tokens, vec![
            Token::Null(None),
            Token::Null(Some('i')),
            Token::Null(Some('f')),
            Token::Null(Some('t')),
        ]);
    }

    #[test]
    fn test_durations() {
        let tokens = tokenize("5m 1h 30s 100ms 2d 1w 500us 10ns").unwrap();
        assert_eq!(tokens, vec![
            Token::Duration(5, DurationUnit::Minutes),
            Token::Duration(1, DurationUnit::Hours),
            Token::Duration(30, DurationUnit::Seconds),
            Token::Duration(100, DurationUnit::Milliseconds),
            Token::Duration(2, DurationUnit::Days),
            Token::Duration(1, DurationUnit::Weeks),
            Token::Duration(500, DurationUnit::Microseconds),
            Token::Duration(10, DurationUnit::Nanoseconds),
        ]);
    }

    #[test]
    fn test_timestamp() {
        let tokens = tokenize("2024.01.15D14:30:00").unwrap();
        assert_eq!(tokens, vec![Token::Timestamp("2024.01.15D14:30:00".into())]);
    }

    #[test]
    fn test_operators() {
        let tokens = tokenize("+ - * % ^ = <> < > <= >= ~ & | ! # ? @").unwrap();
        assert_eq!(tokens, vec![
            Token::Plus, Token::Minus, Token::Star, Token::Percent, Token::Caret,
            Token::Eq, Token::Neq, Token::Lt, Token::Gt, Token::Lte, Token::Gte,
            Token::Tilde, Token::Amp, Token::Pipe, Token::Bang, Token::Hash,
            Token::Question, Token::At,
        ]);
    }

    #[test]
    fn test_brackets() {
        let tokens = tokenize("( ) [ ] { }").unwrap();
        assert_eq!(tokens, vec![
            Token::LParen, Token::RParen,
            Token::LBracket, Token::RBracket,
            Token::LBrace, Token::RBrace,
        ]);
    }

    #[test]
    fn test_iterators() {
        let tokens = tokenize("' ':").unwrap();
        assert_eq!(tokens, vec![Token::Quote, Token::QuoteColon]);
    }

    #[test]
    fn test_pipeline() {
        let tokens = tokenize("|>").unwrap();
        assert_eq!(tokens, vec![Token::PipeArrow]);
    }

    #[test]
    fn test_pipe_vs_pipeline() {
        let tokens = tokenize("| |>").unwrap();
        assert_eq!(tokens, vec![Token::Pipe, Token::PipeArrow]);
    }

    #[test]
    fn test_identifiers() {
        let tokens = tokenize("avg sum cpu usage_idle xbar").unwrap();
        assert_eq!(tokens, vec![
            Token::Ident("avg".into()),
            Token::Ident("sum".into()),
            Token::Ident("cpu".into()),
            Token::Ident("usage_idle".into()),
            Token::Ident("xbar".into()),
        ]);
    }

    #[test]
    fn test_comment() {
        let tokens = tokenize("42\n/ this is a comment\n7").unwrap();
        assert_eq!(tokens, vec![Token::Int(42), Token::Int(7)]);
    }

    #[test]
    fn test_inline_comment() {
        let tokens = tokenize("42 / comment").unwrap();
        assert_eq!(tokens, vec![Token::Int(42)]);
    }

    #[test]
    fn test_assignment() {
        let tokens = tokenize("x: 42").unwrap();
        assert_eq!(tokens, vec![
            Token::Ident("x".into()),
            Token::Colon,
            Token::Int(42),
        ]);
    }

    #[test]
    fn test_member_access() {
        let tokens = tokenize("cpu.usage_idle").unwrap();
        assert_eq!(tokens, vec![
            Token::Ident("cpu".into()),
            Token::Dot,
            Token::Ident("usage_idle".into()),
        ]);
    }

    #[test]
    fn test_complex_expression() {
        let tokens = tokenize("avg cpu.usage_idle @ `host = `server01").unwrap();
        assert_eq!(tokens, vec![
            Token::Ident("avg".into()),
            Token::Ident("cpu".into()),
            Token::Dot,
            Token::Ident("usage_idle".into()),
            Token::At,
            Token::Symbol("host".into()),
            Token::Eq,
            Token::Symbol("server01".into()),
        ]);
    }

    #[test]
    fn test_lambda() {
        let tokens = tokenize("{x + 1}").unwrap();
        assert_eq!(tokens, vec![
            Token::LBrace,
            Token::Ident("x".into()),
            Token::Plus,
            Token::Int(1),
            Token::RBrace,
        ]);
    }

    #[test]
    fn test_dollar_conditional() {
        let tokens = tokenize("$[x > 0; x; neg x]").unwrap();
        assert_eq!(tokens, vec![
            Token::Dollar,
            Token::LBracket,
            Token::Ident("x".into()),
            Token::Gt,
            Token::Int(0),
            Token::Semi,
            Token::Ident("x".into()),
            Token::Semi,
            Token::Ident("neg".into()),
            Token::Ident("x".into()),
            Token::RBracket,
        ]);
    }

    #[test]
    fn test_unterminated_string() {
        let result = tokenize("\"unterminated");
        assert!(result.is_err());
    }

    #[test]
    fn test_peek_does_not_consume() {
        let mut lexer = Lexer::new("42 7");
        let peeked = lexer.peek().unwrap().clone();
        assert_eq!(peeked, Token::Int(42));
        let next = lexer.next_token().unwrap();
        assert_eq!(next, Token::Int(42));
        let next = lexer.next_token().unwrap();
        assert_eq!(next, Token::Int(7));
    }

    #[test]
    fn test_semicolons_in_list() {
        let tokens = tokenize("(1; \"a\"; `x)").unwrap();
        assert_eq!(tokens, vec![
            Token::LParen,
            Token::Int(1),
            Token::Semi,
            Token::Str("a".into()),
            Token::Semi,
            Token::Symbol("x".into()),
            Token::RParen,
        ]);
    }

    #[test]
    fn test_dict_construction() {
        let tokens = tokenize("`a`b ! 1 2").unwrap();
        assert_eq!(tokens, vec![
            Token::Symbol("a".into()),
            Token::Symbol("b".into()),
            Token::Bang,
            Token::Int(1),
            Token::Int(2),
        ]);
    }
}

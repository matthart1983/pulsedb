use anyhow::Result;

use crate::lang::ast::*;
use crate::lang::lexer::{Lexer, Token};

/// Recursive descent parser for PulseLang expressions.
pub struct Parser {
    lexer: Lexer,
    current: Token,
}

impl Parser {
    pub fn new(input: &str) -> Result<Self> {
        let mut lexer = Lexer::new(input);
        let current = lexer.next_token()?;
        Ok(Parser { lexer, current })
    }

    pub fn parse(mut self) -> Result<Expr> {
        let expr = self.parse_expr()?;
        if self.current != Token::Eof {
            return Err(self.error_at_current(&format!("unexpected token after expression: {:?}", self.current)));
        }
        Ok(expr)
    }

    pub fn parse_program(mut self) -> Result<Vec<Expr>> {
        let mut exprs = Vec::new();
        while self.current != Token::Eof {
            exprs.push(self.parse_expr()?);
        }
        if exprs.len() == 1 {
            return Ok(exprs);
        }
        Ok(exprs)
    }

    fn error_at_current(&self, msg: &str) -> anyhow::Error {
        let span = self.lexer.last_span();
        anyhow::anyhow!("{msg} at line {}, col {}", span.line, span.col)
    }

    fn advance(&mut self) -> Result<Token> {
        let prev = std::mem::replace(&mut self.current, Token::Eof);
        self.current = self.lexer.next_token()?;
        Ok(prev)
    }

    fn peek(&mut self) -> Result<&Token> {
        self.lexer.peek()
    }

    fn expect(&mut self, expected: &Token) -> Result<()> {
        if &self.current == expected {
            self.advance()?;
            Ok(())
        } else {
            Err(self.error_at_current(&format!("expected {:?}, got {:?}", expected, self.current)))
        }
    }

    fn expect_ident(&mut self) -> Result<String> {
        match &self.current {
            Token::Ident(name) => {
                let name = name.clone();
                self.advance()?;
                Ok(name)
            }
            other => Err(self.error_at_current(&format!("expected identifier, got {:?}", other))),
        }
    }

    fn at_expr_start(&self) -> bool {
        matches!(
            self.current,
            Token::Int(_)
                | Token::UInt(_)
                | Token::Float(_)
                | Token::Bool(_)
                | Token::BoolVec(_)
                | Token::Symbol(_)
                | Token::Str(_)
                | Token::Timestamp(_)
                | Token::Duration(..)
                | Token::Null(_)
                | Token::Ident(_)
                | Token::LParen
                | Token::LBrace
                | Token::Minus
                | Token::Dollar
        )
    }

    /// Parse a top-level expression: assignment or pipeline.
    fn parse_expr(&mut self) -> Result<Expr> {
        // Check for assignment: `name: expr`
        if let Token::Ident(name) = &self.current {
            let name = name.clone();
            if self.peek()? == &Token::Colon {
                self.advance()?; // consume ident
                self.advance()?; // consume :
                let value = self.parse_expr()?;
                return Ok(Expr::Assign {
                    name,
                    value: Box::new(value),
                });
            }
        }

        // Check for `select` expression
        if self.current == Token::Ident("select".to_string()) {
            return self.parse_select();
        }

        self.parse_pipeline()
    }

    /// Parse pipeline: `expr |> expr |> expr`
    fn parse_pipeline(&mut self) -> Result<Expr> {
        let mut expr = self.parse_right_to_left()?;
        while self.current == Token::PipeArrow {
            self.advance()?;
            let right = self.parse_right_to_left()?;
            expr = Expr::Pipe {
                left: Box::new(expr),
                right: Box::new(right),
            };
        }
        Ok(expr)
    }

    /// Parse right-to-left application (APL-style).
    /// Handles: `f x`, `x op y`, monadic functions, `@`, `within`, iterators.
    fn parse_right_to_left(&mut self) -> Result<Expr> {
        let atom = self.parse_postfix()?;

        // Check for binary operators
        match &self.current {
            Token::Plus => return self.parse_binop(atom, BinOp::Add),
            Token::Minus => {
                // Could be binary sub or the start of a negative literal
                // If followed by a number/ident, treat as binary
                if self.at_expr_start_after_minus() {
                    return self.parse_binop(atom, BinOp::Sub);
                }
            }
            Token::Star => return self.parse_binop(atom, BinOp::Mul),
            Token::Percent => return self.parse_binop(atom, BinOp::Div),
            Token::Caret => return self.parse_binop(atom, BinOp::Pow),
            Token::Eq => return self.parse_binop(atom, BinOp::Eq),
            Token::Neq => return self.parse_binop(atom, BinOp::Neq),
            Token::Lt => return self.parse_binop(atom, BinOp::Lt),
            Token::Gt => return self.parse_binop(atom, BinOp::Gt),
            Token::Lte => return self.parse_binop(atom, BinOp::Lte),
            Token::Gte => return self.parse_binop(atom, BinOp::Gte),
            Token::Tilde => return self.parse_binop(atom, BinOp::Match),
            Token::Amp => return self.parse_binop(atom, BinOp::And),
            Token::Pipe => return self.parse_binop(atom, BinOp::Or),
            Token::Comma => return self.parse_binop(atom, BinOp::Join),
            Token::Hash => return self.parse_binop(atom, BinOp::Take),
            Token::Underscore => return self.parse_binop(atom, BinOp::Drop),
            Token::Question => return self.parse_binop(atom, BinOp::Find),
            Token::Ident(s) if s == "mod" => {
                self.advance()?;
                let right = self.parse_right_to_left()?;
                return Ok(Expr::BinOp {
                    op: BinOp::Mod,
                    left: Box::new(atom),
                    right: Box::new(right),
                });
            }
            Token::Ident(s) if s == "in" => {
                self.advance()?;
                let right = self.parse_right_to_left()?;
                return Ok(Expr::BinOp {
                    op: BinOp::In,
                    left: Box::new(atom),
                    right: Box::new(right),
                });
            }
            Token::Ident(s) if s == "like" => {
                self.advance()?;
                let right = self.parse_right_to_left()?;
                return Ok(Expr::BinOp {
                    op: BinOp::Like,
                    left: Box::new(atom),
                    right: Box::new(right),
                });
            }
            Token::Ident(s) if s == "by" => {
                // `avg x by g` — we don't handle this here; it's part of select
                // Leave it for the caller
            }
            _ => {}
        }

        // Check for `!` as dict constructor
        if self.current == Token::Bang {
            self.advance()?;
            let values = self.parse_right_to_left()?;
            return Ok(Expr::Dict {
                keys: Box::new(atom),
                values: Box::new(values),
            });
        }

        // Check for `@` (tag filter)
        if self.current == Token::At {
            self.advance()?;
            let pred = self.parse_tag_predicate()?;
            let mut result = Expr::TagFilter {
                source: Box::new(atom),
                predicate: Box::new(pred),
            };
            // Check for chained `within`
            if self.current == Token::Ident("within".to_string()) {
                self.advance()?;
                self.expect(&Token::LParen)?;
                let start = self.parse_right_to_left()?;
                self.expect(&Token::Semi)?;
                let end = self.parse_right_to_left()?;
                self.expect(&Token::RParen)?;
                result = Expr::Within {
                    source: Box::new(result),
                    start: Box::new(start),
                    end: Box::new(end),
                };
            }
            return Ok(result);
        }

        // Check for `within` (time range)
        if self.current == Token::Ident("within".to_string()) {
            self.advance()?;
            self.expect(&Token::LParen)?;
            let start = self.parse_right_to_left()?;
            self.expect(&Token::Semi)?;
            let end = self.parse_right_to_left()?;
            self.expect(&Token::RParen)?;
            return Ok(Expr::Within {
                source: Box::new(atom),
                start: Box::new(start),
                end: Box::new(end),
            });
        }

        Ok(atom)
    }

    fn at_expr_start_after_minus(&self) -> bool {
        // Check if the token after minus looks like it starts an expression
        // This is a heuristic: minus is binary if preceded by a value-producing expression
        // For now, always treat as binary when we have a left operand
        true
    }

    fn parse_binop(&mut self, left: Expr, op: BinOp) -> Result<Expr> {
        self.advance()?; // consume operator
        let right = self.parse_right_to_left()?;
        Ok(Expr::BinOp {
            op,
            left: Box::new(left),
            right: Box::new(right),
        })
    }

    /// Parse postfix operations: indexing, member access, iterators.
    fn parse_postfix(&mut self) -> Result<Expr> {
        let mut expr = self.parse_atom()?;

        loop {
            match &self.current {
                Token::Dot => {
                    self.advance()?;
                    let field = self.expect_ident()?;
                    expr = Expr::Member {
                        object: Box::new(expr),
                        field,
                    };
                }
                Token::LBracket => {
                    self.advance()?;
                    if self.current == Token::RBracket {
                        // Niladic call: `now[]`
                        self.advance()?;
                        expr = Expr::Apply {
                            func: Box::new(expr),
                            args: vec![],
                        };
                    } else {
                        let mut args = vec![self.parse_right_to_left()?];
                        while self.current == Token::Semi {
                            self.advance()?;
                            args.push(self.parse_right_to_left()?);
                        }
                        self.expect(&Token::RBracket)?;
                        if args.len() == 1 {
                            expr = Expr::Index {
                                object: Box::new(expr),
                                index: Box::new(args.remove(0)),
                            };
                        } else {
                            expr = Expr::Apply {
                                func: Box::new(expr),
                                args,
                            };
                        }
                    }
                }
                Token::Quote => {
                    self.advance()?;
                    if self.at_expr_start() {
                        let arg = self.parse_postfix()?;
                        expr = Expr::Iterator {
                            func: Box::new(expr),
                            iter: IterKind::Each,
                            arg: Box::new(arg),
                        };
                    }
                    // else it's just a trailing quote, let it be
                }
                Token::QuoteColon => {
                    self.advance()?;
                    if self.at_expr_start() {
                        let arg = self.parse_postfix()?;
                        expr = Expr::Iterator {
                            func: Box::new(expr),
                            iter: IterKind::EachPrior,
                            arg: Box::new(arg),
                        };
                    }
                }
                Token::Slash => {
                    self.advance()?;
                    if self.at_expr_start() {
                        let arg = self.parse_postfix()?;
                        expr = Expr::Iterator {
                            func: Box::new(expr),
                            iter: IterKind::Over,
                            arg: Box::new(arg),
                        };
                    }
                }
                Token::Backslash => {
                    self.advance()?;
                    if self.at_expr_start() {
                        let arg = self.parse_postfix()?;
                        expr = Expr::Iterator {
                            func: Box::new(expr),
                            iter: IterKind::Scan,
                            arg: Box::new(arg),
                        };
                    }
                }
                _ => break,
            }
        }

        // Check for monadic function application: `f x` where f is an ident
        // Only if expr is an identifier that could be a function
        if let Expr::Ident(ref name) = expr {
            if is_monadic_builtin(name) && self.at_expr_start() {
                let op = monadic_from_name(name);
                let operand = self.parse_right_to_left()?;
                return Ok(Expr::UnaryOp {
                    op,
                    operand: Box::new(operand),
                });
            }
        }

        Ok(expr)
    }

    /// Parse an atomic expression.
    fn parse_atom(&mut self) -> Result<Expr> {
        match &self.current {
            Token::Int(v) => {
                let v = *v;
                self.advance()?;
                // Check if next tokens form a vector: `1 2 3`
                self.try_parse_int_vec(v)
            }
            Token::UInt(v) => {
                let v = *v;
                self.advance()?;
                Ok(Expr::UInt(v))
            }
            Token::Float(v) => {
                let v = *v;
                self.advance()?;
                self.try_parse_float_vec(v)
            }
            Token::Bool(v) => {
                let v = *v;
                self.advance()?;
                Ok(Expr::Bool(v))
            }
            Token::BoolVec(v) => {
                let v = v.clone();
                self.advance()?;
                Ok(Expr::BoolVec(v))
            }
            Token::Symbol(s) => {
                let s = s.clone();
                self.advance()?;
                self.try_parse_sym_vec(s)
            }
            Token::Str(s) => {
                let s = s.clone();
                self.advance()?;
                Ok(Expr::Str(s))
            }
            Token::Timestamp(s) => {
                let s = s.clone();
                self.advance()?;
                Ok(Expr::Timestamp(s))
            }
            Token::Duration(v, u) => {
                let v = *v;
                let u = *u;
                self.advance()?;
                Ok(Expr::Duration(v, u))
            }
            Token::Null(q) => {
                let q = *q;
                self.advance()?;
                Ok(Expr::Null(q))
            }
            Token::Ident(_) => {
                let name = self.expect_ident()?;
                Ok(Expr::Ident(name))
            }
            Token::Minus => {
                self.advance()?;
                let operand = self.parse_postfix()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOp::Neg,
                    operand: Box::new(operand),
                })
            }
            Token::LParen => {
                self.advance()?;
                // Check for table literal: ([] ...)
                if self.current == Token::LBracket {
                    self.advance()?;
                    self.expect(&Token::RBracket)?;
                    return self.parse_table_literal();
                }
                // Check for list (semicolons) vs grouping
                let first = self.parse_expr()?;
                if self.current == Token::Semi {
                    // List
                    let mut items = vec![first];
                    while self.current == Token::Semi {
                        self.advance()?;
                        if self.current == Token::RParen {
                            break;
                        }
                        items.push(self.parse_expr()?);
                    }
                    self.expect(&Token::RParen)?;
                    Ok(Expr::List(items))
                } else {
                    self.expect(&Token::RParen)?;
                    Ok(first)
                }
            }
            Token::LBrace => {
                self.advance()?;
                self.parse_lambda()
            }
            Token::Dollar => {
                self.advance()?;
                self.parse_conditional()
            }
            other => return Err(self.error_at_current(&format!("unexpected token: {:?}", other))),
        }
    }

    /// Try to extend a single int into a vector: `1 2 3`
    fn try_parse_int_vec(&mut self, first: i64) -> Result<Expr> {
        if let Token::Int(_) = &self.current {
            let mut vals = vec![first];
            while let Token::Int(v) = &self.current {
                vals.push(*v);
                self.advance()?;
            }
            Ok(Expr::Vec(vals.into_iter().map(Expr::Int).collect()))
        } else {
            Ok(Expr::Int(first))
        }
    }

    /// Try to extend a single float into a vector.
    fn try_parse_float_vec(&mut self, first: f64) -> Result<Expr> {
        if let Token::Float(_) = &self.current {
            let mut vals = vec![first];
            while let Token::Float(v) = &self.current {
                vals.push(*v);
                self.advance()?;
            }
            Ok(Expr::Vec(vals.into_iter().map(Expr::Float).collect()))
        } else {
            Ok(Expr::Float(first))
        }
    }

    /// Try to extend a single symbol into a vector: `` `a`b`c ``
    fn try_parse_sym_vec(&mut self, first: String) -> Result<Expr> {
        if let Token::Symbol(_) = &self.current {
            let mut vals = vec![first];
            while let Token::Symbol(s) = &self.current {
                vals.push(s.clone());
                self.advance()?;
            }
            Ok(Expr::Vec(vals.into_iter().map(Expr::Symbol).collect()))
        } else {
            Ok(Expr::Symbol(first))
        }
    }

    fn parse_lambda(&mut self) -> Result<Expr> {
        // Check for named params: {[a;b] body}
        let params = if self.current == Token::LBracket {
            self.advance()?;
            let mut params = vec![self.expect_ident()?];
            while self.current == Token::Semi {
                self.advance()?;
                params.push(self.expect_ident()?);
            }
            self.expect(&Token::RBracket)?;
            params
        } else {
            // Implicit params: x, y, z detected during interpretation
            vec![]
        };

        // Parse body (possibly multi-expression with semicolons)
        let mut exprs = vec![self.parse_expr()?];
        while self.current == Token::Semi {
            self.advance()?;
            if self.current == Token::RBrace {
                break;
            }
            exprs.push(self.parse_expr()?);
        }
        self.expect(&Token::RBrace)?;

        let body = if exprs.len() == 1 {
            exprs.remove(0)
        } else {
            Expr::Block(exprs)
        };

        Ok(Expr::Lambda {
            params,
            body: Box::new(body),
        })
    }

    fn parse_conditional(&mut self) -> Result<Expr> {
        // $[cond; true_expr; cond2; true_expr2; ...; default]
        self.expect(&Token::LBracket)?;

        let mut all_exprs = vec![self.parse_expr()?];
        while self.current == Token::Semi {
            self.advance()?;
            all_exprs.push(self.parse_expr()?);
        }
        self.expect(&Token::RBracket)?;

        if all_exprs.len() < 3 || all_exprs.len() % 2 == 0 {
            return Err(self.error_at_current("$[...] requires odd number of args >= 3 (cond; true; ...; default)"));
        }

        let default = all_exprs.pop().unwrap();
        let mut pairs = Vec::new();
        for chunk in all_exprs.chunks(2) {
            pairs.push((chunk[0].clone(), chunk[1].clone()));
        }

        Ok(Expr::Cond {
            pairs,
            default: Box::new(default),
        })
    }

    fn parse_table_literal(&mut self) -> Result<Expr> {
        // After `([] ` — parse `name: expr; name: expr; ...)`
        let mut cols = Vec::new();
        if self.current != Token::RParen {
            let name = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let value = self.parse_right_to_left()?;
            cols.push((name, value));
            while self.current == Token::Semi {
                self.advance()?;
                if self.current == Token::RParen {
                    break;
                }
                let name = self.expect_ident()?;
                self.expect(&Token::Colon)?;
                let value = self.parse_right_to_left()?;
                cols.push((name, value));
            }
        }
        self.expect(&Token::RParen)?;
        Ok(Expr::Table(cols))
    }

    fn parse_tag_predicate(&mut self) -> Result<TagPred> {
        let mut pred = self.parse_tag_atom()?;
        loop {
            match &self.current {
                Token::Amp => {
                    self.advance()?;
                    let right = self.parse_tag_atom()?;
                    pred = TagPred::And(Box::new(pred), Box::new(right));
                }
                Token::Pipe => {
                    self.advance()?;
                    let right = self.parse_tag_atom()?;
                    pred = TagPred::Or(Box::new(pred), Box::new(right));
                }
                _ => break,
            }
        }
        Ok(pred)
    }

    fn parse_tag_atom(&mut self) -> Result<TagPred> {
        if self.current == Token::LParen {
            self.advance()?;
            let pred = self.parse_tag_predicate()?;
            self.expect(&Token::RParen)?;
            return Ok(pred);
        }

        // Expect: `tag op value` where tag is a symbol
        let tag = match &self.current {
            Token::Symbol(s) => {
                let s = s.clone();
                self.advance()?;
                s
            }
            Token::Ident(s) => {
                let s = s.clone();
                self.advance()?;
                s
            }
            other => return Err(self.error_at_current(&format!("expected tag name in filter, got {:?}", other))),
        };

        let op = match &self.current {
            Token::Eq => { self.advance()?; TagCmpOp::Eq }
            Token::Neq => { self.advance()?; TagCmpOp::Neq }
            Token::Ident(s) if s == "like" => { self.advance()?; TagCmpOp::Like }
            Token::Ident(s) if s == "in" => { self.advance()?; TagCmpOp::In }
            other => return Err(self.error_at_current(&format!("expected comparison operator in tag filter, got {:?}", other))),
        };

        let value = self.parse_postfix()?;

        Ok(TagPred::Cmp { tag, op, value })
    }

    fn parse_select(&mut self) -> Result<Expr> {
        self.advance()?; // consume `select`

        // Parse field list
        let mut fields = Vec::new();
        fields.push(self.parse_select_field()?);
        while self.current == Token::Comma {
            self.advance()?;
            fields.push(self.parse_select_field()?);
        }

        // FROM
        if self.current != Token::Ident("from".to_string()) {
            return Err(self.error_at_current("expected 'from' in select expression"));
        }
        self.advance()?;
        let from = self.expect_ident()?;

        // Optional WHERE
        let filter = if self.current == Token::Ident("where".to_string()) {
            self.advance()?;
            Some(Box::new(self.parse_tag_predicate()?))
        } else {
            None
        };

        // Optional BY
        let by = if self.current == Token::Ident("by".to_string()) {
            self.advance()?;
            Some(Box::new(self.parse_right_to_left()?))
        } else {
            None
        };

        Ok(Expr::Select {
            fields,
            from,
            filter,
            by,
        })
    }

    fn parse_select_field(&mut self) -> Result<SelectField> {
        let first = self.expect_ident()?;

        // Check if next is also an ident (making first a function name)
        if let Token::Ident(_) = &self.current {
            let field = self.expect_ident()?;
            Ok(SelectField {
                func: Some(first),
                field,
                alias: None,
            })
        } else {
            Ok(SelectField {
                func: None,
                field: first,
                alias: None,
            })
        }
    }
}

fn is_monadic_builtin(name: &str) -> bool {
    matches!(
        name,
        "neg" | "not" | "abs" | "sqrt" | "exp" | "log" | "ceil" | "floor"
            | "signum" | "reciprocal"
            | "sum" | "avg" | "mean" | "min" | "max" | "count" | "first" | "last"
            | "med" | "dev" | "var"
            | "sums" | "avgs" | "mins" | "maxs" | "prds"
            | "til" | "rev" | "asc" | "desc" | "distinct" | "group" | "flip" | "raze"
            | "where"
            | "deltas" | "ratios" | "prev" | "next" | "fills" | "ffill" | "bfill"
            | "type" | "null" | "key" | "value" | "string"
            | "upper" | "lower" | "trim"
            | "now"
    )
}

fn monadic_from_name(name: &str) -> UnaryOp {
    match name {
        "neg" => UnaryOp::Neg,
        "not" => UnaryOp::Not,
        "abs" => UnaryOp::Abs,
        "sqrt" => UnaryOp::Sqrt,
        "exp" => UnaryOp::Exp,
        "log" => UnaryOp::Log,
        "ceil" => UnaryOp::Ceil,
        "floor" => UnaryOp::Floor,
        "signum" => UnaryOp::Signum,
        "reciprocal" => UnaryOp::Reciprocal,
        "sum" => UnaryOp::Sum,
        "avg" | "mean" => UnaryOp::Avg,
        "min" => UnaryOp::Min,
        "max" => UnaryOp::Max,
        "count" => UnaryOp::Count,
        "first" => UnaryOp::First,
        "last" => UnaryOp::Last,
        "med" => UnaryOp::Med,
        "dev" => UnaryOp::Dev,
        "var" => UnaryOp::Var,
        "sums" => UnaryOp::Sums,
        "avgs" => UnaryOp::Avgs,
        "mins" => UnaryOp::Mins,
        "maxs" => UnaryOp::Maxs,
        "prds" => UnaryOp::Prds,
        "til" => UnaryOp::Til,
        "rev" => UnaryOp::Rev,
        "asc" => UnaryOp::Asc,
        "desc" => UnaryOp::Desc,
        "distinct" => UnaryOp::Distinct,
        "group" => UnaryOp::Group,
        "flip" => UnaryOp::Flip,
        "raze" => UnaryOp::Raze,
        "where" => UnaryOp::Where,
        "deltas" => UnaryOp::Deltas,
        "ratios" => UnaryOp::Ratios,
        "prev" => UnaryOp::Prev,
        "next" => UnaryOp::Next,
        "fills" => UnaryOp::Fills,
        "ffill" => UnaryOp::Ffill,
        "bfill" => UnaryOp::Bfill,
        "type" => UnaryOp::Type,
        "null" => UnaryOp::IsNull,
        "key" => UnaryOp::Key,
        "value" => UnaryOp::Value,
        "string" => UnaryOp::ToString,
        "upper" => UnaryOp::Upper,
        "lower" => UnaryOp::Lower,
        "trim" => UnaryOp::Trim,
        "now" => UnaryOp::Now,
        _ => unreachable!("not a monadic builtin: {name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::DurationUnit;

    fn parse(input: &str) -> Result<Expr> {
        Parser::new(input)?.parse()
    }

    #[test]
    fn test_int_literal() {
        assert_eq!(parse("42").unwrap(), Expr::Int(42));
    }

    #[test]
    fn test_float_literal() {
        assert_eq!(parse("3.14").unwrap(), Expr::Float(3.14));
    }

    #[test]
    fn test_string_literal() {
        assert_eq!(parse("\"hello\"").unwrap(), Expr::Str("hello".into()));
    }

    #[test]
    fn test_symbol_literal() {
        assert_eq!(parse("`host").unwrap(), Expr::Symbol("host".into()));
    }

    #[test]
    fn test_bool_literal() {
        assert_eq!(parse("true").unwrap(), Expr::Bool(true));
    }

    #[test]
    fn test_bool_vec() {
        assert_eq!(parse("10010b").unwrap(), Expr::BoolVec(vec![true, false, false, true, false]));
    }

    #[test]
    fn test_int_vec() {
        let result = parse("1 2 3").unwrap();
        assert_eq!(result, Expr::Vec(vec![Expr::Int(1), Expr::Int(2), Expr::Int(3)]));
    }

    #[test]
    fn test_symbol_vec() {
        let result = parse("`a`b`c").unwrap();
        assert_eq!(
            result,
            Expr::Vec(vec![Expr::Symbol("a".into()), Expr::Symbol("b".into()), Expr::Symbol("c".into())])
        );
    }

    #[test]
    fn test_assignment() {
        let result = parse("x: 42").unwrap();
        assert_eq!(
            result,
            Expr::Assign {
                name: "x".into(),
                value: Box::new(Expr::Int(42)),
            }
        );
    }

    #[test]
    fn test_binary_add() {
        let result = parse("2 + 3").unwrap();
        assert_eq!(
            result,
            Expr::BinOp {
                op: BinOp::Add,
                left: Box::new(Expr::Int(2)),
                right: Box::new(Expr::Int(3)),
            }
        );
    }

    #[test]
    fn test_right_to_left_eval() {
        // 2 * 3 + 4 => 2 * (3 + 4) = 2 * 7 = 14
        let result = parse("2 * 3 + 4").unwrap();
        assert_eq!(
            result,
            Expr::BinOp {
                op: BinOp::Mul,
                left: Box::new(Expr::Int(2)),
                right: Box::new(Expr::BinOp {
                    op: BinOp::Add,
                    left: Box::new(Expr::Int(3)),
                    right: Box::new(Expr::Int(4)),
                }),
            }
        );
    }

    #[test]
    fn test_parens_override() {
        let result = parse("(2 * 3) + 4").unwrap();
        assert_eq!(
            result,
            Expr::BinOp {
                op: BinOp::Add,
                left: Box::new(Expr::BinOp {
                    op: BinOp::Mul,
                    left: Box::new(Expr::Int(2)),
                    right: Box::new(Expr::Int(3)),
                }),
                right: Box::new(Expr::Int(4)),
            }
        );
    }

    #[test]
    fn test_member_access() {
        let result = parse("cpu.usage_idle").unwrap();
        assert_eq!(
            result,
            Expr::Member {
                object: Box::new(Expr::Ident("cpu".into())),
                field: "usage_idle".into(),
            }
        );
    }

    #[test]
    fn test_monadic_function() {
        let result = parse("sum 1 2 3").unwrap();
        assert_eq!(
            result,
            Expr::UnaryOp {
                op: UnaryOp::Sum,
                operand: Box::new(Expr::Vec(vec![Expr::Int(1), Expr::Int(2), Expr::Int(3)])),
            }
        );
    }

    #[test]
    fn test_lambda() {
        let result = parse("{x + 1}").unwrap();
        assert!(matches!(result, Expr::Lambda { .. }));
    }

    #[test]
    fn test_lambda_named_params() {
        let result = parse("{[a;b] a + b}").unwrap();
        if let Expr::Lambda { params, .. } = result {
            assert_eq!(params, vec!["a", "b"]);
        } else {
            panic!("expected lambda");
        }
    }

    #[test]
    fn test_conditional() {
        let result = parse("$[x > 0; x; 0]").unwrap();
        assert!(matches!(result, Expr::Cond { .. }));
    }

    #[test]
    fn test_list() {
        let result = parse("(1; \"a\"; `x)").unwrap();
        assert_eq!(
            result,
            Expr::List(vec![Expr::Int(1), Expr::Str("a".into()), Expr::Symbol("x".into())])
        );
    }

    #[test]
    fn test_pipeline() {
        let result = parse("1 2 3 |> sum").unwrap();
        assert!(matches!(result, Expr::Pipe { .. }));
    }

    #[test]
    fn test_indexing() {
        let result = parse("x[0]").unwrap();
        assert_eq!(
            result,
            Expr::Index {
                object: Box::new(Expr::Ident("x".into())),
                index: Box::new(Expr::Int(0)),
            }
        );
    }

    #[test]
    fn test_multi_arg_call() {
        let result = parse("xbar[5m; ts]").unwrap();
        assert_eq!(
            result,
            Expr::Apply {
                func: Box::new(Expr::Ident("xbar".into())),
                args: vec![
                    Expr::Duration(5, DurationUnit::Minutes),
                    Expr::Ident("ts".into()),
                ],
            }
        );
    }

    #[test]
    fn test_tag_filter() {
        let result = parse("cpu @ `host = `server01").unwrap();
        assert!(matches!(result, Expr::TagFilter { .. }));
    }

    #[test]
    fn test_within() {
        let result = parse("cpu within (0; 100)").unwrap();
        assert!(matches!(result, Expr::Within { .. }));
    }

    #[test]
    fn test_neg_unary() {
        let result = parse("-5").unwrap();
        assert_eq!(
            result,
            Expr::UnaryOp {
                op: UnaryOp::Neg,
                operand: Box::new(Expr::Int(5)),
            }
        );
    }

    #[test]
    fn test_dict() {
        let result = parse("`a`b ! 1 2").unwrap();
        assert!(matches!(result, Expr::Dict { .. }));
    }

    #[test]
    fn test_table_literal() {
        let result = parse("([] name: `a`b; val: 1 2)").unwrap();
        assert!(matches!(result, Expr::Table(_)));
    }

    #[test]
    fn test_select_simple() {
        let result = parse("select avg usage_idle from cpu").unwrap();
        assert!(matches!(result, Expr::Select { .. }));
    }
}

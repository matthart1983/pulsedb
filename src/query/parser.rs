use anyhow::{bail, Result};

use crate::query::ast::*;
use crate::query::lexer::{Lexer, Token};

/// Recursive descent parser for PulseQL queries.
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

    pub fn parse(mut self) -> Result<SelectStatement> {
        let stmt = self.parse_select()?;
        if self.current != Token::Eof {
            bail!("unexpected token after statement: {:?}", self.current);
        }
        Ok(stmt)
    }

    fn advance(&mut self) -> Result<Token> {
        let prev = std::mem::replace(&mut self.current, Token::Eof);
        self.current = self.lexer.next_token()?;
        Ok(prev)
    }

    fn expect(&mut self, expected: &Token) -> Result<()> {
        if &self.current == expected {
            self.advance()?;
            Ok(())
        } else {
            bail!("expected {:?}, got {:?}", expected, self.current);
        }
    }

    fn expect_ident(&mut self) -> Result<String> {
        match &self.current {
            Token::Ident(name) => {
                let name = name.clone();
                self.advance()?;
                Ok(name)
            }
            // Allow "time" used as an identifier in some contexts.
            Token::Time => {
                self.advance()?;
                Ok("time".to_string())
            }
            other => bail!("expected identifier, got {:?}", other),
        }
    }

    fn parse_select(&mut self) -> Result<SelectStatement> {
        self.expect(&Token::Select)?;

        let fields = self.parse_field_list()?;

        self.expect(&Token::From)?;

        let measurement = self.expect_ident()?;

        let condition = if self.current == Token::Where {
            self.advance()?;
            Some(self.parse_where()?)
        } else {
            None
        };

        let group_by = if self.current == Token::Group {
            self.advance()?;
            self.expect(&Token::By)?;
            Some(self.parse_group_by()?)
        } else {
            None
        };

        let fill = if self.current == Token::Fill {
            self.advance()?;
            Some(self.parse_fill()?)
        } else {
            None
        };

        let order_by = if self.current == Token::Order {
            self.advance()?;
            self.expect(&Token::By)?;
            self.expect(&Token::Time)?;
            match &self.current {
                Token::Asc => {
                    self.advance()?;
                    Some(OrderBy::TimeAsc)
                }
                Token::Desc => {
                    self.advance()?;
                    Some(OrderBy::TimeDesc)
                }
                _ => Some(OrderBy::TimeAsc),
            }
        } else {
            None
        };

        let limit = if self.current == Token::Limit {
            self.advance()?;
            Some(self.parse_u64()?)
        } else {
            None
        };

        let offset = if self.current == Token::Offset {
            self.advance()?;
            Some(self.parse_u64()?)
        } else {
            None
        };

        Ok(SelectStatement {
            fields,
            measurement,
            condition,
            group_by,
            fill,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_u64(&mut self) -> Result<u64> {
        match &self.current {
            Token::IntLit(n) => {
                let v = *n as u64;
                self.advance()?;
                Ok(v)
            }
            other => bail!("expected integer, got {:?}", other),
        }
    }

    fn parse_field_list(&mut self) -> Result<Vec<FieldExpr>> {
        if self.current == Token::Star {
            self.advance()?;
            return Ok(vec![FieldExpr::Wildcard]);
        }

        let mut fields = Vec::new();
        fields.push(self.parse_field_expr()?);
        while self.current == Token::Comma {
            self.advance()?;
            fields.push(self.parse_field_expr()?);
        }
        Ok(fields)
    }

    fn parse_field_expr(&mut self) -> Result<FieldExpr> {
        let name = self.expect_ident()?;

        if self.current == Token::LParen {
            // This is an aggregate function call: name(field) or percentile(field, N)
            let func_name = name.to_ascii_lowercase();
            self.advance()?; // consume '('

            let field = self.expect_ident()?;

            let func = if func_name == "percentile" {
                self.expect(&Token::Comma)?;
                let p = match &self.current {
                    Token::NumberLit(n) => *n,
                    Token::IntLit(n) => *n as f64,
                    other => bail!("expected number for percentile, got {:?}", other),
                };
                self.advance()?;
                AggFunc::Percentile(p)
            } else {
                match func_name.as_str() {
                    "count" => AggFunc::Count,
                    "sum" => AggFunc::Sum,
                    "mean" => AggFunc::Mean,
                    "avg" => AggFunc::Avg,
                    "min" => AggFunc::Min,
                    "max" => AggFunc::Max,
                    "first" => AggFunc::First,
                    "last" => AggFunc::Last,
                    "stddev" => AggFunc::Stddev,
                    _ => bail!("unknown aggregate function: '{func_name}'"),
                }
            };

            self.expect(&Token::RParen)?;

            let alias = if self.current == Token::As {
                self.advance()?;
                Some(self.expect_ident()?)
            } else {
                None
            };

            Ok(FieldExpr::Aggregate { func, field, alias })
        } else {
            Ok(FieldExpr::Field(name))
        }
    }

    // WHERE parsing with AND/OR precedence (AND binds tighter).
    fn parse_where(&mut self) -> Result<WhereClause> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<WhereClause> {
        let mut left = self.parse_and()?;
        while self.current == Token::Or {
            self.advance()?;
            let right = self.parse_and()?;
            left = WhereClause::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<WhereClause> {
        let mut left = self.parse_predicate()?;
        while self.current == Token::And {
            self.advance()?;
            let right = self.parse_predicate()?;
            left = WhereClause::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_predicate(&mut self) -> Result<WhereClause> {
        // Parenthesized sub-expression
        if self.current == Token::LParen {
            self.advance()?;
            let clause = self.parse_where()?;
            self.expect(&Token::RParen)?;
            return Ok(clause);
        }

        // time BETWEEN ... AND ...
        // time <op> <time_expr>
        if self.current == Token::Time {
            self.advance()?;

            if self.current == Token::Between {
                self.advance()?;
                let start = self.parse_time_expr()?;
                self.expect(&Token::And)?;
                let end = self.parse_time_expr()?;
                return Ok(WhereClause::TimeBetween { start, end });
            }

            let op = self.parse_comp_op()?;
            let value = self.parse_time_expr()?;
            return Ok(WhereClause::TimeComparison { op, value });
        }

        // tag comparison: ident op value
        let tag = self.expect_ident()?;
        let op = self.parse_comp_op()?;

        let value = match &self.current {
            Token::StringLit(s) => {
                let s = s.clone();
                self.advance()?;
                s
            }
            Token::RegexLit(s) => {
                let s = s.clone();
                self.advance()?;
                s
            }
            Token::IntLit(n) => {
                let s = n.to_string();
                self.advance()?;
                s
            }
            Token::NumberLit(n) => {
                let s = n.to_string();
                self.advance()?;
                s
            }
            Token::Ident(s) => {
                let s = s.clone();
                self.advance()?;
                s
            }
            other => bail!("expected value in comparison, got {:?}", other),
        };

        Ok(WhereClause::Comparison { tag, op, value })
    }

    fn parse_comp_op(&mut self) -> Result<CompOp> {
        let op = match &self.current {
            Token::Eq => CompOp::Eq,
            Token::Neq => CompOp::Neq,
            Token::Gt => CompOp::Gt,
            Token::Lt => CompOp::Lt,
            Token::Gte => CompOp::Gte,
            Token::Lte => CompOp::Lte,
            Token::RegexMatch => CompOp::RegexMatch,
            Token::RegexNotMatch => CompOp::RegexNotMatch,
            other => bail!("expected comparison operator, got {:?}", other),
        };
        self.advance()?;
        Ok(op)
    }

    fn parse_time_expr(&mut self) -> Result<TimeExpr> {
        match &self.current {
            Token::Now => {
                self.advance()?;
                self.expect(&Token::LParen)?;
                self.expect(&Token::RParen)?;
                if self.current == Token::Minus {
                    self.advance()?;
                    let dur = self.parse_duration()?;
                    Ok(TimeExpr::NowMinus(dur))
                } else {
                    Ok(TimeExpr::Now)
                }
            }
            Token::StringLit(s) => {
                let s = s.clone();
                self.advance()?;
                Ok(TimeExpr::DateString(s))
            }
            Token::IntLit(n) => {
                let n = *n;
                self.advance()?;
                Ok(TimeExpr::Literal(n))
            }
            other => bail!("expected time expression, got {:?}", other),
        }
    }

    fn parse_duration(&mut self) -> Result<Duration> {
        match &self.current {
            Token::DurationLit(value, unit) => {
                let dur = Duration {
                    value: *value,
                    unit: *unit,
                };
                self.advance()?;
                Ok(dur)
            }
            other => bail!("expected duration literal, got {:?}", other),
        }
    }

    fn parse_group_by(&mut self) -> Result<GroupBy> {
        let mut time_interval = None;
        let mut tags = Vec::new();

        // Check for time(duration)
        if self.current == Token::Time {
            self.advance()?;
            self.expect(&Token::LParen)?;
            let dur = self.parse_duration()?;
            self.expect(&Token::RParen)?;
            time_interval = Some(dur);

            // More items after a comma
            while self.current == Token::Comma {
                self.advance()?;
                tags.push(self.expect_ident()?);
            }
        } else {
            // Just tag names
            tags.push(self.expect_ident()?);
            while self.current == Token::Comma {
                self.advance()?;
                tags.push(self.expect_ident()?);
            }
        }

        Ok(GroupBy {
            time_interval,
            tags,
        })
    }

    fn parse_fill(&mut self) -> Result<FillPolicy> {
        self.expect(&Token::LParen)?;
        let policy = match &self.current {
            Token::Ident(s) => match s.to_ascii_lowercase().as_str() {
                "none" => FillPolicy::None,
                "null" => FillPolicy::Null,
                "linear" => FillPolicy::Linear,
                "previous" => FillPolicy::Previous,
                other => bail!("unknown fill policy: '{other}'"),
            },
            Token::NumberLit(n) => FillPolicy::Value(*n),
            Token::IntLit(n) => FillPolicy::Value(*n as f64),
            other => bail!("expected fill policy, got {:?}", other),
        };
        self.advance()?;
        self.expect(&Token::RParen)?;
        Ok(policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> Result<SelectStatement> {
        Parser::new(input)?.parse()
    }

    #[test]
    fn test_basic_select_star() {
        let stmt = parse("SELECT * FROM cpu").unwrap();
        assert_eq!(stmt.fields, vec![FieldExpr::Wildcard]);
        assert_eq!(stmt.measurement, "cpu");
        assert!(stmt.condition.is_none());
        assert!(stmt.group_by.is_none());
        assert!(stmt.fill.is_none());
        assert!(stmt.order_by.is_none());
        assert!(stmt.limit.is_none());
        assert!(stmt.offset.is_none());
    }

    #[test]
    fn test_with_where_and_time() {
        let stmt =
            parse("SELECT mean(usage) FROM cpu WHERE host = 'a' AND time > now() - 1h").unwrap();

        assert_eq!(
            stmt.fields,
            vec![FieldExpr::Aggregate {
                func: AggFunc::Mean,
                field: "usage".into(),
                alias: None,
            }]
        );
        assert_eq!(stmt.measurement, "cpu");

        let cond = stmt.condition.unwrap();
        match &cond {
            WhereClause::And(left, right) => {
                match left.as_ref() {
                    WhereClause::Comparison { tag, op, value } => {
                        assert_eq!(tag, "host");
                        assert_eq!(*op, CompOp::Eq);
                        assert_eq!(value, "a");
                    }
                    other => panic!("expected Comparison, got {:?}", other),
                }
                match right.as_ref() {
                    WhereClause::TimeComparison { op, value } => {
                        assert_eq!(*op, CompOp::Gt);
                        assert_eq!(
                            *value,
                            TimeExpr::NowMinus(Duration {
                                value: 1,
                                unit: DurationUnit::Hours,
                            })
                        );
                    }
                    other => panic!("expected TimeComparison, got {:?}", other),
                }
            }
            other => panic!("expected And, got {:?}", other),
        }
    }

    #[test]
    fn test_group_by_time() {
        let stmt = parse("SELECT mean(usage) FROM cpu GROUP BY time(5m)").unwrap();
        let gb = stmt.group_by.unwrap();
        assert_eq!(
            gb.time_interval,
            Some(Duration {
                value: 5,
                unit: DurationUnit::Minutes,
            })
        );
        assert!(gb.tags.is_empty());
    }

    #[test]
    fn test_group_by_time_and_tag() {
        let stmt = parse("SELECT sum(bytes) FROM net GROUP BY time(1m), host").unwrap();
        let gb = stmt.group_by.unwrap();
        assert_eq!(
            gb.time_interval,
            Some(Duration {
                value: 1,
                unit: DurationUnit::Minutes,
            })
        );
        assert_eq!(gb.tags, vec!["host".to_string()]);
    }

    #[test]
    fn test_fill_linear() {
        let stmt =
            parse("SELECT mean(temp) FROM sensor GROUP BY time(1h) FILL(linear)").unwrap();
        assert_eq!(stmt.fill, Some(FillPolicy::Linear));
    }

    #[test]
    fn test_fill_value() {
        let stmt = parse("SELECT mean(temp) FROM sensor GROUP BY time(1h) FILL(0)").unwrap();
        assert_eq!(stmt.fill, Some(FillPolicy::Value(0.0)));
    }

    #[test]
    fn test_fill_previous() {
        let stmt =
            parse("SELECT mean(temp) FROM sensor GROUP BY time(1h) FILL(previous)").unwrap();
        assert_eq!(stmt.fill, Some(FillPolicy::Previous));
    }

    #[test]
    fn test_order_by_desc_limit() {
        let stmt = parse("SELECT * FROM cpu ORDER BY time DESC LIMIT 100").unwrap();
        assert_eq!(stmt.order_by, Some(OrderBy::TimeDesc));
        assert_eq!(stmt.limit, Some(100));
    }

    #[test]
    fn test_order_by_asc() {
        let stmt = parse("SELECT * FROM cpu ORDER BY time ASC").unwrap();
        assert_eq!(stmt.order_by, Some(OrderBy::TimeAsc));
    }

    #[test]
    fn test_limit_offset() {
        let stmt = parse("SELECT * FROM cpu LIMIT 50 OFFSET 10").unwrap();
        assert_eq!(stmt.limit, Some(50));
        assert_eq!(stmt.offset, Some(10));
    }

    #[test]
    fn test_multiple_aggregations() {
        let stmt = parse("SELECT min(val), max(val), mean(val) FROM temp").unwrap();
        assert_eq!(
            stmt.fields,
            vec![
                FieldExpr::Aggregate {
                    func: AggFunc::Min,
                    field: "val".into(),
                    alias: None,
                },
                FieldExpr::Aggregate {
                    func: AggFunc::Max,
                    field: "val".into(),
                    alias: None,
                },
                FieldExpr::Aggregate {
                    func: AggFunc::Mean,
                    field: "val".into(),
                    alias: None,
                },
            ]
        );
    }

    #[test]
    fn test_regex_where() {
        let stmt = parse("SELECT * FROM cpu WHERE host =~ /web-\\d+/").unwrap();
        match stmt.condition.unwrap() {
            WhereClause::Comparison { tag, op, value } => {
                assert_eq!(tag, "host");
                assert_eq!(op, CompOp::RegexMatch);
                assert_eq!(value, "web-\\d+");
            }
            other => panic!("expected Comparison, got {:?}", other),
        }
    }

    #[test]
    fn test_time_between() {
        let stmt =
            parse("SELECT * FROM cpu WHERE time BETWEEN '2024-01-01' AND '2024-02-01'").unwrap();
        match stmt.condition.unwrap() {
            WhereClause::TimeBetween { start, end } => {
                assert_eq!(start, TimeExpr::DateString("2024-01-01".into()));
                assert_eq!(end, TimeExpr::DateString("2024-02-01".into()));
            }
            other => panic!("expected TimeBetween, got {:?}", other),
        }
    }

    #[test]
    fn test_percentile() {
        let stmt = parse("SELECT percentile(usage, 95) FROM cpu").unwrap();
        assert_eq!(
            stmt.fields,
            vec![FieldExpr::Aggregate {
                func: AggFunc::Percentile(95.0),
                field: "usage".into(),
                alias: None,
            }]
        );
    }

    #[test]
    fn test_aggregate_with_alias() {
        let stmt = parse("SELECT mean(usage) AS avg_usage FROM cpu").unwrap();
        assert_eq!(
            stmt.fields,
            vec![FieldExpr::Aggregate {
                func: AggFunc::Mean,
                field: "usage".into(),
                alias: Some("avg_usage".into()),
            }]
        );
    }

    #[test]
    fn test_or_precedence() {
        let stmt =
            parse("SELECT * FROM cpu WHERE host = 'a' OR host = 'b' AND region = 'us'").unwrap();
        // AND binds tighter, so this should be: host='a' OR (host='b' AND region='us')
        match stmt.condition.unwrap() {
            WhereClause::Or(left, right) => {
                assert!(matches!(left.as_ref(), WhereClause::Comparison { .. }));
                assert!(matches!(right.as_ref(), WhereClause::And(_, _)));
            }
            other => panic!("expected Or, got {:?}", other),
        }
    }

    #[test]
    fn test_parenthesized_where() {
        let stmt =
            parse("SELECT * FROM cpu WHERE (host = 'a' OR host = 'b') AND region = 'us'")
                .unwrap();
        match stmt.condition.unwrap() {
            WhereClause::And(left, right) => {
                assert!(matches!(left.as_ref(), WhereClause::Or(_, _)));
                assert!(matches!(right.as_ref(), WhereClause::Comparison { .. }));
            }
            other => panic!("expected And, got {:?}", other),
        }
    }

    #[test]
    fn test_multiple_fields() {
        let stmt = parse("SELECT usage, idle, system FROM cpu").unwrap();
        assert_eq!(
            stmt.fields,
            vec![
                FieldExpr::Field("usage".into()),
                FieldExpr::Field("idle".into()),
                FieldExpr::Field("system".into()),
            ]
        );
    }

    #[test]
    fn test_missing_from() {
        let result = parse("SELECT *");
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_select() {
        let result = parse("FROM cpu");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_operator() {
        let result = parse("SELECT * FROM cpu WHERE host ** 'a'");
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_input() {
        let result = parse("");
        assert!(result.is_err());
    }

    #[test]
    fn test_complex_query() {
        let stmt = parse(
            "SELECT mean(usage), max(usage) FROM cpu \
             WHERE host = 'server01' AND time > now() - 6h \
             GROUP BY time(5m), host \
             FILL(none) \
             ORDER BY time DESC \
             LIMIT 1000 OFFSET 0",
        )
        .unwrap();

        assert_eq!(stmt.fields.len(), 2);
        assert_eq!(stmt.measurement, "cpu");
        assert!(stmt.condition.is_some());
        let gb = stmt.group_by.unwrap();
        assert_eq!(
            gb.time_interval,
            Some(Duration {
                value: 5,
                unit: DurationUnit::Minutes,
            })
        );
        assert_eq!(gb.tags, vec!["host".to_string()]);
        assert_eq!(stmt.fill, Some(FillPolicy::None));
        assert_eq!(stmt.order_by, Some(OrderBy::TimeDesc));
        assert_eq!(stmt.limit, Some(1000));
        assert_eq!(stmt.offset, Some(0));
    }

    #[test]
    fn test_time_literal() {
        let stmt = parse("SELECT * FROM cpu WHERE time > 1704067200000000000").unwrap();
        match stmt.condition.unwrap() {
            WhereClause::TimeComparison { op, value } => {
                assert_eq!(op, CompOp::Gt);
                assert_eq!(value, TimeExpr::Literal(1704067200000000000));
            }
            other => panic!("expected TimeComparison, got {:?}", other),
        }
    }

    #[test]
    fn test_now_without_minus() {
        let stmt = parse("SELECT * FROM cpu WHERE time > now()").unwrap();
        match stmt.condition.unwrap() {
            WhereClause::TimeComparison { op, value } => {
                assert_eq!(op, CompOp::Gt);
                assert_eq!(value, TimeExpr::Now);
            }
            other => panic!("expected TimeComparison, got {:?}", other),
        }
    }

    #[test]
    fn test_regex_not_match() {
        let stmt = parse("SELECT * FROM cpu WHERE host !~ /test.*/").unwrap();
        match stmt.condition.unwrap() {
            WhereClause::Comparison { tag, op, value } => {
                assert_eq!(tag, "host");
                assert_eq!(op, CompOp::RegexNotMatch);
                assert_eq!(value, "test.*");
            }
            other => panic!("expected Comparison, got {:?}", other),
        }
    }

    #[test]
    fn test_group_by_tags_only() {
        let stmt = parse("SELECT count(val) FROM events GROUP BY region, host").unwrap();
        let gb = stmt.group_by.unwrap();
        assert!(gb.time_interval.is_none());
        assert_eq!(
            gb.tags,
            vec!["region".to_string(), "host".to_string()]
        );
    }
}

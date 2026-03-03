/// A complete SELECT query.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// SELECT clause
    pub fields: Vec<FieldExpr>,
    /// FROM clause
    pub measurement: String,
    /// WHERE clause
    pub condition: Option<WhereClause>,
    /// GROUP BY clause
    pub group_by: Option<GroupBy>,
    /// FILL clause
    pub fill: Option<FillPolicy>,
    /// ORDER BY clause
    pub order_by: Option<OrderBy>,
    /// LIMIT clause
    pub limit: Option<u64>,
    /// OFFSET clause
    pub offset: Option<u64>,
}

/// A field expression in the SELECT clause.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldExpr {
    /// A raw field reference: `usage_idle`
    Field(String),
    /// An aggregation: `mean(usage_idle)`
    Aggregate {
        func: AggFunc,
        field: String,
        alias: Option<String>,
    },
    /// Wildcard: `*`
    Wildcard,
}

/// Aggregation function names.
#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Mean,
    Avg,
    Min,
    Max,
    First,
    Last,
    Stddev,
    Percentile(f64),
}

/// WHERE clause — a tree of conditions.
#[derive(Debug, Clone, PartialEq)]
pub enum WhereClause {
    Comparison {
        tag: String,
        op: CompOp,
        value: String,
    },
    TimeComparison {
        op: CompOp,
        value: TimeExpr,
    },
    TimeBetween {
        start: TimeExpr,
        end: TimeExpr,
    },
    And(Box<WhereClause>, Box<WhereClause>),
    Or(Box<WhereClause>, Box<WhereClause>),
}

/// Comparison operators.
#[derive(Debug, Clone, PartialEq)]
pub enum CompOp {
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    RegexMatch,
    RegexNotMatch,
}

/// A time expression for WHERE clauses.
#[derive(Debug, Clone, PartialEq)]
pub enum TimeExpr {
    Now,
    NowMinus(Duration),
    /// Nanosecond epoch timestamp.
    Literal(i64),
    /// ISO date string to be parsed later.
    DateString(String),
}

/// A duration value with a unit.
#[derive(Debug, Clone, PartialEq)]
pub struct Duration {
    pub value: u64,
    pub unit: DurationUnit,
}

/// Duration time units.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DurationUnit {
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
    Minutes,
    Hours,
    Days,
    Weeks,
}

impl Duration {
    /// Converts the duration to nanoseconds.
    pub fn to_nanos(&self) -> u64 {
        let multiplier = match self.unit {
            DurationUnit::Nanoseconds => 1,
            DurationUnit::Microseconds => 1_000,
            DurationUnit::Milliseconds => 1_000_000,
            DurationUnit::Seconds => 1_000_000_000,
            DurationUnit::Minutes => 60 * 1_000_000_000,
            DurationUnit::Hours => 3_600 * 1_000_000_000,
            DurationUnit::Days => 86_400 * 1_000_000_000,
            DurationUnit::Weeks => 604_800 * 1_000_000_000,
        };
        self.value * multiplier
    }
}

/// GROUP BY clause.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupBy {
    pub time_interval: Option<Duration>,
    pub tags: Vec<String>,
}

/// FILL policy for missing time buckets.
#[derive(Debug, Clone, PartialEq)]
pub enum FillPolicy {
    None,
    Null,
    Linear,
    Previous,
    Value(f64),
}

/// ORDER BY direction for time column.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderBy {
    TimeAsc,
    TimeDesc,
}

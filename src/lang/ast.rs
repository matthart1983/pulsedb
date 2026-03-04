use crate::query::ast::DurationUnit;

/// Top-level expression node for PulseLang.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    // Literals
    Int(i64),
    UInt(u64),
    Float(f64),
    Bool(bool),
    Str(String),
    Symbol(String),
    Timestamp(String),
    Duration(u64, DurationUnit),
    Null(Option<char>),

    // Identifier reference
    Ident(String),

    // Vector literal: `1 2 3 4`
    Vec(Vec<Expr>),

    // Boolean vector: `10010b`
    BoolVec(Vec<bool>),

    // List (heterogeneous, semicolon-separated): `(1; "a"; `x)`
    List(Vec<Expr>),

    // Dict: `keys ! values`
    Dict {
        keys: Box<Expr>,
        values: Box<Expr>,
    },

    // Table: `([] col1: ...; col2: ...)`
    Table(Vec<(String, Expr)>),

    // Lambda: `{x + 1}` or `{[a;b] a + b}`
    Lambda {
        params: Vec<String>,
        body: Box<Expr>,
    },

    // Binary operation: `x + y`, `x * y`, etc.
    BinOp {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },

    // Unary operation (monadic function application): `neg x`, `not x`
    UnaryOp {
        op: UnaryOp,
        operand: Box<Expr>,
    },

    // Function application: `f x` or `f[x; y]`
    Apply {
        func: Box<Expr>,
        args: Vec<Expr>,
    },

    // Member access: `cpu.usage_idle`, `ts.year`
    Member {
        object: Box<Expr>,
        field: String,
    },

    // Indexing: `x[0]`, `x[2 4]`
    Index {
        object: Box<Expr>,
        index: Box<Expr>,
    },

    // Assignment: `x: 42`
    Assign {
        name: String,
        value: Box<Expr>,
    },

    // Tag filter: `cpu @ `host = `server01`
    TagFilter {
        source: Box<Expr>,
        predicate: Box<TagPred>,
    },

    // Time range: `cpu within (start; end)`
    Within {
        source: Box<Expr>,
        start: Box<Expr>,
        end: Box<Expr>,
    },

    // Pipeline: `x |> f |> g`
    Pipe {
        left: Box<Expr>,
        right: Box<Expr>,
    },

    // Conditional: `$[cond; true_expr; false_expr; ...]`
    Cond {
        pairs: Vec<(Expr, Expr)>,
        default: Box<Expr>,
    },

    // Iterator application: `f' x`, `f/ x`, `f\ x`, `f': x`
    Iterator {
        func: Box<Expr>,
        iter: IterKind,
        arg: Box<Expr>,
    },

    // Select expression (sugar): `select avg usage from cpu where ... by ...`
    Select {
        fields: Vec<SelectField>,
        from: String,
        filter: Option<Box<TagPred>>,
        by: Option<Box<Expr>>,
    },

    // Sequence of expressions (semicolon-separated in a block)
    Block(Vec<Expr>),
}

/// Binary operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Pow,
    Mod,
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    Match,
    And,
    Or,
    Join,
    Take,
    Drop,
    Find,
    In,
    Like,
}

/// Unary (monadic) operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
    Abs,
    Sqrt,
    Exp,
    Log,
    Ceil,
    Floor,
    Signum,
    Reciprocal,
    // Aggregations (monadic: array → scalar)
    Sum,
    Avg,
    Mean,
    Min,
    Max,
    Count,
    First,
    Last,
    Med,
    Dev,
    Var,
    // Scans (monadic: array → array)
    Sums,
    Avgs,
    Mins,
    Maxs,
    Prds,
    // Structural
    Til,
    Rev,
    Asc,
    Desc,
    Distinct,
    Group,
    Flip,
    Raze,
    Where,
    // Time-series
    Deltas,
    Ratios,
    Prev,
    Next,
    Fills,
    Ffill,
    Bfill,
    // Type
    Type,
    IsNull,
    Key,
    Value,
    ToString,
    // String
    Upper,
    Lower,
    Trim,
    // Temporal
    Now,
}

/// Iterator kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IterKind {
    Each,
    Over,
    Scan,
    EachPrior,
}

/// Tag predicate for `@` filter.
#[derive(Debug, Clone, PartialEq)]
pub enum TagPred {
    Cmp {
        tag: String,
        op: TagCmpOp,
        value: Expr,
    },
    And(Box<TagPred>, Box<TagPred>),
    Or(Box<TagPred>, Box<TagPred>),
}

/// Tag comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagCmpOp {
    Eq,
    Neq,
    Like,
    In,
}

/// A field expression in a `select` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectField {
    pub func: Option<String>,
    pub field: String,
    pub alias: Option<String>,
}

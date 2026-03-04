# PulseLang — An APL-Inspired Functional Query Language for PulseDB

## 1. Overview

PulseLang is an array-oriented, functional query language for PulseDB, inspired by the APL family of languages (APL, J, K/Q, BQN). It replaces PulseQL's SQL-like syntax with a concise, composable notation where arrays are the fundamental data type and operations implicitly map over collections.

### Why an APL-Style Language?

PulseDB stores time-series data as columnar arrays — timestamps, floats, integers, booleans — which are the natural substrate for an array language. SQL-like syntax (PulseQL) forces users to express inherently array-oriented operations in a verbose, declarative grammar designed for row-oriented relational databases. An APL-style language:

1. **Maps directly to the storage model** — Columnar segments *are* arrays. The language operates on what the engine already has.
2. **Eliminates syntactic overhead** — `+/x` instead of `SELECT sum(x) FROM ...`. Time-series analytics are naturally expressed as reductions, scans, and windowed operations.
3. **Enables composition** — Pipelines of transformations chain without subqueries or CTEs. `mean ∘ diff ∘ sort` is a function, not three nested SELECT statements.
4. **Matches the domain** — Financial quants, signal processing engineers, and data scientists think in terms of vectors, sliding windows, and element-wise transforms — not JOINs and GROUP BYs.

### Design Goals

- **Terse but readable** — Use ASCII operator names where possible (inspired by K/Q), with optional Unicode glyphs for APL purists
- **Zero learning-curve data access** — `cpu.usage_idle` fetches the column. No FROM clause needed.
- **First-class time** — Temporal operations (bucketing, windowing, alignment, resampling) are primitives, not bolted-on functions
- **Composable** — Every operation returns an array or table. Everything pipes.
- **Embeddable** — Runs in the PulseDB REPL, HTTP API, and as stored procedures
- **Compatible** — Can express anything PulseQL can, and more

### Non-Goals (v1)

- General-purpose programming (no file I/O, no networking, no system calls)
- Multi-statement imperative scripts (pure expression language)
- Backward compatibility with any existing APL/K/Q implementation

---

## 2. Type System

### Scalar Types

PulseLang inherits PulseDB's field types and adds first-class temporal and symbol types:

| Type      | Literal Syntax              | PulseDB Mapping      | Description                          |
|-----------|-----------------------------|----------------------|--------------------------------------|
| `int`     | `42`, `-7`, `0x2A`          | `i64`                | 64-bit signed integer                |
| `uint`    | `42u`                       | `u64`                | 64-bit unsigned integer              |
| `float`   | `3.14`, `1e-5`, `0n` (NaN) | `f64`                | 64-bit IEEE 754 float                |
| `bool`    | `1b`, `0b`, `true`, `false` | `bool`               | Boolean                              |
| `sym`     | `` `host ``, `` `us-east `` | tag values           | Interned symbol (like Q symbols)     |
| `str`     | `"hello"`                   | `String`             | UTF-8 string                         |
| `ts`      | `2024.01.15D14:30:00`       | `i64` (ns epoch)     | Nanosecond timestamp                 |
| `dur`     | `5m`, `1h`, `30s`, `7d`     | `Duration`           | Time duration                        |
| `null`    | `0N`                        | —                    | Typed null (0Ni, 0Nf, 0Nt)          |

### Compound Types

| Type      | Literal Syntax              | Description                              |
|-----------|-----------------------------|------------------------------------------|
| `vec`     | `1 2 3 4 5`                 | Homogeneous array (space-separated)      |
| `list`    | `(1; "a"; `x)`              | Heterogeneous list (semicolon-separated) |
| `dict`    | `` `a`b ! 1 2 ``            | Key-value dictionary                     |
| `table`   | `([] ts:...; val:...)`      | Columnar table (list of same-length vecs)|
| `fn`      | `{x + 1}`                   | Lambda / function                        |

### Type Hierarchy

```
scalar ─┬─ numeric ─┬─ int
        │           ├─ uint
        │           ├─ float
        │           └─ bool (promotes to int: 0/1)
        ├─ temporal ─┬─ ts
        │            └─ dur
        ├─ sym
        └─ str

compound ─┬─ vec (homogeneous)
          ├─ list (heterogeneous)
          ├─ dict
          ├─ table
          └─ fn
```

### Implicit Type Promotion

- `bool` → `int` → `float` (arithmetic contexts)
- `int` × `dur` → `dur` (e.g., `3 * 5m` → `15m`)
- `ts` - `ts` → `dur`
- `ts` + `dur` → `ts`

---

## 3. Syntax

### Evaluation Order

Like APL/K/Q, PulseLang evaluates **right to left** with uniform precedence. Parentheses override:

```
2 * 3 + 4       / => 2 * 7 => 14 (not 10)
(2 * 3) + 4     / => 6 + 4 => 10
```

### Comments

```
/ this is a line comment (K-style)
x: 42  / inline comment
```

### Assignment

```
x: 42                          / assign scalar
ts: 2024.01.15D14:30:00        / assign timestamp
vals: 1.5 2.3 4.1 3.8          / assign vector
```

### Vectors

Vectors are space-separated values of the same type:

```
1 2 3 4 5                      / int vector
1.0 2.5 3.7                    / float vector
`a `b `c                       / symbol vector
10010b                         / boolean vector (compact form)
```

### Indexing

Zero-based indexing, with vector indexing for multi-select:

```
x: 10 20 30 40 50
x[0]                           / => 10
x[2 4]                         / => 30 50
x[-1]                          / => 50 (negative = from end)
x[1..3]                        / => 20 30 40 (range, inclusive)
```

### Lambdas

Functions are curly-brace blocks. Implicit args are `x`, `y`, `z` (monadic, dyadic, triadic). Named args use brackets:

```
{x + 1}                        / monadic: increment
{x + y}                        / dyadic: add
{[a;b;c] a + b * c}            / named args

double: {2 * x}                / named function
double 21                      / => 42
```

### Pipelines (Left-to-Right Composition)

The pipe operator `|>` enables left-to-right dataflow, overriding the default right-to-left evaluation:

```
cpu.usage_idle |> where{x > 90} |> mean
/ equivalent to: mean where{x > 90} cpu.usage_idle
```

---

## 4. Operators

### Arithmetic (Dyadic, element-wise on vectors)

| Op    | Name       | Example              | Result         |
|-------|------------|----------------------|----------------|
| `+`   | Add        | `1 2 3 + 10 20 30`  | `11 22 33`     |
| `-`   | Subtract   | `10 20 - 3 7`       | `7 13`         |
| `*`   | Multiply   | `2 3 * 4 5`         | `8 15`         |
| `%`   | Divide     | `10 20 % 3 4`       | `3.33.. 5.0`   |
| `mod` | Modulo     | `7 mod 3`           | `1`            |
| `^`   | Power      | `2 ^ 10`            | `1024`         |
| `neg` | Negate (M) | `neg 3 -5 7`        | `-3 5 -7`      |
| `abs` | Abs (M)    | `abs -3 5 -7`       | `3 5 7`        |

*(M) = monadic (single argument)*

### Comparison (return boolean vectors)

| Op    | Name            | Example            | Result       |
|-------|-----------------|--------------------|--------------|
| `=`   | Equal           | `1 2 3 = 2`        | `010b`       |
| `<>`  | Not equal        | `1 2 3 <> 2`       | `101b`       |
| `<`   | Less than        | `1 2 3 < 2`        | `100b`       |
| `>`   | Greater than     | `1 2 3 > 2`        | `001b`       |
| `<=`  | At most          | `1 2 3 <= 2`       | `110b`       |
| `>=`  | At least         | `1 2 3 >= 2`       | `011b`       |
| `~`   | Match (deep eq)  | `(1 2) ~ (1 2)`    | `1b`         |
| `like`| Pattern match    | `` `web-01 like "web-*" `` | `1b`  |

### Logic

| Op    | Name    | Example           | Result     |
|-------|---------|--------------------|-----------|
| `&`   | And     | `110b & 101b`      | `100b`    |
| `\|`  | Or      | `110b \| 101b`     | `111b`    |
| `not` | Not (M) | `not 110b`         | `001b`    |

### Aggregation / Reduction (Monadic, array → scalar)

| Op         | Name             | Example                    | Result    |
|------------|------------------|----------------------------|-----------|
| `sum`      | Sum              | `sum 1 2 3 4`              | `10`      |
| `avg`      | Mean             | `avg 1 2 3 4`              | `2.5`     |
| `mean`     | Mean (alias)     | `mean 1 2 3 4`             | `2.5`     |
| `min`      | Minimum          | `min 5 1 9 3`              | `1`       |
| `max`      | Maximum          | `max 5 1 9 3`              | `9`       |
| `count`    | Count            | `count 10 20 30`           | `3`       |
| `first`    | First element    | `first 10 20 30`           | `10`      |
| `last`     | Last element     | `last 10 20 30`            | `30`      |
| `med`      | Median           | `med 1 5 3 9 2`            | `3`       |
| `dev`      | Std deviation    | `dev 2 4 4 4 6`            | `1.265`   |
| `var`      | Variance         | `var 2 4 4 4 6`            | `1.6`     |
| `pct`      | Percentile       | `90 pct vals`              | (90th)    |

### Scan / Running Aggregates (Monadic, array → array)

Scans produce running (cumulative) versions of reductions. Suffix `s` for scan form:

| Op       | Name             | Example                 | Result          |
|----------|------------------|-------------------------|-----------------|
| `sums`   | Running sum      | `sums 1 2 3 4`          | `1 3 6 10`      |
| `avgs`   | Running mean     | `avgs 1 2 3 4`          | `1 1.5 2 2.5`   |
| `mins`   | Running min      | `mins 5 1 9 3`          | `5 1 1 1`       |
| `maxs`   | Running max      | `maxs 5 1 9 3`          | `5 5 9 9`       |
| `prds`   | Running product  | `prds 1 2 3 4`          | `1 2 6 24`      |

### Structural / Array Operations

| Op         | Name             | Example                      | Result           |
|------------|------------------|------------------------------|------------------|
| `til`      | Iota / range     | `til 5`                      | `0 1 2 3 4`      |
| `rev`      | Reverse          | `rev 1 2 3`                  | `3 2 1`          |
| `asc`      | Sort ascending   | `asc 3 1 4 1 5`              | `1 1 3 4 5`      |
| `desc`     | Sort descending  | `desc 3 1 4 1 5`             | `5 4 3 1 1`      |
| `distinct` | Unique values    | `distinct 1 2 1 3 2`         | `1 2 3`          |
| `group`    | Group by value   | `group `a`b`a`c`b`           | dict of indices  |
| `where`    | Bool → indices   | `where 10010b`               | `0 3`            |
| `flip`     | Transpose        | `flip (1 2; 3 4)`            | `(1 3; 2 4)`     |
| `raze`     | Flatten          | `raze (1 2; 3 4 5)`          | `1 2 3 4 5`      |
| `,`        | Join / concat    | `1 2 3 , 4 5`                | `1 2 3 4 5`      |
| `#`        | Take / count     | `3 # 1 2 3 4 5`              | `1 2 3`          |
| `_`        | Drop             | `2 _ 1 2 3 4 5`              | `3 4 5`          |
| `?`        | Find             | `1 2 3 ? 2`                  | `1` (index)      |
| `in`       | Membership       | `1 2 3 in 2 4 6`             | `010b`           |
| `cross`    | Cross product    | `1 2 cross 10 20`            | all combinations |

---

## 5. Iterators (Higher-Order Operators)

Iterators modify how a function is applied. They correspond to APL's operators:

| Iterator | Name          | Syntax          | Description                                    |
|----------|---------------|-----------------|------------------------------------------------|
| `'`      | Each          | `f' x`          | Apply `f` to each element of `x`               |
| `/'`     | Each-right    | `x f/' y`       | Hold `x` fixed, apply `f` to each of `y`       |
| `\\'`    | Each-left     | `x f\\' y`      | Hold `y` fixed, apply `f` to each of `x`       |
| `/`      | Over (reduce) | `f/ x`          | Left fold: `f(f(f(x0,x1),x2),x3)...`           |
| `\\`     | Scan          | `f\\ x`         | Left fold, keeping intermediates                |
| `':`     | Each-prior    | `f': x`         | Apply `f` to consecutive pairs: `f(x[i],x[i-1])`|

### Examples

```
(+/) 1 2 3 4                    / => 10 (reduce with +)
(+\) 1 2 3 4                    / => 1 3 6 10 (scan with +)
(-':) 10 13 17 22               / => 10 3 4 5 (deltas)
count' (1 2; 3 4 5; ,6)         / => 2 3 1 (count each sub-list)
```

---

## 6. Time-Series Primitives

These are built-in operations purpose-built for time-series data, with no equivalent in standard APL. They operate on timestamp-indexed data.

### Time Bucketing

```
xbar[dur; ts_vec]               / Bucket timestamps by duration

/ Example: bucket to 5-minute intervals
xbar[5m; timestamps]            / => truncated timestamps
```

### Windowed Aggregation

Sliding-window variants of all aggregation functions:

```
mavg[n; vec]                    / Moving average (window size n)
msum[n; vec]                    / Moving sum
mmin[n; vec]                    / Moving minimum
mmax[n; vec]                    / Moving maximum
mdev[n; vec]                    / Moving std deviation
mcount[n; vec]                  / Moving count (non-null)

/ Example: 10-point moving average of CPU usage
mavg[10; cpu.usage_idle]
```

### Temporal Arithmetic

```
ts + dur                        / Shift forward
ts - dur                        / Shift backward
ts - ts                         / Duration between
```

### Temporal Extraction

```
ts.year                         / => 2024
ts.month                        / => 1
ts.day                          / => 15
ts.hour                         / => 14
ts.minute                       / => 30
ts.second                       / => 0
ts.week                         / => ISO week number
ts.dow                          / => day of week (0=Mon)
```

### Alignment & Resampling

```
resample[dur; ts_vec; val_vec; agg_fn]

/ Example: resample 1-second data to 1-minute averages
resample[1m; timestamps; values; avg]
```

### As-of Join (Point-in-Time Lookup)

```
asof[ts_left; val_left; ts_right; val_right]

/ For each timestamp in ts_left, find the most recent value from ts_right
/ Critical for financial data: "what was the price at trade time?"
```

---

## 7. Database Integration

### Measurement Access

Measurements (tables) are accessed by name. Fields (columns) are accessed with dot notation. The database is the implicit environment — no `FROM` clause needed.

```
cpu                             / => table: all data from 'cpu' measurement
cpu.usage_idle                  / => float vector: the usage_idle column
cpu.ts                          / => timestamp vector: the __timestamp column
```

### Tag Filtering

Tags filter which series are included. The `@` operator applies tag predicates:

```
cpu @ `host = `server01                     / single tag filter
cpu @ (`host = `server01) & (`region = `us-east)  / compound filter
cpu @ `host like "web-*"                    / pattern match
cpu @ `host in `server01`server02`server03  / set membership
```

### Time Range

The `within` operator restricts the time range:

```
cpu within (now[] - 1h; now[])              / last hour
cpu within (2024.01.15D00:00:00; 2024.01.16D00:00:00)  / specific day
```

### Combined Queries

Tag filter and time range compose naturally:

```
cpu @ `host = `server01 within (now[] - 1h; now[])

/ Then operate on the result:
avg cpu.usage_idle @ `host = `server01 within (now[] - 1h; now[])
```

### Select Expression (Structured Output)

For multi-column output with grouping, use `select`:

```
select avg usage_idle, max usage_system
  from cpu
  where host = `server01
  by xbar[5m; ts]

/ Returns a table with columns: ts, avg_usage_idle, max_usage_system
```

The `select` form is syntactic sugar — it compiles to the same primitives:

```
/ Equivalent pipeline form:
cpu @ `host = `server01
  |> {([] ts: xbar[5m; x.ts];
        avg_idle: avg' x.usage_idle by xbar[5m; x.ts];
        max_sys: max' x.usage_system by xbar[5m; x.ts])}
```

---

## 8. Adverbs & Composition

### Function Composition

```
f ∘ g                           / compose: (f ∘ g) x => f g x
f comp g                        / ASCII alias for ∘

avg comp abs                    / mean of absolute values

/ Applied:
(avg comp abs) -3 5 -7 2        / => abs => 3 5 7 2 => avg => 4.25
```

### Partial Application (Projection)

Fix one argument of a dyadic function to create a monadic function:

```
double: 2 *                     / fix left arg: double is {2 * x}
double 21                       / => 42

add10: + 10                     / fix right arg: {x + 10}
add10 5                         / => 15
```

### Trains (Tacit Programming)

Three-function trains (forks) and two-function trains (atops), as in J/BQN:

```
/ Fork: (f g h) x => (f x) g (h x)
(min + max) % 2                 / midrange: (min + max) / 2

/ Atop: (f g) x => f (g x)
(count distinct)                / count of unique values
```

---

## 9. Tables & Dictionaries

### Dictionary Operations

```
d: `a`b`c ! 1 2 3              / create dict
d[`a]                           / => 1 (lookup)
d,(`d ! 4)                      / => `a`b`c`d ! 1 2 3 4 (extend)
key d                           / => `a`b`c
value d                         / => 1 2 3
```

### Table Operations

Tables are collections of same-length vectors with named columns:

```
t: ([] name: `a`b`c; val: 10 20 30)

t[`val]                         / => 10 20 30 (column access)
t[0]                            / => dict: `name`val ! (`a; 10) (row access)
count t                         / => 3

/ Column arithmetic
t[`val] * 2                     / => 20 40 60

/ Add computed column
t,`double ! t[`val] * 2         / table with new column
```

### Group-By (fby — filter-by)

The `by` adverb groups and aggregates:

```
avg x by g                      / average of x, grouped by g

/ Example:
avg cpu.usage_idle by cpu.host
/ => dict: `server01`server02 ! 95.3 87.1
```

---

## 10. Control Flow

PulseLang is expression-oriented. Control flow is handled by conditional expressions and function application, not statements.

### Conditional

```
$[cond; true_expr; false_expr]          / if-else (ternary)
$[c1; e1; c2; e2; c3; e3; default]     / cascading if-elif-else

/ Example:
classify: {$[x > 90; `high; x > 50; `mid; `low]}
classify 75                              / => `mid
classify' 95 42 73                       / => `high `low `mid
```

### Vector Conditional

```
?[bool_vec; true_vec; false_vec]         / element-wise conditional

/ Example:
?[10010b; 100 200 300 400 500; 0]        / => 100 0 0 400 0
```

---

## 11. Error Handling

```
@[f; arg; error_handler]                 / trap errors

/ Example: safe division
safe_div: {[a;b] @[{x % y}; (a;b); {0N}]}
safe_div[10; 0]                          / => 0N (null) instead of error
```

---

## 12. Standard Library

### Math

`abs`, `neg`, `ceil`, `floor`, `sqrt`, `exp`, `log`, `log2`, `log10`,
`sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`,
`round`, `signum`, `reciprocal`

### Statistics

`avg`, `mean`, `med`, `dev`, `var`, `cov`, `cor`,
`sum`, `prd`, `min`, `max`, `count`,
`sums`, `avgs`, `mins`, `maxs`, `prds`,
`mavg`, `msum`, `mmin`, `mmax`, `mdev`, `mcount`,
`ema`, `wma`, `wavg`, `percentile`

### Time-Series

`xbar`, `resample`, `asof`, `fills`, `ffill`, `bfill`,
`deltas` (≡ `-':`, consecutive differences),
`ratios` (consecutive ratios),
`prev`, `next` (shifted values)

### String

`upper`, `lower`, `trim`, `ltrim`, `rtrim`,
`split`, `join`, `like`, `ss` (string search), `ssr` (search & replace)

### Type

`type`, `null`, `count`, `key`, `value`, `flip`,
`string` (→ str), `parse` (str → typed), `cast`

---

## 13. REPL & Execution

### Interactive REPL

```
$ pulsedb lang
PulseLang v0.1.0 — PulseDB Array Language
Connected to localhost:8087 (1.2M active series)

pl> 2 + 3
5

pl> avg cpu.usage_idle @ `host = `server01 within (now[] - 1h; now[])
94.73

pl> mavg[10; cpu.usage_idle @ `host = `server01] |> last
95.1

pl> / define a reusable function
pl> health: {avg x.usage_idle > 90}
pl> health cpu @ `host = `server01
0.97
```

### HTTP API

```
POST /lang HTTP/1.1
Content-Type: application/json

{
  "q": "avg cpu.usage_idle @ `host = `server01 within (now[] - 1h; now[])"
}
```

Response:

```json
{
  "result": 94.73,
  "type": "float",
  "elapsed_ns": 284000
}
```

### Script Execution

```
$ pulsedb lang run analysis.pulse
```

Scripts are `.pulse` files containing PulseLang expressions, one per line (indented continuations).

---

## 14. Execution Model

### Compilation Pipeline

```
Source Text
    │
    ▼
  Lexer ──► Token Stream
    │
    ▼
  Parser ──► AST (Expression Tree)
    │
    ▼
  Type Checker / Inferencer
    │
    ▼
  Query Planner
    ├─ Resolve measurement references → SeriesIndex
    ├─ Push tag predicates → InvertedIndex
    ├─ Push time ranges → SegmentCache (prune segments)
    ├─ Identify aggregation boundaries
    └─ Vectorize element-wise operations
    │
    ▼
  Executor
    ├─ Segment scan (columnar, zero-copy via mmap)
    ├─ MemTable merge (unflushed recent data)
    ├─ Vectorized array operations (SIMD where possible)
    └─ Streaming aggregation
    │
    ▼
  Result (Array / Table / Scalar)
```

### Optimization Strategies

1. **Predicate pushdown** — Tag filters and time ranges are pushed into the segment scan, not applied post-hoc
2. **Projection pushdown** — Only referenced columns are decompressed
3. **Vectorized execution** — Element-wise operations compile to tight loops over contiguous arrays; SIMD auto-vectorization via Rust's `#[target_feature]`
4. **Lazy evaluation** — Pipeline stages are fused; intermediate arrays are not materialized when possible
5. **Common subexpression elimination** — `cpu.usage_idle` referenced twice in an expression is scanned once
6. **Sort elimination** — Operations on time-ordered data skip re-sorting

---

## 15. Grammar (EBNF)

```ebnf
program     = { expr EOL } ;

expr        = assignment
            | pipeline
            | select_expr
            | apply ;

assignment  = IDENT ":" expr ;

pipeline    = apply { "|>" apply } ;

apply       = term { term } ;              (* right-to-left function application *)

term        = atom
            | "(" expr ")"
            | "{" [ "[" params "]" ] expr { ";" expr } "}"   (* lambda *)
            | term "[" args "]"            (* indexing / application *)
            | term "." IDENT               (* member access *)
            | term "@" predicate           (* tag filter *)
            | term "within" "(" expr ";" expr ")"  (* time range *) ;

atom        = INT | FLOAT | STRING | SYMBOL | BOOL | TIMESTAMP | DURATION
            | IDENT
            | NULL
            | vec_literal ;

vec_literal = atom atom { atom } ;         (* space-separated homogeneous values *)

predicate   = "(" predicate ")"
            | SYMBOL "=" atom
            | SYMBOL "<>" atom
            | SYMBOL "like" STRING
            | SYMBOL "in" vec_literal
            | predicate "&" predicate
            | predicate "|" predicate ;

select_expr = "select" field_list
              "from" IDENT
              [ "where" predicate ]
              [ "by" expr ] ;

field_list  = field_expr { "," field_expr } ;
field_expr  = IDENT IDENT                  (* agg_fn field_name *)
            | IDENT                        (* raw field *) ;

params      = IDENT { ";" IDENT } ;
args        = expr { ";" expr } ;

IDENT       = [a-zA-Z_][a-zA-Z0-9_]* ;
INT         = "-"? [0-9]+ ;
FLOAT       = "-"? [0-9]+ "." [0-9]* ( [eE] [+-]? [0-9]+ )? ;
SYMBOL      = "`" [a-zA-Z0-9_./-]+ ;
STRING      = '"' [^"]* '"' ;
BOOL        = [01]+ "b" | "true" | "false" ;
TIMESTAMP   = date "D" time ;
DURATION    = [0-9]+ ("ns"|"us"|"ms"|"s"|"m"|"h"|"d"|"w") ;
NULL        = "0N" [iftu]? ;
```

---

## 16. PulseQL Compatibility

Every PulseQL query has a PulseLang equivalent:

| PulseQL | PulseLang |
|---------|-----------|
| `SELECT mean(usage_idle) FROM cpu WHERE host='server01' GROUP BY time(5m)` | `select avg usage_idle from cpu where host = `server01 by xbar[5m; ts]` |
| `SELECT * FROM cpu WHERE time > now() - 1h ORDER BY time DESC LIMIT 100` | `100 # desc cpu within (now[] - 1h; now[])` |
| `SELECT sum(bytes_in) FROM network WHERE host =~ /web-\d+/ GROUP BY time(1m), host` | `select sum bytes_in from network where host like "web-*" by (xbar[1m; ts]; host)` |
| `SELECT mean(temp) FROM sensors WHERE time BETWEEN '2024-01-01' AND '2024-01-02' FILL(linear)` | `fills[`linear; select avg temp from sensors within (2024.01.01D0; 2024.01.02D0) by xbar[1h; ts]]` |

---

## 17. Example Programs

### 1. Anomaly Detection (Z-Score)

```
/ Flag CPU readings > 3 standard deviations from mean
vals: cpu.usage_idle @ `host = `server01 within (now[] - 24h; now[])
mu: avg vals
sigma: dev vals
zscore: abs (vals - mu) % sigma
anomalies: vals where zscore > 3
```

### 2. Correlation Between Metrics

```
/ Pearson correlation between CPU usage and memory pressure
c: cpu @ `host = `server01 within (now[] - 1h; now[])
m: mem @ `host = `server01 within (now[] - 1h; now[])
cor[c.usage_system; m.used_percent]
```

### 3. Top-N Hosts by Load

```
/ Top 5 hosts by average CPU usage in the last hour
loads: avg cpu.usage_system by cpu.host within (now[] - 1h; now[])
5 # desc loads
```

### 4. Rate of Change (Derivative)

```
/ Requests per second from a monotonic counter
ts: http.ts @ `endpoint = `/api/v1
vals: http.total_requests @ `endpoint = `/api/v1
deltas[vals] % (deltas[ts] % 1000000000)    / convert ns to seconds
```

### 5. Multi-Metric Dashboard Query

```
/ Single expression returning a table with multiple aggregates
select avg usage_idle, max usage_system, avg usage_iowait,
       count usage_idle
  from cpu
  where region = `us-east
  by (xbar[5m; ts]; host)
```

### 6. Exponential Moving Average

```
/ EMA with alpha=0.1 for smoothing noisy sensor data
ema[0.1; sensor.temperature @ `device = `D-42]
```

### 7. Financial OHLC Bars

```
/ Build 1-minute OHLC bars from tick data
ticks: trade @ `instrument = `AAPL within (now[] - 1d; now[])
bars: xbar[1m; ticks.ts]
([] ts:    distinct bars;
    open:  first' ticks.price by bars;
    high:  max'   ticks.price by bars;
    low:   min'   ticks.price by bars;
    close: last'  ticks.price by bars;
    vol:   sum'   ticks.volume by bars)
```

---

## 18. Implementation Phases

### Phase 1 — Core Language (Interpreter)

| # | Task | Effort |
|---|------|--------|
| 1.1 | Token types and lexer | 3h |
| 1.2 | AST node types | 2h |
| 1.3 | Recursive descent parser (exprs, lambdas, vectors, indexing) | 6h |
| 1.4 | Value type system (Scalar, Vec, Dict, Table, Fn) | 3h |
| 1.5 | Tree-walk interpreter with environment/scope | 4h |
| 1.6 | Built-in arithmetic, comparison, logic operators | 3h |
| 1.7 | Built-in aggregation functions (sum, avg, min, max, count, etc.) | 2h |
| 1.8 | Scan operators (sums, avgs, mins, maxs) | 1h |
| 1.9 | Structural ops (til, rev, asc, desc, distinct, where, take, drop) | 2h |
| 1.10 | Iterators (each, over, scan, each-prior) | 3h |
| 1.11 | Pipe operator `\|>` | 1h |
| 1.12 | Unit tests for all pure-language features | 3h |

**Exit criteria**: Can evaluate PulseLang expressions on in-memory arrays with correct results.

### Phase 2 — Database Integration

| # | Task | Effort |
|---|------|--------|
| 2.1 | Measurement access: resolve `cpu` to table scan | 3h |
| 2.2 | Column access: resolve `cpu.usage_idle` to column vector | 2h |
| 2.3 | Tag filter: `@` operator → InvertedIndex lookup | 3h |
| 2.4 | Time range: `within` → segment pruning | 2h |
| 2.5 | `select ... from ... by` sugar → AST transform to primitives | 4h |
| 2.6 | `now[]` built-in | 0.5h |
| 2.7 | Integration tests: write data → query with PulseLang → verify | 3h |

**Exit criteria**: Can query live PulseDB data using PulseLang expressions.

### Phase 3 — Time-Series Primitives

| # | Task | Effort |
|---|------|--------|
| 3.1 | `xbar` (time bucketing) | 1h |
| 3.2 | Moving-window functions (mavg, msum, mmin, mmax, mdev) | 3h |
| 3.3 | `deltas`, `ratios`, `prev`, `next` | 1h |
| 3.4 | `resample` | 2h |
| 3.5 | `ema`, `wma` | 1h |
| 3.6 | `fills`, `ffill`, `bfill` | 1h |
| 3.7 | `asof` join | 3h |
| 3.8 | Temporal extraction (`.year`, `.month`, `.day`, etc.) | 1h |

### Phase 4 — REPL & API

| # | Task | Effort |
|---|------|--------|
| 4.1 | `pulsedb lang` REPL subcommand | 2h |
| 4.2 | `POST /lang` HTTP endpoint | 1h |
| 4.3 | Result formatting (table, JSON, CSV) | 2h |
| 4.4 | Script file loading (`.pulse`) | 1h |
| 4.5 | Error messages with source positions | 2h |

### Phase 5 — Optimization

| # | Task | Effort |
|---|------|--------|
| 5.1 | Predicate & projection pushdown | 4h |
| 5.2 | Vectorized execution (avoid per-element dispatch) | 4h |
| 5.3 | Common subexpression elimination | 2h |
| 5.4 | Lazy pipeline fusion | 4h |
| 5.5 | Benchmarks: PulseLang vs PulseQL for equivalent queries | 2h |

---

## 19. Open Questions

1. **Unicode glyphs** — Should we support APL glyphs (`+/` vs `⊕/`) as aliases? Pros: APL user familiarity. Cons: input difficulty, font requirements.
2. **Mutability** — Should variables be rebindable (`x: 1; x: 2`) or single-assignment? K/Q allows rebinding; pure functional says no.
3. **Stored procedures** — Allow saving named PulseLang functions in the database for reuse across sessions?
4. **Interop** — Should PulseLang be callable from PulseQL (e.g., `SELECT pulse_eval('avg cpu.usage_idle') ...`)? Or should PulseLang fully subsume PulseQL?
5. **SIMD intrinsics** — Worth hand-vectorizing core loops (sum, avg, comparison) for f64 vectors, or rely on auto-vectorization?
6. **Streaming** — Should PulseLang support continuous/streaming queries (`subscribe cpu.usage_idle |> {$[x > 95; alert[]; ()]}`)?

---

*PulseLang: arrays in, arrays out. No ceremony.*

# PulseDB Wiki

> **PulseDB** — A high-performance time-series database written in pure Rust. Columnar storage, type-aware compression, and a SQL-like query language. All from a single binary.

**Repository:** [github.com/matthart1983/pulsedb](https://github.com/matthart1983/pulsedb) · **License:** MIT · **Language:** Rust 100%

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Data Model](#data-model)
4. [Compression Codecs](#compression-codecs)
5. [Write Path](#write-path)
6. [Storage Engine](#storage-engine)
7. [Segment File Format](#segment-file-format)
8. [Indexing](#indexing)
9. [PulseQL Language Reference](#pulseql-language-reference)
10. [Query Engine](#query-engine)
11. [Server & API](#server--api)
12. [Line Protocol](#line-protocol)
13. [Configuration](#configuration)
14. [Module Reference](#module-reference)
15. [Performance Targets](#performance-targets)
16. [Tech Stack & Dependencies](#tech-stack--dependencies)
17. [Building & Testing](#building--testing)
18. [Codebase Statistics](#codebase-statistics)
19. [Roadmap](#roadmap)
20. [Troubleshooting](#troubleshooting)

---

## Project Overview

PulseDB is a purpose-built time-series database written in Rust, optimized for high-throughput ingestion, efficient compression, and fast time-range queries. Designed for metrics, IoT telemetry, financial tick data, and observability workloads.

### Why PulseDB?

Existing time-series databases trade off between performance and simplicity. InfluxDB rewrote its engine multiple times. TimescaleDB bolts onto PostgreSQL's row-oriented storage. VictoriaMetrics is Go with GC pauses. PulseDB is built from scratch in Rust with a single goal: maximum throughput on a single node, with zero dependencies outside the binary.

### Design Principles

1. **Append-only architecture** — No in-place updates. Immutable segments simplify concurrency and crash recovery.
2. **Columnar storage** — Fields stored column-by-column. Same-type values compress dramatically better than row-oriented layouts.
3. **Type-aware compression** — Each data type gets its own codec tuned for time-series patterns.
4. **Zero-copy reads** — Memory-mapped segments avoid serialization overhead on the read path.
5. **Lock-free write path** — WAL append + memtable insert with minimal contention via `parking_lot::RwLock`.
6. **Ecosystem compatibility** — InfluxDB line protocol for ingestion means existing collectors (Telegraf, Prometheus remote_write adapters, IoT agents) work out of the box.

### Design Goals

- **Ingest ≥ 1M data points/sec** on commodity hardware (single node, NVMe SSD)
- **Sub-millisecond queries** on recent data, sub-second on historical ranges
- **10–20× compression ratio** using time-series-aware encodings
- **Zero-copy reads** via memory-mapped columnar segments
- **Simple query language** with SQL-like syntax tailored for time-series

### Non-Goals (v1)

- Distributed clustering / replication (single-node first; clustering is a v2 concern)
- Full SQL compliance (JOINs, subqueries, CTEs)
- ACID transactions (append-only, eventual consistency is acceptable)
- String field indexing (tags are indexed; string fields are stored but not searchable)

---

## Architecture

### System Overview

```
                    ┌──────────────────────────────────────────────────────┐
                    │                   PulseDB Server                    │
                    ├──────────────────────────────────────────────────────┤
                    │                                                      │
  TCP :8086 ──────► │  Line Protocol Parser ──► Engine.write()             │
  (line protocol)   │                                                      │
                    │         ┌─────────────────────────────────────┐      │
                    │         │          Database Engine            │      │
                    │         ├─────────────────────────────────────┤      │
                    │  Write  │                                     │ Read │
                    │  Path   │                                     │ Path │
                    │         │                                     │      │
                    │  ──► WAL ──► MemTable ──► Flush ──► Segment   │      │
                    │         │    (sorted)     │      (columnar)   │      │
                    │         │                 │         ▲         │      │
                    │         │                 ▼         │         │      │
                    │         │            Compactor ─────┘         │      │
                    │         │         (merge + compress)          │      │
                    │         │                                     │      │
                    │         │   SeriesIndex ◄──── InvertedIndex   │      │
                    │         │   (key → ID)       (tag → IDs)     │      │
                    │         └─────────────────────────────────────┘      │
                    │                                                      │
  HTTP :8087 ─────► │  Query Parser ──► Planner ──► Executor ──► JSON     │
  (PulseQL)         │                                                      │
                    └──────────────────────────────────────────────────────┘
```

### Database Engine Components

The `Database` struct in `engine/database.rs` is the top-level coordinator, holding:

| Component | Type | Purpose |
|---|---|---|
| `wal` | `RwLock<Wal>` | Append-only write-ahead log for durability |
| `active` | `RwLock<MemTable>` | Currently receiving writes |
| `frozen` | `RwLock<Vec<FrozenMemTable>>` | Flushed snapshots awaiting GC |
| `series_index` | `RwLock<SeriesIndex>` | Series key → numeric ID mapping |
| `inverted_index` | `RwLock<InvertedIndex>` | Tag → series ID posting lists |
| `segment_cache` | `RwLock<SegmentCache>` | On-disk segment metadata |
| `partition_mgr` | `PartitionManager` | Time-based partition directory management |
| `schema_registry` | `SchemaRegistry` | Schema-on-write field type enforcement |

### Write Path

```
Client ──TCP──► Line Protocol Parser
                   │
                   ▼
              Batch of DataPoints
                   │
                   ├──► Schema validation (type check)
                   ├──► WAL.append(batch)           → Durability first
                   ├──► MemTable.insert(point)      → In-memory indexing
                   │
                   └──► if size_bytes > threshold:
                           ├─ Freeze active MemTable → FrozenMemTable
                           ├─ Swap in new empty MemTable
                           ├─ SeriesIndex.get_or_create(key)
                           ├─ InvertedIndex.index_series(id, tags)
                           ├─ SegmentWriter.write_segment() per series
                           ├─ SegmentCache.add(meta)
                           └─ WAL.truncate()
```

### Read Path

```
Client ──HTTP POST /query──► PulseQL Parser
                                │
                                ▼
                           SelectStatement (AST)
                                │
                                ▼
                           Query Planner
                           ├─ Resolve measurement → series keys
                           ├─ Filter by tag predicates (InvertedIndex)
                           ├─ Resolve time range → prune segments
                           ├─ Check MemTable for unflushed data
                           └─ Produce QueryPlan
                                │
                                ▼
                           Query Executor
                           ├─ Scan matching segments (decompress columns)
                           ├─ Scan active MemTable
                           ├─ Merge results (time-ordered)
                           └─ Apply offset/limit
                                │
                                ▼
                           Aggregator
                           ├─ GROUP BY time(interval): bucket timestamps
                           ├─ GROUP BY tag: split by tag values
                           ├─ Compute aggregation functions
                           ├─ Apply FILL policy
                           └─ Return QueryResult → JSON
```

---

## Data Model

### Series

A time series is uniquely identified by a **measurement name** + **tag set**:

```
cpu,host=server01,region=us-east usage_idle=98.2,usage_system=1.3 1672531200000000000
│    │                            │                                 │
│    └─ tags (indexed)            └─ fields (values)                └─ timestamp (ns)
measurement
```

### Components

| Component | Type | Description |
|---|---|---|
| **Measurement** | `String` | Logical grouping (e.g., `cpu`, `mem`, `http_requests`). Analogous to a table name. |
| **Tags** | `BTreeMap<String, String>` | Key-value string pairs. Always indexed, always sorted. Used for filtering and grouping. |
| **Fields** | `BTreeMap<String, FieldValue>` | The actual data values. Not indexed. |
| **Timestamp** | `i64` | Nanosecond Unix epoch. If omitted on write, the server assigns `now()`. |

### Field Types

```rust
pub enum FieldValue {
    Float(f64),      // Default numeric type
    Integer(i64),    // Suffix: 42i
    UInteger(u64),   // Suffix: 42u
    Boolean(bool),   // t/f/true/false/TRUE/FALSE
    String(String),  // "quoted string"
}
```

### Series Key

The unique identifier for a series: `measurement + sorted(tags)`. Example: `cpu,host=server01,region=us-east`. This string maps to a compact `SeriesId(u64)` stored in the series index.

### Schema-on-Write

PulseDB enforces **schema-on-write** via the `SchemaRegistry`:

- The **first time** a field name appears for a measurement, its type is recorded
- **Subsequent writes** to the same field must use the same type
- **Type mismatches are rejected** with a descriptive error
- Different measurements have independent schemas

```
✓  cpu usage=42.0     → registers "usage" as Float
✓  cpu usage=99.0     → same type, accepted
✗  cpu usage=42i      → type mismatch: expected Float, got Integer
✓  mem usage=1024i    → independent measurement, registers as Integer
```

---

## Compression Codecs

PulseDB uses type-aware encodings tuned for time-series patterns, then wraps each column in LZ4 for an additional compression layer.

### Overview

| Data Type | Codec | Algorithm | Typical Ratio | Source |
|---|---|---|---|---|
| Timestamps | Delta-of-delta | `delta[i] - delta[i-1]` → zigzag → varint | 10–50× | `encoding/timestamp.rs` |
| Floats | Gorilla XOR | XOR consecutive values → leading/meaningful bits | 8–15× | `encoding/float.rs` |
| Integers | Delta + zigzag | Delta encode → zigzag → varint | 5–20× | `encoding/integer.rs` |
| Booleans | Bit-packing | 8 values per byte, MSB first | 8× | `encoding/boolean.rs` |

**Combined**: For typical metric workloads (regular timestamps, slowly changing floats), expect **12–25× total compression** over raw storage.

### Timestamp Codec — Delta-of-Delta

Timestamps in time-series data are typically monotonically increasing with roughly constant intervals. Delta-of-delta encoding exploits this:

```
Raw timestamps:  1000, 1010, 1020, 1030, 1040
Deltas:                  10,   10,   10,   10
Delta-of-deltas:               0,    0,    0   ← mostly zeros!
```

**Encoding pipeline:**
1. Store count as varint
2. Store first timestamp as raw 8-byte LE
3. Store first delta as zigzag varint
4. Store subsequent delta-of-deltas as zigzag varints

**Helper functions:**
- `zigzag_encode(n)` — Maps signed integers to unsigned: 0→0, -1→1, 1→2, -2→3
- `encode_varint(value)` — LEB128 variable-length encoding: values < 128 use 1 byte
- Constant-interval timestamps compress to **~1 byte per point** (vs 8 bytes raw)

### Float Codec — Gorilla XOR

Based on the Facebook Gorilla paper (Pelkonen et al., 2015). Consecutive float values in time-series data share many bits:

**Encoding per value (after first):**
1. XOR current value with previous value
2. If XOR == 0: write single `0` bit (values identical)
3. If XOR != 0: write `1` bit, then:
   - 6 bits: leading zero count (0–63)
   - 6 bits: meaningful bit length minus 1 (0–63)
   - N bits: the meaningful (non-zero) bits

Uses custom `BitWriter`/`BitReader` for bit-level I/O with MSB-first ordering.

### Integer Codec — Delta + Zigzag

Integer values (counters, gauges) change by small amounts between consecutive points:

1. Store count as varint
2. Store first value as raw 8-byte LE
3. For subsequent values: compute delta → zigzag encode → varint encode

### Boolean Codec — Bit-Packing

Booleans are packed 8 per byte, MSB first:
- 4-byte u32 LE count prefix
- Packed bits, final byte zero-padded on the right
- 1000 booleans → 4 + 125 = 129 bytes (vs 1000 bytes)

---

## Write Path

### Write-Ahead Log

The WAL (`engine/wal.rs`) provides crash durability by logging all writes before they reach the memtable.

**Entry format:**

```
┌───────────┬──────────┬────────────┬─────────────────────┐
│ len: u32  │ crc: u32 │ type: u8   │ payload: [u8; len-1]│
│ (LE)      │ (LE)     │ (1=Write)  │ (JSON batch)        │
└───────────┴──────────┴────────────┴─────────────────────┘
```

- **CRC32**: Computed over `payload` only. Detects corruption on recovery.
- **Batch writes**: Multiple points packed into a single WAL entry for throughput.
- **Sequential recovery**: Entries read front-to-back. Corrupted/truncated entries are skipped.

**Fsync policies:**

| Policy | Behavior | Trade-off |
|---|---|---|
| `every` | Fsync after every write | Maximum durability, lowest throughput |
| `batch` | Fsync periodically (default) | Balance of durability and throughput |
| `none` | OS page cache decides | Maximum throughput, risk of data loss on crash |

**Operations:**
- `append(points)` — Serialize to JSON, compute CRC32, write entry
- `recover()` — Replay all valid entries, skip corrupted ones
- `truncate()` — Clear WAL after successful segment flush

### MemTable

The MemTable (`engine/memtable.rs`) is the in-memory write buffer:

**Data structure:**
```
BTreeMap<series_key, BTreeMap<timestamp, BTreeMap<field_name, FieldValue>>>
```

- **Sorted by series key and timestamp** — BTreeMap ensures natural ordering
- **Tracks approximate size** — `size_bytes` estimates memory usage for flush decisions
- **Last-write-wins** — Writing to the same (series, timestamp) overwrites previous fields

**Lifecycle:**
1. `insert(point)` — Add data point, update size estimate
2. `should_flush()` → When `size_bytes ≥ memtable_size_bytes` (default 64 MiB)
3. `freeze()` — Consume MemTable → immutable `FrozenMemTable`
4. New empty MemTable swapped in atomically

### Database Coordinator

The `Database::write()` method orchestrates the full write path:

1. **Schema validation** — `SchemaRegistry.validate()` checks field types
2. **WAL append** — Durable write under WAL write lock
3. **MemTable insert** — Insert each point under active memtable write lock
4. **Flush check** — If memtable exceeds threshold, trigger rotation
5. **Rotation** — Swap memtable, flush to segments, index series, truncate WAL

---

## Storage Engine

### Time-Based Partitioning

Data is partitioned into fixed-duration time windows (default: 1 hour). Each partition maps to a directory containing segment files:

```
pulsedb_data/
├── wal/
│   └── wal.log
├── partitions/
│   ├── 2024-01-15T14/              ← 1-hour partition
│   │   ├── cpu_host-server01.seg   ← Compressed columnar segment
│   │   └── mem_host-server01.seg
│   └── 2024-01-15T15/
│       └── ...
├── index/
│   ├── series.idx
│   └── tags.idx
└── meta/
    └── measurements.json
```

The `PartitionManager` computes partition keys from timestamps:
- `partition_key_for(1705329000_000_000_000)` → `"2024-01-15T14"` (14:30 UTC → hour 14)
- Enables **time-range pruning**: queries skip entire partition directories outside the time range
- Enables **easy data deletion**: drop old data by removing partition directories

### Segment Cache

The `SegmentCache` (`storage/cache.rs`) maintains in-memory metadata for all on-disk segments:

```rust
pub struct SegmentMeta {
    pub path: PathBuf,
    pub series_key: String,
    pub min_time: i64,
    pub max_time: i64,
    pub point_count: u64,
}
```

Key operations:
- `segments_for_range(series_key, min_time, max_time)` — Find overlapping segments
- `series_keys_for_measurement(name)` — List all series keys for a measurement
- Used by the query planner to **avoid reading segment files** during planning

### Background Compaction

The `Compactor` (`storage/compactor.rs`) merges small segments within the same partition:

**Process:**
1. List all `.seg` files in a partition directory
2. Group by series key
3. For groups with 2+ segments:
   - Read all timestamps and fields from each segment
   - Sort by timestamp, deduplicate (last value wins for overlapping timestamps)
   - Write merged segment to temp file
   - Delete original segments
   - Rename temp to final

**Result:** Fewer files → faster scans, better compression from larger batches.

Runs every **60 seconds** in a background tokio task.

### Retention Policies

The `RetentionPolicy` (`storage/retention.rs`) auto-drops old data:

- Configured via `--retention` flag (e.g., `30d`, `1y`)
- Scans partition directories, parses timestamps from directory names
- Deletes entire directories older than the cutoff
- Runs every **60 seconds** alongside compaction

---

## Segment File Format

Segments are immutable on-disk files containing time-sorted data for a single series. Data is stored column-by-column with type-aware compression.

### Layout

```
┌──────────────────────────────────────────────────────────────────┐
│ Magic: "PLSDB001" (8 bytes)                                     │
├──────────────────────────────────────────────────────────────────┤
│ Header                                                           │
│   min_timestamp: i64 LE                                          │
│   max_timestamp: i64 LE                                          │
│   point_count:   u64 LE                                          │
│   column_count:  u32 LE                                          │
│   series_key_len: u16 LE                                         │
│   series_key:    [u8; series_key_len]                            │
├──────────────────────────────────────────────────────────────────┤
│ Column Block: __timestamp                                        │
│   name_len: u16 LE │ name: bytes │ enc: u8 │ comp_len: u32 LE   │
│   compressed_data: [u8; comp_len]   (LZ4 → delta-of-delta)      │
├──────────────────────────────────────────────────────────────────┤
│ Column Block: field_0                                            │
│   name_len: u16 LE │ name: bytes │ enc: u8 │ comp_len: u32 LE   │
│   compressed_data: [u8; comp_len]   (LZ4 → gorilla/delta/bits)  │
├──────────────────────────────────────────────────────────────────┤
│ Column Block: field_1 ...                                        │
├──────────────────────────────────────────────────────────────────┤
│ Footer                                                           │
│   checksum: u32 LE (CRC32 of everything above)                   │
└──────────────────────────────────────────────────────────────────┘
```

### Encoding Type Markers

| Marker | Type | Codec |
|---|---|---|
| `1` | Timestamp | Delta-of-delta + zigzag + varint |
| `2` | Float | Gorilla XOR |
| `3` | Integer | Delta + zigzag + varint |
| `4` | Boolean | Bit-packing |

### Reading

The `SegmentReader` parses the file format:
- Validates magic bytes and CRC32 checksum
- Parses header and all column blocks
- Decompresses columns on demand: LZ4 decompress → type-specific decode
- `read_timestamps()` — Decode timestamp column
- `read_column(name)` — Decode a specific field column
- `field_names()` — List all field columns (excluding `__timestamp`)

### Compression Effectiveness

For 10,000 regular-interval timestamps with slowly changing float values, the segment file is **less than half** the size of raw uncompressed storage.

---

## Indexing

### Series Index

The `SeriesIndex` (`index/series.rs`) maps series key strings to compact numeric IDs:

```rust
pub struct SeriesIndex {
    map: HashMap<String, SeriesId>,
    next_id: u64,  // starts at 1, auto-increments
}
```

- `get_or_create(key)` — Returns existing ID or assigns a new sequential one
- `get(key)` — Lookup without creation
- `series_count()` — Total registered series

IDs are used internally for efficient posting list operations in the inverted index.

### Inverted Index

The `InvertedIndex` (`index/inverted.rs`) enables fast tag-based series lookup:

**Structure:** `HashMap<"tagkey=tagvalue", Vec<SeriesId>>` (sorted posting lists)

**Operations:**
- `index_series(id, tags)` — Add series to posting lists for each tag pair
- `lookup(tag_key, tag_value)` — Return the posting list for a specific tag
- `intersect(lists)` — O(n+m) sorted merge for AND semantics; starts with shortest list
- `union(lists)` — Sorted merge for OR semantics

**Example:**
```
index_series(1, {host=web1, region=us})
index_series(2, {host=web2, region=us})
index_series(3, {host=web1, region=eu})

lookup("host", "web1")     → [1, 3]
lookup("region", "us")     → [1, 2]
intersect([1,3], [1,2])    → [1]     ← host=web1 AND region=us
```

---

## PulseQL Language Reference

PulseQL is a SQL-like query language purpose-built for time-series data.

### Syntax

```sql
SELECT <fields>
FROM <measurement>
[WHERE <conditions>]
[GROUP BY <groupings>]
[FILL(<policy>)]
[ORDER BY time [ASC|DESC]]
[LIMIT <n>]
[OFFSET <n>]
```

### SELECT Clause

| Expression | Example | Description |
|---|---|---|
| Field reference | `usage_idle` | Raw field value |
| Wildcard | `*` | All fields |
| Aggregation | `mean(usage_idle)` | Aggregate function |
| Aliased | `mean(usage) AS avg` | Named result column |
| Multiple | `min(v), max(v), mean(v)` | Multiple aggregations |

### Aggregation Functions

| Function | Description |
|---|---|
| `count(field)` | Number of non-null values |
| `sum(field)` | Sum of values |
| `mean(field)` / `avg(field)` | Arithmetic mean |
| `min(field)` | Minimum value |
| `max(field)` | Maximum value |
| `first(field)` | Value at earliest timestamp |
| `last(field)` | Value at latest timestamp |
| `stddev(field)` | Population standard deviation |
| `percentile(field, N)` | Nth percentile (0–100) |

### WHERE Clause

**Tag predicates:**

| Operator | Example | Description |
|---|---|---|
| `=` | `host = 'server01'` | Exact match |
| `!=` | `host != 'test'` | Not equal |
| `=~` | `host =~ /web-\d+/` | Regex match |
| `!~` | `host !~ /test.*/` | Regex not match |

**Time predicates:**

| Form | Example |
|---|---|
| Relative | `time > now() - 1h` |
| Absolute (ns) | `time > 1704067200000000000` |
| Date string | `time BETWEEN '2024-01-01' AND '2024-02-01'` |
| Comparison | `time >= now() - 30m` |

**Logical operators:** `AND` (binds tighter) · `OR` · parentheses for grouping

### GROUP BY Clause

```sql
GROUP BY time(5m)              -- Time bucketing only
GROUP BY time(1h), host        -- Time + tag grouping
GROUP BY region, host          -- Tag grouping only
```

### FILL Policy

Controls missing time buckets when using `GROUP BY time()`:

| Policy | Behavior |
|---|---|
| `FILL(none)` | Omit empty buckets |
| `FILL(null)` | Include with null values |
| `FILL(linear)` | Linear interpolation |
| `FILL(previous)` | Carry forward last value |
| `FILL(0)` | Fill with specific value |

### Duration Syntax

`1ns` · `100us` · `5ms` · `10s` · `5m` · `1h` · `7d` · `2w`

### Example Queries

```sql
-- Aggregation with time bucketing
SELECT mean(usage_idle), max(usage_system)
FROM cpu
WHERE host = 'server01' AND time > now() - 1h
GROUP BY time(5m)

-- Multi-tag filter with regex
SELECT sum(bytes_in)
FROM network
WHERE region = 'us-east' AND host =~ /web-\d+/
GROUP BY time(1m), host

-- Raw data retrieval with ordering
SELECT *
FROM temperature
WHERE sensor_id = 'T-42'
  AND time BETWEEN '2024-01-01' AND '2024-01-02'
ORDER BY time DESC
LIMIT 1000

-- Downsampling with fill
SELECT mean(value) AS avg_temp, min(value), max(value)
FROM temperature
GROUP BY time(1h), location
FILL(linear)

-- Percentile analysis
SELECT percentile(latency, 95) AS p95, percentile(latency, 99) AS p99
FROM http_requests
WHERE service = 'api'
GROUP BY time(5m)
```

---

## Query Engine

The query engine transforms PulseQL text into results through four stages: lexing, parsing, planning, and execution with aggregation.

### Lexer (`query/lexer.rs` — 561 lines)

Tokenizes PulseQL input into a stream of `Token` variants:

- **Keywords** (case-insensitive): `SELECT`, `FROM`, `WHERE`, `GROUP`, `BY`, `ORDER`, `FILL`, `LIMIT`, `OFFSET`, `AND`, `OR`, `AS`, `BETWEEN`, `IN`, `ASC`, `DESC`, `NOW`, `TIME`
- **Literals**: `StringLit('...')`, `NumberLit(3.14)`, `IntLit(42)`, `DurationLit(5, Minutes)`, `RegexLit(/pattern/)`
- **Operators**: `=`, `!=`, `>`, `<`, `>=`, `<=`, `=~`, `!~`, `-`
- **Punctuation**: `,`, `(`, `)`, `*`

Supports `peek()` for lookahead and `next_token()` for consumption.

### Parser (`query/parser.rs` — 759 lines)

Recursive descent parser producing `SelectStatement` AST nodes:

```rust
pub struct SelectStatement {
    pub fields: Vec<FieldExpr>,       // SELECT clause
    pub measurement: String,          // FROM clause
    pub condition: Option<WhereClause>, // WHERE clause (tree)
    pub group_by: Option<GroupBy>,    // GROUP BY clause
    pub fill: Option<FillPolicy>,    // FILL clause
    pub order_by: Option<OrderBy>,   // ORDER BY clause
    pub limit: Option<u64>,          // LIMIT
    pub offset: Option<u64>,         // OFFSET
}
```

**WHERE parsing** implements operator precedence:
- `parse_or()` → `parse_and()` → `parse_predicate()` (AND binds tighter than OR)
- Supports parenthesized sub-expressions
- Time predicates: `now()`, `now() - duration`, nanosecond literals, date strings

### Planner (`query/planner.rs` — 463 lines)

Resolves a parsed `SelectStatement` into an executable `QueryPlan`:

1. **Series resolution**: Collect series keys from `SegmentCache` + active `MemTable`
2. **Tag filtering**: Evaluate tag predicates via `InvertedIndex` posting lists + string matching with regex support
3. **Time range extraction**: Walk the WHERE clause, extract min/max bounds
4. **Projection**: Determine which fields to read (projection pushdown)

```rust
pub struct QueryPlan {
    pub measurement: String,
    pub fields: Vec<FieldExpr>,
    pub series_keys: Vec<String>,
    pub time_range: (i64, i64),
    pub group_by: Option<GroupBy>,
    pub fill: Option<FillPolicy>,
    pub order_desc: bool,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}
```

### Executor (`query/executor.rs` — 416 lines)

Scans segments and the active memtable to produce raw `ScanRow` results:

1. For each matching series key:
   - Scan segments: open reader → decompress timestamps → filter by time range → read requested field columns
   - Scan active memtable: iterate matching series entries
2. Sort by timestamp (ASC or DESC)
3. Apply OFFSET and LIMIT

### Aggregator (`query/aggregator.rs` — 583 lines)

Computes aggregations on scan results:

1. **Group rows** by `(time_bucket, tag_group_key)`:
   - Time bucketing: `(timestamp / interval) * interval`
   - Tag grouping: extract tag values from series key strings
2. **Compute aggregations** for each group (count, sum, mean, min, max, first, last, stddev, percentile)
3. **Fill missing buckets** according to FILL policy
4. **Return `QueryResult`** with columns and rows

---

## Server & API

### TCP Ingestion Server (`server/tcp.rs`)

- Listens on port **8086** (default) via `tokio::net::TcpListener`
- Accepts concurrent connections, each spawned as a tokio task
- Per-connection: reads lines via `BufReader`, parses line protocol, batches up to 1000 points before writing
- Auto-assigns `now()` timestamp if missing from the data point
- Skips empty lines and comments (`#`)

### HTTP Query API (`server/http.rs`)

Built with `axum`, listens on port **8087** (default):

| Endpoint | Method | Description |
|---|---|---|
| `/query` | `POST` | Execute PulseQL query, return JSON results |
| `/write` | `POST` | Ingest line protocol data over HTTP |
| `/health` | `GET` | Liveness check: `{"status": "ok"}` |
| `/status` | `GET` | Engine statistics |

**Query request/response:**

```bash
# Request
curl -X POST http://localhost:8087/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "SELECT mean(usage) FROM cpu GROUP BY time(5m)"}'

# Response (InfluxDB-compatible JSON)
{
  "results": [{
    "series": [{
      "name": "cpu",
      "columns": ["time", "mean(usage)"],
      "values": [[1704067200000000000, 42.5], ...]
    }]
  }]
}
```

**Status response:**

```json
{
  "version": "0.1.0",
  "series_count": 1234,
  "points_in_memtable": 56789,
  "segment_count": 42
}
```

---

## Line Protocol

PulseDB supports the InfluxDB line protocol (`server/protocol.rs` — 457 lines) for data ingestion.

### Format

```
<measurement>[,<tag1>=<val1>[,<tag2>=<val2>...]] <field1>=<fval1>[,<field2>=<fval2>...] [<timestamp_ns>]
```

### Field Type Suffixes

| Suffix | Type | Example |
|---|---|---|
| *(none)* | Float | `value=3.14` |
| `i` | Integer | `count=42i` |
| `u` | Unsigned | `bytes=1024u` |
| `t`/`f`/`true`/`false` | Boolean | `healthy=t` |
| `"..."` | String | `msg="hello world"` |

### Examples

```bash
# Basic point
cpu,host=server01 value=1.0 1609459200000000000

# Multiple tags and fields
weather,city=nyc,station=central temp=72.5,humidity=45i 1609459200000000000

# All field types
m,t=v flt=3.14,int=42i,uint=100u,b=true,s="hello" 1000

# No tags, no timestamp (auto-assigned)
cpu value=42.0

# Batch write (multiple lines)
cpu,host=server01 usage=98.2 1672531200000000000
cpu,host=server02 usage=95.1 1672531200000000000
mem,host=server01 available=8589934592i 1672531200000000000
```

### Parsing Details

- Comments (`#` prefix) and empty lines are skipped in batch mode
- Quoted string fields may contain spaces and commas
- Scientific notation supported for floats (e.g., `1.5e10`)
- Negative values supported for floats and integers

---

## Configuration

### CLI Flags

```bash
pulsedb server [OPTIONS]
```

| Flag | Default | Description |
|---|---|---|
| `--data-dir` | `./pulsedb_data` | Root directory for all data |
| `--tcp-port` | `8086` | Line protocol ingestion port |
| `--http-port` | `8087` | HTTP query API port |
| `--wal-fsync` | `batch` | WAL fsync policy: `every` / `batch` / `none` |
| `--memtable-size` | `67108864` (64 MiB) | Flush threshold for in-memory buffer |
| `--segment-duration` | `3600` | Partition duration in seconds (1 hour) |
| `--retention` | `0` (keep all) | Auto-drop data older than N seconds |
| `--log-level` | `info` | Logging: `trace` / `debug` / `info` / `warn` / `error` |

### Data Directory Layout

```
pulsedb_data/
├── wal/
│   └── wal.log                    # Write-ahead log
├── partitions/
│   ├── 2024-01-15T14/             # Hourly partition
│   │   ├── cpu_host-server01.seg  # Compressed columnar segment
│   │   └── mem_host-server01.seg
│   └── 2024-01-15T15/
│       └── ...
├── index/
│   ├── series.idx                 # Series key → ID mapping
│   └── tags.idx                   # Tag inverted index
└── meta/
    └── measurements.json          # Schema (field names + types)
```

### Subcommands

```bash
pulsedb server    # Start the database server
pulsedb version   # Print version information
```

---

## Module Reference

### Source Files (37 files, 7,753 lines)

| File | Lines | Description |
|---|---|---|
| `src/main.rs` | 110 | CLI entry point, server bootstrap, background maintenance tasks |
| `src/lib.rs` | 8 | Module re-exports (cli, encoding, engine, index, model, query, server, storage) |

#### Model (`src/model/`)

| File | Lines | Description |
|---|---|---|
| `mod.rs` | 7 | Module exports: DataPoint, FieldValue, Tags, SeriesId, SeriesKey, SchemaRegistry |
| `point.rs` | 58 | `DataPoint` struct, `FieldValue` enum, `Tags` type alias, `series_key()` method |
| `series.rs` | 28 | `SeriesId(u64)` wrapper, `SeriesKey` struct with canonical key formatting |
| `schema.rs` | 137 | `SchemaRegistry` (schema-on-write enforcement), `FieldType` enum, `MeasurementSchema` |

#### Encoding (`src/encoding/`)

| File | Lines | Description |
|---|---|---|
| `mod.rs` | 9 | Module exports |
| `timestamp.rs` | 276 | Delta-of-delta + zigzag + varint timestamp compression |
| `float.rs` | 329 | Gorilla XOR float compression with BitWriter/BitReader |
| `integer.rs` | 132 | Delta + zigzag + varint integer compression |
| `boolean.rs` | 149 | Bit-packing boolean compression (8 per byte) |

#### Engine (`src/engine/`)

| File | Lines | Description |
|---|---|---|
| `mod.rs` | 9 | Module exports |
| `config.rs` | 86 | `EngineConfig` struct, `FsyncPolicy` enum, defaults |
| `database.rs` | 487 | `Database` coordinator: write path, flush, rotation, query dispatch |
| `wal.rs` | 237 | Append-only WAL with CRC32 checksums and crash recovery |
| `memtable.rs` | 184 | `MemTable` (write buffer) and `FrozenMemTable` (immutable snapshot) |

#### Index (`src/index/`)

| File | Lines | Description |
|---|---|---|
| `mod.rs` | 5 | Module exports |
| `series.rs` | 105 | `SeriesIndex`: series key → numeric ID mapping |
| `inverted.rs` | 246 | `InvertedIndex`: tag posting lists with intersect/union operations |

#### Query (`src/query/`)

| File | Lines | Description |
|---|---|---|
| `mod.rs` | 6 | Module exports |
| `ast.rs` | 155 | AST types: `SelectStatement`, `FieldExpr`, `AggFunc`, `WhereClause`, `GroupBy`, etc. |
| `lexer.rs` | 561 | PulseQL tokenizer with keyword recognition, literal parsing, regex support |
| `parser.rs` | 759 | Recursive descent parser with operator precedence for AND/OR |
| `planner.rs` | 463 | Query planner: series resolution, tag filtering, time range extraction |
| `executor.rs` | 416 | Query executor: segment scanning, memtable scanning, result merging |
| `aggregator.rs` | 583 | Aggregation engine: GROUP BY, 10 aggregation functions, FILL policies |

#### Server (`src/server/`)

| File | Lines | Description |
|---|---|---|
| `mod.rs` | 3 | Module exports |
| `tcp.rs` | 60 | TCP line protocol server (tokio, per-connection handler, batching) |
| `http.rs` | 164 | HTTP API server (axum: /query, /write, /health, /status) |
| `protocol.rs` | 457 | InfluxDB line protocol parser with full type support |

#### Storage (`src/storage/`)

| File | Lines | Description |
|---|---|---|
| `mod.rs` | 11 | Module exports |
| `segment.rs` | 555 | Columnar segment file reader/writer (PLSDB001 format) |
| `partition.rs` | 170 | Time-based partitioning (hourly directories) |
| `cache.rs` | 147 | In-memory segment metadata cache for query planning |
| `compactor.rs` | 472 | Background segment merging with deduplication |
| `retention.rs` | 134 | Retention policy enforcement (auto-drop old partitions) |

---

## Performance Targets

| Metric | Target | Method |
|---|---|---|
| Write throughput | ≥ 1M points/sec | Batch of 10K points × 100 batches, wall clock |
| Single-point write latency | < 10μs | WAL append + memtable insert, p99 |
| Time-range query (1h, 1 series) | < 1ms | Scan 1 segment, return raw |
| Time-range query (1h, 1K series) | < 50ms | Scan + merge 1000 segments |
| Aggregation (24h, GROUP BY 5m) | < 10ms | Scan 24 segments, 288 buckets |
| Compression ratio (float metrics) | ≥ 10× | Regular 10s-interval CPU metrics |
| Memory (1M active series) | < 2GB | Series index + inverted index + memtable |
| Segment flush (1M points) | < 100ms | Encode + compress + write to disk |
| Startup recovery (10GB data) | < 5s | WAL replay + index load |
| TCP ingestion throughput | ≥ 500K lines/sec | Sustained TCP write, single connection |

### Benchmark Workloads

| Workload | Description |
|---|---|
| **Telegraf CPU** | 10 fields, 10s interval, 100 hosts → 1000 series, ~6M points/hour |
| **IoT Temperature** | 1 field, 1s interval, 10K sensors → 10K series, ~36M points/hour |
| **Financial Tick** | 4 fields (OHLC), irregular timestamps, 1K instruments |
| **High Cardinality** | 1M unique series, 1 field each, verifying index performance |

### Criterion Benchmarks

Three benchmark suites are included (`benches/`):

- **`ingestion.rs`** — Points/sec for batch writes of varying sizes
- **`query.rs`** — Latency for time-range scans and aggregation queries
- **`compression.rs`** — Ratio and throughput for each codec

---

## Tech Stack & Dependencies

| Layer | Crate | Purpose |
|---|---|---|
| Async Runtime | `tokio` (full features) | TCP/HTTP server, background tasks, signal handling |
| HTTP Framework | `axum` 0.7 | HTTP API server with routing and JSON extraction |
| HTTP Core | `hyper` 1.0 | HTTP/1.1 protocol support |
| Compression | `lz4_flex` 0.11 | Fast outer compression (~4GB/s decompression) |
| Checksums | `crc32fast` 1.x | WAL and segment integrity verification |
| Concurrency | `parking_lot` 0.12 | Low-overhead RwLock (no poisoning) |
| CLI | `clap` 4.x (derive) | Command-line argument parsing |
| Serialization | `serde` + `serde_json` | WAL payload, HTTP requests/responses |
| Time | `chrono` 0.4 | Partition key formatting, date parsing |
| Hashing | `xxhash-rust` 0.8 (xxh3) | Fast non-cryptographic hashing |
| Memory Mapping | `memmap2` 0.9 | Zero-copy segment reads |
| Logging | `tracing` + `tracing-subscriber` | Structured logging with env filter |
| Errors | `thiserror` 2.x + `anyhow` 1.x | Error handling and propagation |
| Regex | `regex` 1.x | Tag pattern matching in WHERE clauses |
| Unique IDs | `uuid` 1.x (v4) | UUID generation |
| Byte Buffers | `bytes` 1.x | Efficient byte manipulation |
| Channels | `crossbeam-channel` 0.5 | High-performance MPMC channels |

### Dev Dependencies

| Crate | Purpose |
|---|---|
| `criterion` 0.5 | Benchmarking framework |
| `tempfile` 3.x | Temporary directories for tests |
| `rand` 0.8 | Random data generation for tests |

---

## Building & Testing

### Build

```bash
cargo build              # Debug build
cargo build --release    # Optimized release build (LTO + codegen-units=1)
```

Release profile is configured for maximum performance:
- `opt-level = 3` — Full optimizations
- `lto = "thin"` — Link-time optimization
- `codegen-units = 1` — Single codegen unit for better optimization

### Test

```bash
cargo test               # Run all 198 tests
cargo test -- --nocapture  # With output
```

Tests are co-located with source code using `#[cfg(test)]` modules. Coverage includes:
- **Roundtrip tests** for all compression codecs
- **WAL recovery** including corrupted entry handling
- **MemTable** insert, freeze, overwrite semantics
- **Segment** write, read, CRC verification, compression effectiveness
- **Full query pipeline** from write → flush → query → aggregated results
- **Parser** tests for all PulseQL syntax variants
- **Compaction** merge, deduplication, and data preservation
- **Retention** policy enforcement

### Lint & Format

```bash
cargo clippy             # Lint checks
cargo fmt --check        # Format verification
```

### Benchmark

```bash
cargo bench              # Run all Criterion benchmarks
```

---

## Codebase Statistics

| Metric | Value |
|---|---|
| Total lines of Rust | 7,753 |
| Source files | 37 |
| Modules | 8 (model, encoding, engine, index, query, server, storage, cli) |
| Test count | 198 |
| Benchmark suites | 3 (ingestion, query, compression) |
| Dependencies | 19 (runtime) + 3 (dev) |

### Largest Files

| File | Lines | Component |
|---|---|---|
| `query/parser.rs` | 759 | PulseQL recursive descent parser |
| `query/aggregator.rs` | 583 | Aggregation engine |
| `query/lexer.rs` | 561 | PulseQL tokenizer |
| `storage/segment.rs` | 555 | Columnar segment reader/writer |
| `engine/database.rs` | 487 | Database coordinator |
| `storage/compactor.rs` | 472 | Background compaction |
| `query/planner.rs` | 463 | Query planner |
| `server/protocol.rs` | 457 | Line protocol parser |
| `query/executor.rs` | 416 | Query executor |
| `encoding/float.rs` | 329 | Gorilla XOR compression |

### Module Breakdown

| Module | Files | Lines | % |
|---|---|---|---|
| query | 6 | 2,943 | 38.0% |
| storage | 6 | 1,489 | 19.2% |
| encoding | 5 | 895 | 11.5% |
| engine | 5 | 1,003 | 12.9% |
| server | 4 | 684 | 8.8% |
| model | 4 | 230 | 3.0% |
| index | 3 | 356 | 4.6% |
| cli | 1 | 35 | 0.5% |
| root | 2 | 118 | 1.5% |

---

## Roadmap

### Completed ✅

- [x] Core data model (DataPoint, FieldValue, Tags, SeriesKey)
- [x] Compression codecs (delta-of-delta, Gorilla XOR, delta+zigzag, bit-pack)
- [x] Write-ahead log with CRC32 crash recovery
- [x] MemTable with freeze/rotate
- [x] Columnar segment writer/reader with LZ4
- [x] Time-based partitioning
- [x] Series index + tag inverted index
- [x] Segment flush integration (memtable → disk)
- [x] Line protocol parser
- [x] PulseQL query engine (lexer, parser, planner, executor)
- [x] Aggregation functions (count, sum, mean, min, max, GROUP BY)
- [x] TCP ingestion server
- [x] HTTP query API
- [x] CLI (server, version)
- [x] Background compactor
- [x] Retention policies
- [x] Regex tag matching (=~ and !~ operators)
- [x] Schema enforcement (type-mismatch rejection)
- [x] Criterion benchmarks (ingestion, query, compression)

### Planned

- [ ] Flamegraph profiling + hot-path optimization
- [ ] GitHub Actions CI

### Future

- **v1.1 — Observability**: Prometheus `/metrics` endpoint, query profiling (EXPLAIN)
- **v1.2 — Advanced Query**: Subqueries, continuous queries, math expressions in SELECT
- **v2.0 — Distributed**: Raft replication, consistent hashing, cross-node query fan-out
- **v2.1 — Ecosystem**: Prometheus remote_write/read, Grafana plugin, OpenTelemetry receiver

---

## Troubleshooting

### Port Already in Use

```
Error: Address already in use (os error 98)
```

Another process is using port 8086 or 8087. Use `--tcp-port` and `--http-port` to configure different ports, or stop the conflicting process.

### WAL Corruption

```
WARN: CRC mismatch in WAL entry, skipping
```

A WAL entry was corrupted (e.g., due to an unclean shutdown). PulseDB automatically skips corrupted entries during recovery. Data in corrupted entries may be lost if it wasn't flushed to segments.

### Schema Type Mismatch

```
Error: schema conflict: field 'usage' in measurement 'cpu' has type Float but got Integer
```

A write attempted to use a different type for an existing field. The first write to a field sets its type. Use consistent types, or use a different field name.

### High Cardinality Warning

If you have millions of unique tag combinations, the series index will consume significant memory. Avoid using high-cardinality values (UUIDs, timestamps, user IDs) as tags — use fields instead.

### Slow Queries

- Use `WHERE time > now() - <duration>` to limit the scan range
- Add tag predicates to reduce the number of series scanned
- Use `LIMIT` to cap result sizes
- Run compaction to reduce the number of segment files

---

*PulseDB v0.1.0 — Built with 🦀 Rust — designed for speed, compressed for efficiency*

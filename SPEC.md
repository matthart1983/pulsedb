# PulseDB — High-Performance Time-Series Database

## 1. Overview

PulseDB is a purpose-built time-series database written in Rust, optimized for high-throughput ingestion, efficient compression, and fast time-range queries. Designed for metrics, IoT telemetry, financial tick data, and observability workloads.

### Why Another TSDB?

Existing time-series databases trade off between performance and simplicity. InfluxDB rewrote its engine multiple times. TimescaleDB bolts onto PostgreSQL's row-oriented storage. VictoriaMetrics is Go with GC pauses. PulseDB is built from scratch in Rust with a single goal: maximum throughput on a single node, with zero dependencies outside the binary.

### Design Principles

1. **Append-only architecture** — No in-place updates. Immutable segments simplify concurrency and crash recovery.
2. **Columnar storage** — Fields stored column-by-column. Same-type values compress dramatically better than row-oriented layouts.
3. **Type-aware compression** — Each data type gets its own codec tuned for time-series patterns (see §4).
4. **Zero-copy reads** — Memory-mapped segments avoid serialization overhead on the read path.
5. **Lock-free write path** — WAL append + memtable insert with minimal contention.
6. **Ecosystem compatibility** — InfluxDB line protocol for ingestion means existing collectors (Telegraf, Prometheus remote_write adapters, IoT agents) work out of the box.

### Design Goals

- **Ingest ≥ 1M data points/sec** on commodity hardware (single node, NVMe SSD)
- **Sub-millisecond queries** on recent data (in-memory), sub-second on historical ranges
- **10–20× compression ratio** using time-series-aware encodings
- **Zero-copy reads** via memory-mapped columnar segments
- **Lock-free write path** with append-only WAL
- **Simple query language** with SQL-like syntax tailored for time-series

### Non-Goals (v1)

- Distributed clustering / replication (single-node first; clustering is a v2 concern)
- Full SQL compliance (JOINs, subqueries, CTEs)
- ACID transactions (append-only, eventual consistency is acceptable)
- String field indexing (tags are indexed; string fields are stored but not searchable)

---

## 2. Data Model

### Series

A time series is uniquely identified by a **measurement name** + **tag set**:

```
cpu,host=server01,region=us-east usage_idle=98.2,usage_system=1.3 1672531200000000000
│    │                            │                                 │
│    └─ tags (indexed)            └─ fields (values)                └─ timestamp (ns)
measurement
```

- **Measurement**: Logical grouping (e.g., `cpu`, `mem`, `http_requests`). Analogous to a table name.
- **Tags**: Key-value string pairs. Always indexed. Used for filtering (`WHERE host = 'x'`) and grouping (`GROUP BY region`). Cardinality matters — tags should have bounded, low-cardinality values.
- **Fields**: Key-value pairs containing the actual data. Types: `f64`, `i64`, `u64`, `bool`. Not indexed. A measurement can have multiple fields.
- **Timestamp**: Nanosecond Unix epoch (`i64`). Always present. If omitted on write, the server assigns `now()`.

### Series Key

The unique identifier for a series: `measurement + sorted(tags)`. Example: `cpu,host=server01,region=us-east`. This string maps to a compact **Series ID** (`u64`) stored in the series index.

### Cardinality

Each unique series key is a distinct time series. The series index maps every unique key to an ID. High-cardinality tags (e.g., `user_id=<uuid>`) create millions of series and should be avoided — use fields instead.

**Target**: Support up to 10M active series with < 4GB index memory.

### Schema on Write

PulseDB uses **schema-on-write**: the first time a field name appears for a measurement, its type is recorded. Subsequent writes to the same field must use the same type or the write is rejected. This prevents type conflicts that cause query-time errors.

---

## 3. Architecture

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

### Write Path (Detail)

```
Client ──TCP──► Line Protocol Parser
                   │
                   ▼
              Batch of DataPoints
                   │
                   ├──► WAL.append(batch)          [1] Durability first
                   │       └─ [len][crc32][type][json_payload]
                   │       └─ fsync per policy (every / batch / none)
                   │
                   ├──► MemTable.insert(point)     [2] In-memory indexing
                   │       └─ BTreeMap<series_key, BTreeMap<timestamp, fields>>
                   │       └─ Track approximate size_bytes
                   │
                   └──► if size_bytes > threshold:
                           ├─ Freeze active MemTable → FrozenMemTable
                           ├─ Swap in new empty MemTable
                           ├─ SeriesIndex.get_or_create(key) for each series
                           ├─ InvertedIndex.index_series(id, tags) for each series
                           ├─ SegmentWriter.write_segment() per series
                           │     └─ Encode timestamp column (delta-of-delta)
                           │     └─ Encode field columns (gorilla/delta/bitpack)
                           │     └─ LZ4 compress each column
                           │     └─ Write to partition dir with CRC footer
                           ├─ SegmentCache.add(meta) for each new segment
                           └─ WAL.truncate()
```

### Read Path (Detail)

```
Client ──HTTP POST /query──► Query Parser
                                │
                                ▼
                           PulseQL AST
                                │
                                ▼
                           Query Planner
                           ├─ Resolve measurement → series keys via InvertedIndex
                           │     └─ Evaluate tag predicates (AND → intersect, OR → union)
                           │     └─ Regex tag matching via posting list scan
                           ├─ Resolve time range → candidate segments via SegmentCache
                           │     └─ Prune segments whose [min_ts, max_ts] doesn't overlap
                           ├─ Check MemTable for recent unflushed data
                           └─ Produce QueryPlan (list of scan operations)
                                │
                                ▼
                           Query Executor
                           ├─ For each segment:
                           │     ├─ SegmentReader.open() (memory-map file)
                           │     ├─ Read + decompress timestamp column
                           │     ├─ Binary search for time range boundaries
                           │     ├─ Read + decompress only requested field columns
                           │     └─ Yield (timestamp, field_values) tuples
                           ├─ Merge segment results with MemTable data (time-ordered)
                           └─ Feed into Aggregator
                                │
                                ▼
                           Aggregator
                           ├─ GROUP BY time(interval): bucket timestamps
                           ├─ GROUP BY tag: split by tag values
                           ├─ Compute: count, sum, mean, min, max, first, last,
                           │           stddev, percentile
                           ├─ Apply FILL policy (none, null, linear, previous)
                           └─ Return QueryResult → JSON response
```

### Storage Path

```
~/.pulsedb/                            (or --data-dir)
├── wal/
│   └── wal.log                         Append-only write-ahead log
├── partitions/
│   ├── 2024-01-15T14/                  1-hour time partition
│   │   ├── cpu_host=server01.seg       Segment: one series, one partition
│   │   ├── cpu_host=server02.seg
│   │   └── mem_host=server01.seg
│   ├── 2024-01-15T15/
│   │   └── ...
│   └── 2024-01-15T16/
│       └── ...
├── index/
│   ├── series.idx                      Series key → ID mapping (persistence)
│   └── tags.idx                        Tag inverted index (persistence)
└── meta/
    └── measurements.json               Schema: field names + types per measurement
```

---

## 4. Storage Format

### WAL Entry Format

```
┌───────────┬──────────┬────────────┬─────────────────────┐
│ len: u32  │ crc: u32 │ type: u8   │ payload: [u8; len-1]│
│ (LE)      │ (LE)     │ (1=Write)  │ (JSON batch)        │
└───────────┴──────────┴────────────┴─────────────────────┘
```

- **Batch writes**: Multiple points packed into a single WAL entry for throughput.
- **CRC32**: Computed over `payload` only. Detects corruption. On mismatch, entry is skipped during recovery.
- **Sequential recovery**: Entries are read front-to-back. Truncated or corrupted trailing entries are discarded.
- **Fsync policy**: `every` (durability guarantee per write), `batch` (fsync every N ms or on flush), `none` (OS decides — highest throughput, risk of data loss on crash).

**Future optimization**: Replace JSON payload serialization with a compact binary format (4–8× smaller WAL entries).

### Segment File Layout

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

Encoding type markers:
- `1` = Timestamp (delta-of-delta + zigzag + varint)
- `2` = Float (Gorilla XOR)
- `3` = Integer (delta + zigzag + varint)
- `4` = Boolean (bit-packing)

### Compression Strategies

| Data Type | Encoding | Algorithm | Expected Ratio | Notes |
|---|---|---|---|---|
| Timestamps | Delta-of-delta | Store `delta[i] - delta[i-1]`, zigzag encode, varint encode | 10–50× | Regular intervals compress to ~1 byte/point |
| Float fields | Gorilla XOR | XOR consecutive values; store leading zeros + meaningful bits | 8–15× | Facebook Gorilla paper (Pelkonen 2015) |
| Integer fields | Delta + zigzag | Delta encode, zigzag for signed, varint for compactness | 5–20× | Counters/gauges with small deltas |
| Booleans | Bit-packing | 8 values per byte, u32 count prefix | 8× | Trivial but effective |
| All columns | LZ4 | Outer wrapper on encoded data | 1.2–3× additional | Fast decompression (~4GB/s) |

**Combined ratio**: For typical metric workloads (regular timestamps, slowly changing floats), expect **12–25× total compression** over raw storage.

### Series Index

```
Series Index (in-memory HashMap, persisted to series.idx)
┌────────────────────────────────────┬───────────┐
│ Series Key (String)                │ SeriesId  │
├────────────────────────────────────┼───────────┤
│ "cpu,host=server01,region=us-east" │ SeriesId(1)│
│ "cpu,host=server02,region=us-east" │ SeriesId(2)│
│ "mem,host=server01"                │ SeriesId(3)│
└────────────────────────────────────┴───────────┘

Inverted Index (in-memory HashMap, persisted to tags.idx)
┌────────────────────────┬──────────────────────────┐
│ Tag Term (String)      │ Posting List [SeriesId]  │
├────────────────────────┼──────────────────────────┤
│ "host=server01"        │ [1, 3]                   │
│ "host=server02"        │ [2]                      │
│ "region=us-east"       │ [1, 2]                   │
└────────────────────────┴──────────────────────────┘
```

Posting lists are kept sorted for O(n+m) intersection/union using merge-join.

**Future**: Replace `Vec<SeriesId>` with roaring bitmaps for >100K series per posting list.

---

## 5. Query Language — PulseQL

### Grammar (Simplified EBNF)

```ebnf
query         = select_stmt ;
select_stmt   = "SELECT" field_list
                "FROM" measurement
                [ "WHERE" condition ]
                [ "GROUP BY" group_list ]
                [ "FILL" "(" fill_policy ")" ]
                [ "ORDER BY" "time" ("ASC" | "DESC") ]
                [ "LIMIT" integer ]
                [ "OFFSET" integer ] ;

field_list    = field_expr { "," field_expr } | "*" ;
field_expr    = [ agg_func "(" ] field_name [ ")" ] [ "AS" alias ] ;
agg_func      = "count" | "sum" | "mean" | "avg" | "min" | "max"
              | "first" | "last" | "stddev"
              | "percentile" ;

condition     = predicate { ("AND" | "OR") predicate } ;
predicate     = tag_name op value
              | "time" time_op time_expr
              | "(" condition ")" ;

op            = "=" | "!=" | ">" | "<" | ">=" | "<="
              | "=~" | "!~" | "IN" ;

time_op       = ">" | "<" | ">=" | "<=" | "BETWEEN" ;
time_expr     = "now()" [ "-" duration ]
              | "'" iso_datetime "'"
              | integer ;

group_list    = group_expr { "," group_expr } ;
group_expr    = "time" "(" duration ")" | tag_name ;

fill_policy   = "none" | "null" | "linear" | "previous" | number ;

duration      = integer ("ns" | "us" | "ms" | "s" | "m" | "h" | "d" | "w") ;
```

### Query Examples

```sql
-- Basic aggregation with time bucketing
SELECT mean(usage_idle), max(usage_system)
FROM cpu
WHERE host = 'server01' AND time > now() - 1h
GROUP BY time(5m)

-- Multi-tag filter with regex
SELECT sum(bytes_in)
FROM network
WHERE region = 'us-east' AND host =~ /web-\d+/
GROUP BY time(1m), host

-- Raw data retrieval
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

-- Cardinality exploration
SELECT count(DISTINCT host)
FROM cpu
WHERE time > now() - 24h

-- Last known value
SELECT last(value)
FROM sensor_reading
WHERE device_id = 'D-100'
GROUP BY sensor_type
```

### Supported Aggregations

| Function | Description | Notes |
|---|---|---|
| `count(field)` | Number of non-null values | |
| `sum(field)` | Sum of values | Float/integer fields only |
| `mean(field)` / `avg(field)` | Arithmetic mean | |
| `min(field)` | Minimum value | |
| `max(field)` | Maximum value | |
| `first(field)` | Earliest value by time | |
| `last(field)` | Latest value by time | |
| `stddev(field)` | Population standard deviation | |
| `percentile(field, N)` | Nth percentile (0–100) | Uses linear interpolation |
| `count(DISTINCT tag)` | Cardinality of a tag | Phase 5 |

### Supported Predicates

| Operator | Description | Example |
|---|---|---|
| `=` | Equality | `host = 'server01'` |
| `!=` | Inequality | `region != 'eu'` |
| `>`, `<`, `>=`, `<=` | Comparison | `time > now() - 1h` |
| `=~` | Regex match | `host =~ /web-\d+/` |
| `!~` | Regex not match | `host !~ /test/` |
| `IN` | Set membership | `host IN ('a', 'b', 'c')` |
| `AND` | Logical AND | Intersects results |
| `OR` | Logical OR | Unions results |
| `BETWEEN` | Inclusive range | `time BETWEEN '2024-01-01' AND '2024-02-01'` |

### Time Functions

| Function | Description |
|---|---|
| `now()` | Current server time (nanoseconds) |
| `time(interval)` | GROUP BY time bucketing |
| `BETWEEN` | Inclusive time range |

### Duration Syntax

`1ns`, `100us`, `5ms`, `10s`, `5m`, `1h`, `7d`, `2w`

---

## 6. Wire Protocol

### Ingestion — Line Protocol (TCP :8086)

InfluxDB-compatible line protocol over raw TCP. Each line is one data point:

```
<measurement>,<tag1>=<val1>,<tag2>=<val2> <field1>=<fval1>,<field2>=<fval2> <timestamp>
```

#### Syntax Rules

- **Measurement**: Required. No spaces, commas, or equals signs.
- **Tags**: Optional. Comma-separated `key=value` pairs after measurement (no space before first tag).
- **Fields**: Required. Space-separated from tags. Comma-separated `key=value` pairs.
  - Float: `1.0` or `1` (no suffix)
  - Integer: `1i`
  - Unsigned integer: `1u`
  - Boolean: `t`, `f`, `true`, `false`, `T`, `F`, `TRUE`, `FALSE`
  - String: `"hello"` (double-quoted)
- **Timestamp**: Optional nanosecond Unix epoch. If omitted, server assigns `now()`.
- **Line terminator**: `\n`

#### Examples

```
cpu,host=server01,region=us-east usage_idle=98.2,usage_system=1.3 1672531200000000000
mem,host=server01 available=8589934592i,total=17179869184i 1672531200000000000
http_requests,method=GET,path=/api/v1/users count=1i,latency_ms=12.4 1672531200000000000
sensor,device=D-42 temperature=23.5,healthy=t
```

#### Batch Ingestion

Multiple lines can be sent in a single TCP write for throughput. The parser processes lines until the connection closes or a configurable idle timeout.

### Query — HTTP API (:8087)

#### POST /query

```http
POST /query HTTP/1.1
Content-Type: application/json

{
  "q": "SELECT mean(usage_idle) FROM cpu WHERE host='server01' AND time > now() - 1h GROUP BY time(5m)"
}
```

**Success Response** (200):

```json
{
  "results": [
    {
      "series": [
        {
          "name": "cpu",
          "tags": { "host": "server01" },
          "columns": ["time", "mean_usage_idle"],
          "values": [
            [1672531200000000000, 98.2],
            [1672531500000000000, 97.8],
            [1672531800000000000, 96.5]
          ]
        }
      ]
    }
  ]
}
```

**Error Response** (400):

```json
{
  "error": "parse error: expected FROM clause at position 24"
}
```

#### POST /write

Alternative HTTP ingestion endpoint (for compatibility with tools that don't support raw TCP):

```http
POST /write HTTP/1.1
Content-Type: text/plain

cpu,host=server01 usage_idle=98.2 1672531200000000000
cpu,host=server02 usage_idle=95.1 1672531200000000000
```

**Success**: 204 No Content
**Error**: 400 with JSON body

#### GET /health

```json
200 OK
{ "status": "ok" }
```

#### GET /status

```json
{
  "version": "0.1.0",
  "uptime_secs": 9240,
  "series_count": 42000,
  "measurement_count": 15,
  "points_ingested": 1283948123,
  "points_per_sec": 138912,
  "active_memtable_bytes": 23948288,
  "segment_count": 847,
  "total_disk_bytes": 2147483648,
  "compression_ratio": 14.2
}
```

---

## 7. Module Structure

```
src/
├── main.rs                 # CLI entry point (clap), server bootstrap
├── lib.rs                  # Library root, public API for embedding
│
├── model/                  # Core data types
│   ├── mod.rs
│   ├── point.rs            # DataPoint, FieldValue, Tags
│   ├── series.rs           # SeriesKey, SeriesId
│   └── schema.rs           # MeasurementSchema, FieldType
│
├── encoding/               # Compression codecs
│   ├── mod.rs
│   ├── timestamp.rs        # Delta-of-delta + zigzag + varint
│   ├── float.rs            # Gorilla XOR encoding (Facebook paper)
│   ├── integer.rs          # Delta + zigzag + varint
│   └── boolean.rs          # Bit-packing (8 per byte)
│
├── engine/                 # Core database engine
│   ├── mod.rs
│   ├── database.rs         # Top-level DB handle, write path coordinator
│   ├── wal.rs              # Write-ahead log (append, recover, truncate)
│   ├── memtable.rs         # In-memory sorted buffer + FrozenMemTable
│   └── config.rs           # EngineConfig, FsyncPolicy
│
├── storage/                # On-disk storage
│   ├── mod.rs
│   ├── segment.rs          # Columnar segment reader/writer
│   ├── compactor.rs        # Background segment merging
│   ├── partition.rs        # Time-based partitioning (hourly dirs)
│   └── cache.rs            # SegmentMeta cache for query planning
│
├── index/                  # Series & tag indexing
│   ├── mod.rs
│   ├── series.rs           # Series key → ID mapping (HashMap)
│   └── inverted.rs         # Tag inverted index (posting lists)
│
├── query/                  # Query engine
│   ├── mod.rs
│   ├── lexer.rs            # PulseQL tokenizer
│   ├── parser.rs           # Recursive descent parser → AST
│   ├── ast.rs              # Query AST types (SelectStatement, Expr, etc.)
│   ├── planner.rs          # Query plan generation (segment pruning)
│   ├── executor.rs         # Plan execution (segment scanning + memtable merge)
│   └── aggregator.rs       # Aggregation functions + GROUP BY bucketing
│
├── server/                 # Network layer
│   ├── mod.rs
│   ├── tcp.rs              # Line protocol TCP listener (tokio)
│   ├── http.rs             # HTTP query API (axum or hyper)
│   └── protocol.rs         # Line protocol parser
│
└── cli/                    # CLI commands
    ├── mod.rs
    ├── server.rs           # `pulsedb server` — start the daemon
    ├── query.rs            # `pulsedb query` — interactive PulseQL REPL
    ├── import.rs           # `pulsedb import` — bulk file import
    └── status.rs           # `pulsedb status` — show engine stats
```

---

## 8. Implementation Status

### What's Built (✅)

| Component | Module | Files | Tests | Notes |
|---|---|---|---|---|
| Data model | `model/` | `point.rs`, `series.rs`, `schema.rs` | ✅ | DataPoint, FieldValue, Tags, SeriesKey, SeriesId |
| Timestamp codec | `encoding/timestamp.rs` | 1 | 8 tests | Delta-of-delta + zigzag + varint |
| Float codec | `encoding/float.rs` | 1 | 8 tests | Gorilla XOR with BitWriter/BitReader |
| Integer codec | `encoding/integer.rs` | 1 | 7 tests | Delta + zigzag + varint |
| Boolean codec | `encoding/boolean.rs` | 1 | 7 tests | Bit-packing |
| WAL | `engine/wal.rs` | 1 | 4 tests | Append, recover, truncate with CRC32 |
| MemTable | `engine/memtable.rs` | 1 | 4 tests | BTreeMap-based, freeze to immutable |
| Database engine | `engine/database.rs` | 1 | 4 tests | Write path: WAL → memtable → freeze |
| Engine config | `engine/config.rs` | 1 | — | FsyncPolicy, data dirs, thresholds |
| Segment storage | `storage/segment.rs` | 1 | 7 tests | Columnar write/read with LZ4 + type codecs |
| Partitioning | `storage/partition.rs` | 1 | 5 tests | Hourly time partitions |
| Segment cache | `storage/cache.rs` | 1 | 3 tests | In-memory metadata for query planning |
| Compactor | `storage/compactor.rs` | 1 | — | Placeholder (stub) |
| Series index | `index/series.rs` | 1 | 6 tests | HashMap key → ID mapping |
| Inverted index | `index/inverted.rs` | 1 | 11 tests | Tag posting lists, intersect, union |

**Total: 78 tests passing, 0 warnings.**

### What's Missing (⬜)

| Component | Priority | Complexity | Notes |
|---|---|---|---|
| Segment flush integration | P0 | Medium | Connect Database.rotate_memtable() to SegmentWriter |
| Line protocol parser | P0 | Medium | Parse InfluxDB line protocol text |
| PulseQL lexer | P0 | Medium | Tokenize query strings |
| PulseQL parser | P0 | High | Recursive descent → AST |
| AST types | P0 | Low | SelectStatement, Expr, AggFunc |
| Query planner | P1 | High | Series resolution, segment pruning |
| Query executor | P1 | High | Segment scanning, memtable merge |
| Aggregator | P1 | Medium | count/sum/mean/min/max + GROUP BY time |
| TCP server | P1 | Medium | Tokio TCP listener for line protocol |
| HTTP server | P1 | Medium | Query API, health, status |
| CLI commands | P2 | Low | clap subcommands |
| Index persistence | P2 | Medium | Save/load series + tag indexes |
| WAL binary format | P2 | Medium | Replace JSON with compact binary |
| Compactor | P2 | High | Merge small segments |
| Retention policies | P3 | Low | Auto-drop old partitions |
| Advanced aggregations | P3 | Medium | percentile, stddev, first, last |
| FILL policies | P3 | Low | linear, previous, null, none |
| Regex tag matching | P3 | Low | =~ operator in query |
| Bulk import tool | P3 | Low | CSV/line-protocol file import |
| Memory-mapped reads | P3 | Medium | Replace fs::read with mmap |
| Benchmarks | P3 | Medium | Criterion benchmarks + flamegraphs |

---

## 9. Build Plan

### Phase 1 — Flush Integration + Line Protocol (Current Priority)

**Goal**: Complete the write path end-to-end. Data flows from TCP → disk segments.

| # | Task | Depends On | Estimated Effort |
|---|---|---|---|
| 1.1 | Wire Database.rotate_memtable() to SegmentWriter | — | 2–3 hours |
| | Extract series from FrozenMemTable, separate timestamps + fields per series | | |
| | Write each series to a segment file in the correct partition directory | | |
| | Register segment metadata in SegmentCache | | |
| | Truncate WAL after successful flush | | |
| 1.2 | Index persistence (series + tags) | — | 2 hours |
| | Serialize SeriesIndex to JSON on flush, load on startup | | |
| | Serialize InvertedIndex to JSON on flush, load on startup | | |
| 1.3 | Line protocol parser (`server/protocol.rs`) | — | 3 hours |
| | Parse measurement, tags, fields, timestamp from text lines | | |
| | Handle all field types: float, integer, unsigned, boolean, string | | |
| | Handle missing timestamp (assign now()) | | |
| | Handle batch (multi-line) input | | |
| | Comprehensive test suite (valid + malformed input) | | |
| 1.4 | Integration test: parse → write → flush → read segment | 1.1, 1.3 | 1 hour |
| | End-to-end test without network layer | | |

**Exit criteria**: Can feed line protocol text into the engine, have it WAL'd, memtabled, flushed to compressed segments, and read back correctly.

### Phase 2 — Query Engine

**Goal**: Parse PulseQL queries, scan segments, compute aggregations.

| # | Task | Depends On | Estimated Effort |
|---|---|---|---|
| 2.1 | AST types (`query/ast.rs`) | — | 1 hour |
| | SelectStatement, FieldExpr, AggFunc enum, WhereClause, GroupBy, etc. | | |
| 2.2 | PulseQL lexer (`query/lexer.rs`) | — | 2 hours |
| | Tokenize: keywords, identifiers, numbers, strings, operators, durations | | |
| | Handle quoted strings, regex literals | | |
| 2.3 | PulseQL parser (`query/parser.rs`) | 2.1, 2.2 | 4 hours |
| | Recursive descent: SELECT, FROM, WHERE, GROUP BY, FILL, ORDER BY, LIMIT | | |
| | Operator precedence for AND/OR | | |
| | Error messages with source position | | |
| | Parser test suite | | |
| 2.4 | Query planner (`query/planner.rs`) | 2.3, Phase 1 | 3 hours |
| | Evaluate WHERE tag predicates → series IDs via InvertedIndex | | |
| | Evaluate WHERE time predicates → segment list via SegmentCache | | |
| | Determine which fields to read (projection pushdown) | | |
| | Produce QueryPlan struct | | |
| 2.5 | Query executor (`query/executor.rs`) | 2.4 | 4 hours |
| | For each segment in plan: open, read timestamps, binary search time range | | |
| | Read only requested field columns | | |
| | Merge segment data with active MemTable data | | |
| | Yield time-ordered result stream | | |
| 2.6 | Aggregator (`query/aggregator.rs`) | 2.5 | 3 hours |
| | Implement: count, sum, mean, min, max | | |
| | GROUP BY time(interval) bucketing | | |
| | GROUP BY tag splitting | | |
| | Return structured QueryResult | | |
| 2.7 | Integration tests | 2.6 | 2 hours |
| | Write data → flush → query → verify results | | |
| | Test time range pruning, tag filtering, aggregations | | |

**Exit criteria**: Can write data, query it with PulseQL, and get correct aggregated results.

### Phase 3 — Server & API

**Goal**: Network-accessible database server.

| # | Task | Depends On | Estimated Effort |
|---|---|---|---|
| 3.1 | TCP listener (`server/tcp.rs`) | Phase 1 | 2 hours |
| | Tokio TcpListener on :8086 | | |
| | Per-connection handler: read lines, parse, batch, write to engine | | |
| | Configurable batch size + flush interval | | |
| | Connection logging | | |
| 3.2 | HTTP server (`server/http.rs`) | Phase 2 | 3 hours |
| | Use `axum` or `hyper` on :8087 | | |
| | POST /query — parse PulseQL, execute, return JSON | | |
| | POST /write — accept line protocol over HTTP | | |
| | GET /health — liveness check | | |
| | GET /status — engine statistics | | |
| 3.3 | CLI: `pulsedb server` (`cli/server.rs`) | 3.1, 3.2 | 1 hour |
| | Clap subcommand with --data-dir, --tcp-port, --http-port, etc. | | |
| | Graceful shutdown (SIGTERM/SIGINT): flush memtable, close listeners | | |
| 3.4 | CLI: `pulsedb query` (`cli/query.rs`) | 3.2 | 2 hours |
| | Interactive REPL: read PulseQL, send to HTTP, display results | | |
| | Output formats: table, json, csv | | |
| | History + readline support | | |
| 3.5 | CLI: `pulsedb status` (`cli/status.rs`) | 3.2 | 30 min |
| | Fetch /status, format output | | |
| 3.6 | End-to-end test: TCP ingest → HTTP query | All | 2 hours |

**Exit criteria**: `pulsedb server` starts, accepts TCP writes and HTTP queries, returns correct results.

### Phase 4 — Production Hardening

**Goal**: Make it reliable and fast enough for real workloads.

| # | Task | Estimated Effort |
|---|---|---|
| 4.1 | Background compactor — merge segments within a partition | 4 hours |
| 4.2 | Retention policies — auto-drop partitions older than config | 1 hour |
| 4.3 | WAL binary format — replace JSON with compact binary serialization | 3 hours |
| 4.4 | Memory-mapped segment reads — replace `fs::read` with `memmap2` | 2 hours |
| 4.5 | Advanced aggregations — first, last, stddev, percentile | 3 hours |
| 4.6 | FILL policies — none, null, linear, previous | 2 hours |
| 4.7 | Regex tag matching — =~ and !~ in WHERE clauses | 1 hour |
| 4.8 | Schema enforcement — reject type-mismatched field writes | 1 hour |
| 4.9 | Bulk import tool — `pulsedb import` for CSV/line-protocol files | 2 hours |
| 4.10 | Background flush — async flush thread instead of blocking on write | 3 hours |

### Phase 5 — Performance & Polish

**Goal**: Hit performance targets, add benchmarks, write documentation.

| # | Task | Estimated Effort |
|---|---|---|
| 5.1 | Criterion benchmarks: ingestion throughput (points/sec) | 2 hours |
| 5.2 | Criterion benchmarks: query latency (time-range, aggregation) | 2 hours |
| 5.3 | Criterion benchmarks: compression ratio by data pattern | 1 hour |
| 5.4 | Flamegraph profiling + hot-path optimization | 4 hours |
| 5.5 | Lock contention analysis — minimize RwLock hold times | 2 hours |
| 5.6 | README with badges, architecture diagram, quick start | 2 hours |
| 5.7 | Publish to crates.io | 30 min |
| 5.8 | GitHub Actions CI — build, test, clippy, fmt | 1 hour |

---

## 10. Performance Targets

| Metric | Target | Measurement Method |
|---|---|---|
| Write throughput | ≥ 1M points/sec | Batch of 10K points × 100 batches, wall clock |
| Single-point write latency | < 10μs | WAL append + memtable insert, p99 |
| Time-range query (1h, 1 series) | < 1ms | Scan 1 segment, return raw |
| Time-range query (1h, 1000 series) | < 50ms | Scan + merge 1000 segments |
| Aggregation query (24h, GROUP BY 5m) | < 10ms | Scan 24 segments, 288 buckets |
| Compression ratio (float metrics) | ≥ 10× | Regular 10s-interval CPU metrics |
| Memory usage (1M active series) | < 2GB | Series index + inverted index + memtable |
| Segment flush time (1M points) | < 100ms | Encode + compress + write to disk |
| Startup time (recovery, 10GB data) | < 5s | WAL replay + index load |
| TCP ingestion throughput | ≥ 500K lines/sec | Sustained TCP write, single connection |

### Benchmark Workloads

1. **Telegraf CPU** — 10 fields, 10s interval, 100 hosts → 1000 series, ~6M points/hour
2. **IoT Temperature** — 1 field, 1s interval, 10K sensors → 10K series, ~36M points/hour
3. **Financial Tick** — 4 fields (open/high/low/close), irregular timestamps, 1K instruments
4. **High Cardinality** — 1M unique series, 1 field each, verifying index performance

---

## 11. CLI Reference

```
USAGE:
    pulsedb <COMMAND>

COMMANDS:
    server      Start the PulseDB server
    query       Interactive PulseQL query REPL
    import      Bulk import data from file
    status      Show server statistics
    compact     Trigger manual compaction
    version     Print version information

─────────────────────────────────────────────────

pulsedb server [OPTIONS]
    --data-dir <PATH>              Data directory (default: ./pulsedb_data)
    --tcp-port <PORT>              Line protocol port (default: 8086)
    --http-port <PORT>             HTTP API port (default: 8087)
    --wal-fsync <POLICY>           WAL fsync: every | batch | none (default: batch)
    --memtable-size <BYTES>        Flush threshold (default: 64MB)
    --segment-duration <SECS>      Partition duration (default: 3600)
    --retention <DURATION>         Auto-drop data older than (e.g., 30d, 1y)
    --log-level <LEVEL>            Log level: trace|debug|info|warn|error (default: info)

pulsedb query [OPTIONS]
    --host <HOST>                  Server address (default: localhost)
    --port <PORT>                  HTTP port (default: 8087)
    --format <FMT>                 Output: table | json | csv (default: table)

pulsedb import <FILE> [OPTIONS]
    --format <FMT>                 Input format: line | csv (default: line)
    --batch-size <N>               Batch size (default: 10000)
    --host <HOST>                  Server address (default: localhost)
    --port <PORT>                  TCP port (default: 8086)

pulsedb status [OPTIONS]
    --host <HOST>                  Server address (default: localhost)
    --port <PORT>                  HTTP port (default: 8087)
    --json                         Output raw JSON

pulsedb compact [OPTIONS]
    --measurement <NAME>           Compact specific measurement (default: all)
    --data-dir <PATH>              Data directory (default: ./pulsedb_data)

pulsedb version
```

---

## 12. Testing Strategy

### Unit Tests (per module)

Every module has co-located `#[cfg(test)]` tests covering:
- Happy path (normal operation)
- Edge cases (empty input, single element, boundary values)
- Error conditions (corrupted data, missing fields)
- Roundtrip verification (encode → decode, write → read)

### Integration Tests

Located in `tests/`:
- **Write path**: Line protocol → WAL → memtable → flush → segment on disk
- **Read path**: Write data → query → verify results
- **Recovery**: Write → crash (kill process) → restart → verify data intact
- **Compression**: Verify compression ratios meet targets for each workload
- **Concurrent access**: Multiple writers + reader threads

### Benchmarks

Located in `benches/`:
- `ingestion.rs` — Points/sec for batch writes of varying sizes
- `query.rs` — Latency for time-range scans and aggregation queries
- `compression.rs` — Ratio and throughput for each codec
- `index.rs` — Series index + inverted index lookup performance

### CI Pipeline

```yaml
- cargo fmt --check
- cargo clippy -- -D warnings
- cargo test
- cargo bench (nightly, weekly)
```

---

## 13. Future Work (Post v1)

### v1.1 — Observability
- Prometheus metrics endpoint (`/metrics`)
- Structured logging with tracing spans
- Query execution profiling (EXPLAIN)

### v1.2 — Advanced Query
- Subqueries (`SELECT mean(max_temp) FROM (SELECT max(temp) ... GROUP BY time(1h))`)
- Continuous queries (materialized views, auto-downsample)
- Math expressions in SELECT (`usage_system + usage_user AS usage_total`)

### v2.0 — Distributed
- Raft-based replication (3-node quorum)
- Consistent hashing for series → node assignment
- Cross-node query fan-out and merge
- Rebalancing on node add/remove

### v2.1 — Ecosystem
- Prometheus remote_write/remote_read compatibility
- Grafana data source plugin
- OpenTelemetry metrics receiver
- InfluxDB 2.x API compatibility layer

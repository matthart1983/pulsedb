# PulseDB — High-Performance Time-Series Database

## 1. Overview

PulseDB is a purpose-built time-series database written in Rust, optimized for high-throughput ingestion, efficient compression, and fast time-range queries. Designed for metrics, IoT telemetry, financial tick data, and observability workloads.

### Design Goals

- **Ingest 1M+ data points/sec** on commodity hardware (single node)
- **Sub-millisecond queries** on recent data, sub-second on historical ranges
- **10–20× compression** using time-series-aware encodings
- **Zero-copy reads** via memory-mapped columnar segments
- **Lock-free write path** with append-only WAL
- **Simple query language** with SQL-like syntax tailored for time-series

### Non-Goals (v1)

- Distributed clustering / replication (single-node first)
- Full SQL compliance
- ACID transactions (append-only, eventual consistency is fine)

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

- **Measurement**: logical grouping (e.g., `cpu`, `mem`, `http_requests`)
- **Tags**: key-value string pairs, indexed, used for filtering/grouping
- **Fields**: key-value numeric pairs (f64, i64, u64, bool), the actual data
- **Timestamp**: nanosecond Unix epoch, always present

### Series Key

The unique identifier for a series: `measurement + sorted(tags)`. This maps to a **Series ID** (u64) stored in the series index.

---

## 3. Architecture

```
                    ┌──────────────────────────────────────┐
                    │           PulseDB Engine             │
                    ├──────────────────────────────────────┤
  Write Path        │                                      │  Read Path
                    │                                      │
  Client ──────►  WAL ──► MemTable ──► Flush ──► Segment   │  Client
  (line proto)     │       (sorted)     │      (columnar)  │  (query)
                    │                   │         ▲        │     │
                    │                   ▼         │        │     ▼
                    │              Compactor ──────┘        │  QueryEngine
                    │           (merge + compress)          │  (scan/agg)
                    │                                      │     │
                    │              SeriesIndex              │     │
                    │           (tag inverted index)        │  ◄──┘
                    └──────────────────────────────────────┘
```

### Write Path

1. **Line Protocol Parser** — Parse incoming data (InfluxDB-compatible line protocol)
2. **WAL (Write-Ahead Log)** — Append-only binary log for durability. Fsync per batch.
3. **MemTable** — In-memory sorted buffer (per-series BTreeMap of timestamp → fields). Bounded by size/age.
4. **Flush** — When MemTable exceeds threshold, freeze it and write a columnar **Segment** to disk.

### Read Path

1. **Query Parser** — Parse PulseQL query into an AST
2. **Planner** — Resolve series IDs via tag index, identify segments to scan
3. **Segment Scanner** — Memory-map segments, decompress columns, apply time-range filter
4. **Aggregator** — Compute aggregations (mean, sum, min, max, count, percentile) with optional GROUP BY time bucketing

### Storage Path

1. **Segments** — Immutable columnar files, one per time partition (e.g., 1-hour blocks)
2. **Compactor** — Background thread merges small segments into larger ones, re-compresses, drops tombstoned data

---

## 4. Storage Format

### WAL Entry Format

```
┌─────────┬────────┬──────────┬─────────┬──────────┐
│ len: u32│ crc: u32│ type: u8 │ ts: i64 │ payload  │
└─────────┴────────┴──────────┴─────────┴──────────┘
```

- Batch writes: multiple points packed into a single WAL entry
- CRC32 for corruption detection
- Sequential reads for crash recovery

### Segment File Layout

```
┌──────────────────────────────────────────────────────┐
│ Segment Header (magic, version, time range, count)   │
├──────────────────────────────────────────────────────┤
│ Timestamp Column (delta-of-delta + varint encoded)   │
├──────────────────────────────────────────────────────┤
│ Field Column 0 (gorilla XOR float compression)       │
├──────────────────────────────────────────────────────┤
│ Field Column 1 ...                                   │
├──────────────────────────────────────────────────────┤
│ Column Index (offsets, min/max per column)            │
├──────────────────────────────────────────────────────┤
│ Footer (index offset, checksum)                      │
└──────────────────────────────────────────────────────┘
```

### Compression Strategies

| Data Type | Encoding | Expected Ratio |
|---|---|---|
| Timestamps | Delta-of-delta + varint | 10–50× |
| Float fields | Gorilla XOR (Facebook) | 8–15× |
| Integer fields | Delta + zigzag + varint | 5–20× |
| Booleans | Bit-packing | 8× |

### Series Index

Inverted index mapping tag key-value pairs → set of Series IDs:

```
"host=server01"  → [1, 5, 12]
"region=us-east" → [1, 2, 5, 8, 12]
```

Intersection/union of posting lists for tag predicate evaluation. Stored as sorted u64 arrays with optional roaring bitmap compression.

---

## 5. Query Language — PulseQL

SQL-like, purpose-built for time-series:

```sql
-- Basic query
SELECT mean(usage_idle), max(usage_system)
FROM cpu
WHERE host = 'server01' AND time > now() - 1h
GROUP BY time(5m)

-- Multiple tag filters
SELECT sum(bytes_in)
FROM network
WHERE region = 'us-east' AND host =~ /web-\d+/
GROUP BY time(1m), host

-- Raw data
SELECT *
FROM temperature
WHERE sensor_id = 'T-42'
  AND time BETWEEN '2024-01-01' AND '2024-01-02'
LIMIT 1000

-- Downsampling
SELECT mean(value) as avg_temp, min(value), max(value)
FROM temperature
GROUP BY time(1h), location
FILL(linear)
```

### Supported Aggregations

`count`, `sum`, `mean`, `min`, `max`, `first`, `last`, `stddev`, `percentile(field, N)`

### Supported Predicates

`=`, `!=`, `>`, `<`, `>=`, `<=`, `=~` (regex), `!~`, `IN`, `AND`, `OR`

### Time Functions

`now()`, `time(interval)` for GROUP BY bucketing, `BETWEEN`

---

## 6. Wire Protocol

### Ingestion — Line Protocol (TCP/UDP)

InfluxDB-compatible line protocol for easy ecosystem adoption:

```
measurement,tag1=val1,tag2=val2 field1=1.0,field2=2i 1672531200000000000
```

### Query — HTTP API

```
POST /query
Content-Type: application/json

{ "q": "SELECT mean(usage_idle) FROM cpu WHERE host='server01' GROUP BY time(5m)" }

Response:
{
  "results": [{
    "series": [{
      "name": "cpu",
      "tags": { "host": "server01" },
      "columns": ["time", "mean_usage_idle"],
      "values": [
        [1672531200000, 98.2],
        [1672531500000, 97.8]
      ]
    }]
  }]
}
```

### Health / Status

```
GET /health        → 200 OK
GET /status        → { "version": "0.1.0", "uptime": "2h34m", "series_count": 42000, "points_ingested": 1283948123 }
```

---

## 7. Module Structure

```
src/
├── main.rs                 # CLI entry point, server bootstrap
├── lib.rs                  # Library root, public API
├── server/                 # Network layer
│   ├── mod.rs
│   ├── tcp.rs              # Line protocol TCP listener
│   ├── http.rs             # HTTP query API (tokio + hyper)
│   └── protocol.rs         # Line protocol parser
├── engine/                 # Core database engine
│   ├── mod.rs
│   ├── database.rs         # Top-level DB handle, coordinates components
│   ├── wal.rs              # Write-ahead log (append, recover, truncate)
│   ├── memtable.rs         # In-memory sorted buffer
│   └── config.rs           # Engine configuration
├── storage/                # On-disk storage
│   ├── mod.rs
│   ├── segment.rs          # Columnar segment reader/writer
│   ├── compactor.rs        # Background segment merging
│   ├── partition.rs        # Time-based partitioning logic
│   └── cache.rs            # Segment metadata cache
├── encoding/               # Compression codecs
│   ├── mod.rs
│   ├── timestamp.rs        # Delta-of-delta + varint
│   ├── float.rs            # Gorilla XOR encoding
│   ├── integer.rs          # Delta + zigzag + varint
│   └── boolean.rs          # Bit-packing
├── index/                  # Series & tag indexing
│   ├── mod.rs
│   ├── series.rs           # Series key → ID mapping
│   └── inverted.rs         # Tag inverted index (posting lists)
├── query/                  # Query engine
│   ├── mod.rs
│   ├── parser.rs           # PulseQL parser (hand-written recursive descent)
│   ├── ast.rs              # Query AST types
│   ├── planner.rs          # Query plan generation
│   ├── executor.rs         # Plan execution, segment scanning
│   └── aggregator.rs       # Aggregation functions
├── model/                  # Core data types
│   ├── mod.rs
│   ├── point.rs            # DataPoint, FieldValue, Tags
│   ├── series.rs           # SeriesKey, SeriesID
│   └── schema.rs           # Measurement schema tracking
└── cli/                    # CLI commands
    ├── mod.rs
    ├── server.rs            # `pulsedb server` — start the daemon
    ├── query.rs             # `pulsedb query` — interactive query REPL
    ├── import.rs            # `pulsedb import` — bulk CSV/line-protocol import
    └── status.rs            # `pulsedb status` — show engine stats
```

---

## 8. Build Phases

### Phase 1 — Foundation (Core Engine)
- [ ] Data model types (Point, Series, FieldValue, Tags)
- [ ] Line protocol parser
- [ ] WAL (write, fsync, recovery)
- [ ] MemTable (insert, freeze, iterate)
- [ ] Basic segment writer (columnar, uncompressed)
- [ ] Basic segment reader (mmap, scan)
- [ ] Database engine (write path: parse → WAL → memtable → flush)
- [ ] Unit tests for all components

### Phase 2 — Compression & Indexing
- [ ] Delta-of-delta timestamp encoding
- [ ] Gorilla XOR float encoding
- [ ] Delta + zigzag integer encoding
- [ ] Segment writer with compression
- [ ] Series index (key → ID)
- [ ] Tag inverted index (posting lists)
- [ ] Segment metadata (time range, series list, min/max)
- [ ] Compression benchmarks

### Phase 3 — Query Engine
- [ ] PulseQL lexer + parser
- [ ] AST types
- [ ] Query planner (series resolution, segment selection)
- [ ] Segment scanner with time-range pruning
- [ ] Aggregation functions (count, sum, mean, min, max)
- [ ] GROUP BY time bucketing
- [ ] Query benchmarks

### Phase 4 — Server & API
- [ ] TCP listener for line protocol ingestion
- [ ] HTTP server for queries (JSON response)
- [ ] Health + status endpoints
- [ ] CLI: `pulsedb server`, `pulsedb query`, `pulsedb status`
- [ ] Graceful shutdown with WAL flush

### Phase 5 — Production Hardening
- [ ] Background compactor (merge small segments)
- [ ] Time-based partitioning (auto-create hourly partitions)
- [ ] Retention policies (auto-drop old partitions)
- [ ] Advanced aggregations (percentile, stddev, first, last)
- [ ] Regex tag matching
- [ ] FILL policies (none, null, linear, previous)
- [ ] Bulk import tool (CSV, line protocol files)
- [ ] Comprehensive benchmarks + flamegraph profiling

---

## 9. Performance Targets

| Metric | Target |
|---|---|
| Write throughput | ≥ 1M points/sec (batch ingestion) |
| Single-point write latency | < 10μs (to WAL + memtable) |
| Time-range query (1h, 1 series) | < 1ms |
| Time-range query (1h, 1000 series) | < 50ms |
| Aggregation query (24h, GROUP BY 5m) | < 10ms |
| Compression ratio (float metrics) | ≥ 10× |
| Memory usage (1M active series) | < 2GB |
| Segment flush time (1M points) | < 100ms |

---

## 10. CLI Reference

```
pulsedb server                        Start the database server
  --data-dir <PATH>                     Data directory (default: ./pulsedb_data)
  --tcp-port <PORT>                     Line protocol port (default: 8086)
  --http-port <PORT>                    HTTP API port (default: 8087)
  --wal-fsync <POLICY>                  WAL fsync: every | batch | none (default: batch)
  --memtable-size <BYTES>               Flush threshold (default: 64MB)
  --retention <DURATION>                Auto-drop data older than (e.g., 30d, 1y)

pulsedb query                         Interactive PulseQL REPL
  --host <HOST>                         Server address (default: localhost)
  --port <PORT>                         HTTP port (default: 8087)
  --format <FMT>                        Output: table | json | csv (default: table)

pulsedb import <FILE>                 Bulk import from file
  --format <FMT>                        Input format: line | csv
  --batch-size <N>                      Batch size (default: 10000)

pulsedb status                        Show server statistics
  --host <HOST>                         Server address (default: localhost)
  --port <PORT>                         HTTP port (default: 8087)

pulsedb compact                       Trigger manual compaction
  --measurement <NAME>                  Compact specific measurement

pulsedb version                       Print version
```


<p align="center">

```
                ██████╗ ██╗   ██╗██╗     ███████╗███████╗██████╗ ██████╗
                ██╔══██╗██║   ██║██║     ██╔════╝██╔════╝██╔══██╗██╔══██╗
                ██████╔╝██║   ██║██║     ███████╗█████╗  ██║  ██║██████╔╝
                ██╔═══╝ ██║   ██║██║     ╚════██║██╔══╝  ██║  ██║██╔══██╗
                ██║     ╚██████╔╝███████╗███████║███████╗██████╔╝██████╔╝
                ╚═╝      ╚═════╝ ╚══════╝╚══════╝╚══════╝╚═════╝ ╚═════╝
                     High-Performance Time-Series Database for Rust
```

</p>

<p align="center">
  <a href="https://crates.io/crates/pulsedb"><img src="https://img.shields.io/crates/v/pulsedb.svg" alt="crates.io"></a>
  <a href="https://github.com/matthart1983/pulsedb/blob/main/LICENSE"><img src="https://img.shields.io/crates/l/pulsedb.svg" alt="License: MIT"></a>
</p>

<p align="center">
  <b>A purpose-built time-series database written in pure Rust — columnar storage, type-aware compression, and a SQL-like query language. All from a single binary.</b>
</p>

---

## ✨ Feature Highlights

| Feature | Description |
|---|---|
| **Pure Rust** | Zero C dependencies. Single static binary. Cross-compiles anywhere Rust does. |
| **Columnar Storage** | Fields stored column-by-column in immutable segments for cache-friendly scans and dramatic compression. |
| **Gorilla Float Compression** | Facebook's XOR-based float encoding — 8–15× compression on metric data. |
| **Delta-of-Delta Timestamps** | Regular-interval timestamps compress to ~1 byte per point (10–50×). |
| **Write-Ahead Log** | Append-only WAL with CRC32 checksums. Crash recovery replays unflushed data on startup. |
| **Inverted Tag Index** | Tag key-value pairs map to sorted posting lists. O(n+m) intersection for compound predicates. |
| **Time-Based Partitioning** | Hourly partition directories for fast time-range pruning. Drop old data by deleting directories. |
| **PulseQL** | SQL-like query language: `SELECT mean(cpu) FROM metrics WHERE host='a' GROUP BY time(5m)`. |
| **InfluxDB Line Protocol** | Compatible ingestion format — existing Telegraf, Prometheus, and IoT collectors work out of the box. |
| **LZ4 Compression** | Outer compression layer on encoded columns. ~4GB/s decompression speed. |
| **Concurrent Reads** | `parking_lot::RwLock` for minimal contention between writers and readers. |
| **Background Compactor** | Merges small segments within partitions for fewer files and faster scans. |
| **Retention Policies** | Auto-drop data older than a configurable duration. Delete a directory, reclaim space. |
| **Regex Tag Matching** | `=~` and `!~` operators in PulseQL WHERE clauses for flexible tag filtering. |
| **Schema Enforcement** | Schema-on-write prevents type conflicts — first write sets the type, mismatches are rejected. |

---

## 🏗️ Architecture

```
  TCP :8086                                                    HTTP :8087
  (line protocol)                                              (PulseQL)
       │                                                           │
       ▼                                                           ▼
  ┌─────────┐    ┌─────────────────────────────────────────┐   ┌────────┐
  │  Parser  │───►│              Database Engine            │◄──│ Parser │
  └─────────┘    │                                         │   └────────┘
                 │  WAL ──► MemTable ──► Flush ──► Segment  │
                 │                        │      (columnar) │
                 │                        ▼         ▲       │
                 │                   Compactor ──────┘       │
                 │                                          │
                 │       SeriesIndex ◄── InvertedIndex       │
                 └─────────────────────────────────────────┘
```

### Write Path

1. **Line Protocol Parser** — Parse incoming InfluxDB-compatible text
2. **WAL** — Append-only log with CRC32 for durability
3. **MemTable** — In-memory sorted buffer (BTreeMap per series)
4. **Flush** — When memtable exceeds 64MB, freeze and write columnar segments

### Read Path

1. **PulseQL Parser** — Parse SQL-like query into an AST
2. **Planner** — Resolve series via tag index, prune segments by time range
3. **Executor** — Decompress and scan only needed columns
4. **Aggregator** — Compute `mean`, `sum`, `min`, `max`, `count` with `GROUP BY time(interval)`

---

## 📊 Compression

PulseDB uses type-aware encodings tuned for time-series patterns, then wraps each column in LZ4:

| Data Type | Encoding | Algorithm | Typical Ratio |
|---|---|---|---|
| Timestamps | Delta-of-delta | `delta[i] - delta[i-1]` → zigzag → varint | 10–50× |
| Floats | Gorilla XOR | XOR consecutive values → leading zeros + meaningful bits | 8–15× |
| Integers | Delta + zigzag | Delta encode → zigzag → varint | 5–20× |
| Booleans | Bit-packing | 8 values per byte | 8× |

**Combined**: For typical metric workloads (regular timestamps, slowly changing floats), expect **12–25× total compression** over raw storage.

---

## 📐 Data Model

```
cpu,host=server01,region=us-east usage_idle=98.2,usage_system=1.3 1672531200000000000
│    │                            │                                 │
│    └─ tags (indexed)            └─ fields (values)                └─ timestamp (ns)
measurement
```

- **Measurement** — Logical grouping (like a table)
- **Tags** — Indexed string key-value pairs for filtering and grouping
- **Fields** — The actual data: `f64`, `i64`, `u64`, `bool`
- **Timestamp** — Nanosecond Unix epoch

---

## 📦 Installation

### From Source

```bash
git clone https://github.com/matthart1983/pulsedb.git
cd pulsedb
cargo build --release
# Binary is at ./target/release/pulsedb
```

### From crates.io

```bash
cargo install pulsedb
```

---

## 🚀 Quick Start

### Start the Server

```bash
# Start with defaults (data in ./pulsedb_data, TCP :8086, HTTP :8087)
pulsedb server

# Custom configuration
pulsedb server \
  --data-dir /var/lib/pulsedb \
  --tcp-port 8086 \
  --http-port 8087 \
  --wal-fsync batch \
  --memtable-size 67108864
```

### Write Data (Line Protocol)

Send data over TCP using InfluxDB line protocol:

```bash
# Single point
echo 'cpu,host=server01,region=us-east usage_idle=98.2,usage_system=1.3' | nc localhost 8086

# Batch write
cat <<EOF | nc localhost 8086
cpu,host=server01 usage_idle=98.2,usage_system=1.3 1672531200000000000
cpu,host=server02 usage_idle=95.1,usage_system=3.7 1672531200000000000
mem,host=server01 available=8589934592i,total=17179869184i 1672531200000000000
sensor,device=D-42 temperature=23.5,healthy=t
EOF
```

Or via HTTP:

```bash
curl -X POST http://localhost:8087/write \
  -H 'Content-Type: text/plain' \
  -d 'cpu,host=server01 usage_idle=98.2 1672531200000000000'
```

### Query Data (PulseQL)

```bash
# Interactive REPL
pulsedb query

# HTTP API
curl -X POST http://localhost:8087/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "SELECT mean(usage_idle) FROM cpu WHERE host='\''server01'\'' GROUP BY time(5m)"}'
```

---

## 📝 Query Language — PulseQL

SQL-like, purpose-built for time-series:

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
```

### Aggregation Functions

`count` · `sum` · `mean` / `avg` · `min` · `max` · `first` · `last` · `stddev` · `percentile(field, N)`

### Operators

`=` · `!=` · `>` · `<` · `>=` · `<=` · `=~` (regex) · `!~` · `IN` · `AND` · `OR` · `BETWEEN`

### Duration Syntax

`1ns` · `100us` · `5ms` · `10s` · `5m` · `1h` · `7d` · `2w`

---

## 🔌 Wire Protocol

### Ingestion — TCP :8086

InfluxDB-compatible line protocol. Works with Telegraf, Prometheus remote_write adapters, and any tool that speaks line protocol.

```
<measurement>,<tag1>=<val1> <field1>=<fval1>,<field2>=<fval2> <timestamp_ns>
```

Field type suffixes: `1.0` (float), `1i` (integer), `1u` (unsigned), `t`/`f` (boolean), `"hello"` (string).

### Query — HTTP :8087

| Endpoint | Method | Description |
|---|---|---|
| `/query` | POST | Execute PulseQL query, return JSON |
| `/write` | POST | Ingest line protocol over HTTP |
| `/health` | GET | Liveness check |
| `/status` | GET | Engine statistics (series count, throughput, disk usage) |

---

## ⚙️ Configuration

PulseDB is configured via CLI flags (config file support coming):

| Flag | Default | Description |
|---|---|---|
| `--data-dir` | `./pulsedb_data` | Root directory for all data |
| `--tcp-port` | `8086` | Line protocol ingestion port |
| `--http-port` | `8087` | HTTP query API port |
| `--wal-fsync` | `batch` | WAL fsync policy: `every` / `batch` / `none` |
| `--memtable-size` | `64MB` | Flush threshold for in-memory buffer |
| `--segment-duration` | `3600` | Partition duration in seconds (1 hour) |
| `--retention` | ∞ | Auto-drop data older than duration (e.g., `30d`) |
| `--log-level` | `info` | Logging: `trace` / `debug` / `info` / `warn` / `error` |

### Data Directory Layout

```
pulsedb_data/
├── wal/
│   └── wal.log                    # Write-ahead log
├── partitions/
│   ├── 2024-01-15T14/             # Hourly partition
│   │   ├── cpu_host=server01.seg  # Compressed columnar segment
│   │   └── mem_host=server01.seg
│   └── 2024-01-15T15/
│       └── ...
├── index/
│   ├── series.idx                 # Series key → ID mapping
│   └── tags.idx                   # Tag inverted index
└── meta/
    └── measurements.json          # Schema (field names + types)
```

---

## 🎯 Performance Targets

| Metric | Target |
|---|---|
| Write throughput | ≥ 1M points/sec (batch) |
| Single-point write latency | < 10μs (WAL + memtable) |
| Time-range query (1h, 1 series) | < 1ms |
| Time-range query (1h, 1K series) | < 50ms |
| Aggregation (24h, GROUP BY 5m) | < 10ms |
| Compression ratio (float metrics) | ≥ 10× |
| Memory (1M active series) | < 2GB |
| Segment flush (1M points) | < 100ms |

---

## 🏛️ Tech Stack

| Layer | Crate | Purpose |
|---|---|---|
| Async Runtime | `tokio` | TCP/HTTP server, background tasks |
| Compression | `lz4_flex` | Fast outer compression layer |
| Checksums | `crc32fast` | WAL and segment integrity |
| Concurrency | `parking_lot` | Low-overhead RwLock |
| CLI | `clap` (derive) | Command-line argument parsing |
| Serialization | `serde`, `serde_json` | Config, WAL payload, HTTP responses |
| Time | `chrono` | Partition key formatting |
| Hashing | `xxhash-rust` (xxh3) | Fast non-crypto hashing |
| Memory Mapping | `memmap2` | Zero-copy segment reads |
| Logging | `tracing`, `tracing-subscriber` | Structured logging |
| Errors | `thiserror`, `anyhow` | Error handling |

### Module Structure

```
src/
├── main.rs              # CLI entry point, server bootstrap
├── model/               # DataPoint, FieldValue, Tags, SeriesKey, SeriesId
├── encoding/            # Compression codecs
│   ├── timestamp.rs     # Delta-of-delta + zigzag + varint
│   ├── float.rs         # Gorilla XOR (Facebook paper)
│   ├── integer.rs       # Delta + zigzag + varint
│   └── boolean.rs       # Bit-packing
├── engine/              # Core database engine
│   ├── database.rs      # Write path coordinator
│   ├── wal.rs           # Write-ahead log
│   ├── memtable.rs      # In-memory sorted buffer
│   └── config.rs        # Engine configuration
├── storage/             # On-disk storage
│   ├── segment.rs       # Columnar segment reader/writer
│   ├── partition.rs     # Hourly time partitions
│   ├── cache.rs         # Segment metadata cache
│   └── compactor.rs     # Background segment merging
├── index/               # Series & tag indexing
│   ├── series.rs        # Key → ID mapping
│   └── inverted.rs      # Tag inverted index (posting lists)
├── query/               # Query engine (PulseQL parser, planner, executor)
├── server/              # TCP + HTTP network layer
└── cli/                 # CLI commands (server, query, import, status)
```

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes with tests (`cargo test`)
4. Ensure formatting (`cargo fmt`) and lints pass (`cargo clippy`)
5. Open a pull request

### Building & Testing

```bash
cargo build              # Debug build
cargo build --release    # Optimized release build
cargo test               # Run all tests (198 tests)
cargo clippy             # Lint checks
cargo fmt --check        # Format check
cargo bench              # Run benchmarks
```

---

## 🗺️ Roadmap

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
- [x] CLI (server, query, import, status)
- [x] Background compactor
- [x] Retention policies
- [x] Regex tag matching (=~ and !~ operators)
- [x] Schema enforcement (type-mismatch rejection)
- [x] Criterion benchmarks (ingestion, query, compression)
- [ ] Flamegraph profiling + hot-path optimization
- [ ] GitHub Actions CI

---

## 📄 License

MIT — see [LICENSE](LICENSE) for details.

---

<p align="center">
  <sub>Built with 🦀 Rust — designed for speed, compressed for efficiency</sub>
</p>

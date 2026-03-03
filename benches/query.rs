use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use tempfile::TempDir;

use pulsedb::engine::{Database, EngineConfig};
use pulsedb::model::{DataPoint, FieldValue};

/// Create a database pre-loaded with data for query benchmarks.
fn setup_db(point_count: usize, series_count: usize) -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = EngineConfig {
        data_dir: dir.path().to_path_buf(),
        memtable_size_bytes: 1, // flush immediately so data lands in segments
        ..Default::default()
    };
    let db = Database::open(config).unwrap();

    let base_ts = 1_000_000_000_000_000_000i64; // 1e18 ns
    let interval = 1_000_000_000i64; // 1s in ns

    // Write in batches per series so each flush produces one segment per series.
    for s in 0..series_count {
        let points: Vec<DataPoint> = (0..point_count)
            .map(|i| DataPoint {
                measurement: "cpu".into(),
                tags: BTreeMap::from([("host".into(), format!("server{s:03}"))]),
                fields: BTreeMap::from([
                    ("usage_idle".into(), FieldValue::Float(98.2 - (i % 50) as f64 * 0.1)),
                    ("usage_system".into(), FieldValue::Float(1.3 + (i % 30) as f64 * 0.05)),
                ]),
                timestamp: base_ts + i as i64 * interval,
            })
            .collect();

        db.write(points).unwrap();
    }

    (db, dir)
}

fn bench_raw_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("query/raw_scan");

    let (db, _dir) = setup_db(1_000, 1);
    group.throughput(Throughput::Elements(1_000));

    group.bench_function("select_star_1series_1000pts", |b| {
        b.iter(|| {
            let result = db
                .query(black_box("SELECT usage_idle FROM cpu WHERE host = 'server000'"))
                .unwrap();
            assert!(!result.rows.is_empty());
        });
    });

    group.finish();
}

fn bench_aggregation_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("query/aggregation");

    // 3600 points at 1s interval → 1 hour of data
    let (db, _dir) = setup_db(3_600, 1);

    group.bench_function("mean_group_by_5m_1hour", |b| {
        b.iter(|| {
            let result = db
                .query(black_box(
                    "SELECT mean(usage_idle) FROM cpu WHERE host = 'server000' GROUP BY time(5m)",
                ))
                .unwrap();
            assert!(!result.rows.is_empty());
        });
    });

    group.bench_function("min_max_group_by_1m_1hour", |b| {
        b.iter(|| {
            let result = db
                .query(black_box(
                    "SELECT min(usage_idle), max(usage_idle) FROM cpu WHERE host = 'server000' GROUP BY time(1m)",
                ))
                .unwrap();
            assert!(!result.rows.is_empty());
        });
    });

    group.finish();
}

fn bench_multi_series_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("query/multi_series");

    let (db, _dir) = setup_db(100, 10);

    group.bench_function("mean_10_series", |b| {
        b.iter(|| {
            let result = db
                .query(black_box(
                    "SELECT mean(usage_idle) FROM cpu GROUP BY time(10s)",
                ))
                .unwrap();
            assert!(!result.rows.is_empty());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_raw_query,
    bench_aggregation_query,
    bench_multi_series_query,
);
criterion_main!(benches);

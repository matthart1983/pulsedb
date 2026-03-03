use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use tempfile::TempDir;

use pulsedb::engine::{Database, EngineConfig};
use pulsedb::model::{DataPoint, FieldValue};
use pulsedb::server::protocol::parse_lines;

fn make_batch(size: usize) -> Vec<DataPoint> {
    let base_ts = 1_700_000_000_000_000_000i64;
    let interval = 10_000_000_000i64; // 10s in ns
    (0..size)
        .map(|i| DataPoint {
            measurement: "cpu".into(),
            tags: BTreeMap::from([
                ("host".into(), format!("server{:03}", i % 100)),
                ("region".into(), "us-east".into()),
            ]),
            fields: BTreeMap::from([
                ("usage_idle".into(), FieldValue::Float(98.2 - (i % 50) as f64 * 0.1)),
                ("usage_system".into(), FieldValue::Float(1.3 + (i % 30) as f64 * 0.05)),
            ]),
            timestamp: base_ts + i as i64 * interval,
        })
        .collect()
}

fn bench_write_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingestion/write");

    for batch_size in [100, 1_000, 10_000] {
        let batch = make_batch(batch_size);
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch,
            |b, batch| {
                b.iter_with_setup(
                    || {
                        let dir = TempDir::new().unwrap();
                        let config = EngineConfig {
                            data_dir: dir.path().to_path_buf(),
                            memtable_size_bytes: 256 * 1024 * 1024, // 256 MiB — avoid flush during bench
                            ..Default::default()
                        };
                        let db = Database::open(config).unwrap();
                        (db, dir)
                    },
                    |(db, _dir)| {
                        db.write(black_box(batch.clone())).unwrap();
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_parse_line_protocol(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingestion/parse");

    for line_count in [100, 1_000, 10_000] {
        let input: String = (0..line_count)
            .map(|i| {
                format!(
                    "cpu,host=server{:03},region=us-east usage_idle={:.1},usage_system={:.2} {}\n",
                    i % 100,
                    98.2 - (i % 50) as f64 * 0.1,
                    1.3 + (i % 30) as f64 * 0.05,
                    1_700_000_000_000_000_000i64 + i as i64 * 10_000_000_000,
                )
            })
            .collect();

        group.throughput(Throughput::Elements(line_count as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(line_count),
            &input,
            |b, input| {
                b.iter(|| {
                    let _ = parse_lines(black_box(input)).unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_end_to_end_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingestion/end_to_end");

    let line_count = 1_000;
    let input: String = (0..line_count)
        .map(|i| {
            format!(
                "cpu,host=server{:03},region=us-east usage_idle={:.1} {}\n",
                i % 100,
                98.2 - (i % 50) as f64 * 0.1,
                1_700_000_000_000_000_000i64 + i as i64 * 10_000_000_000,
            )
        })
        .collect();

    group.throughput(Throughput::Elements(line_count as u64));

    group.bench_function("parse_and_write_1000", |b| {
        b.iter_with_setup(
            || {
                let dir = TempDir::new().unwrap();
                let config = EngineConfig {
                    data_dir: dir.path().to_path_buf(),
                    memtable_size_bytes: 256 * 1024 * 1024,
                    ..Default::default()
                };
                let db = Database::open(config).unwrap();
                (db, dir)
            },
            |(db, _dir)| {
                let points = parse_lines(black_box(&input)).unwrap();
                db.write(points).unwrap();
            },
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_write_throughput,
    bench_parse_line_protocol,
    bench_end_to_end_ingest,
);
criterion_main!(benches);

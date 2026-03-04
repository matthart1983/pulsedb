use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use tempfile::TempDir;

use pulsedb::engine::{Database, EngineConfig};
use pulsedb::model::{DataPoint, FieldValue};

fn setup_db(point_count: usize) -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let config = EngineConfig {
        data_dir: dir.path().to_path_buf(),
        memtable_size_bytes: 1,
        ..Default::default()
    };
    let db = Database::open(config).unwrap();

    let base_ts = 1_000_000_000_000i64;
    let sec = 1_000_000_000i64;
    let points: Vec<DataPoint> = (0..point_count)
        .map(|i| DataPoint {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".into(), "web1".into())]),
            fields: BTreeMap::from([(
                "usage".into(),
                FieldValue::Float(50.0 + (i % 100) as f64 * 0.5),
            )]),
            timestamp: base_ts + i as i64 * sec,
        })
        .collect();

    db.write(points).unwrap();
    (db, dir)
}

fn bench_lang_vs_sql(c: &mut Criterion) {
    let mut group = c.benchmark_group("lang_vs_sql");
    let (db, _dir) = setup_db(1_000);
    group.throughput(Throughput::Elements(1_000));

    // Raw column access
    group.bench_function("pulselang_column_access", |b| {
        b.iter(|| {
            let result = db.query_lang(black_box("cpu.usage")).unwrap();
            black_box(result);
        });
    });

    group.bench_function("pulseql_column_access", |b| {
        b.iter(|| {
            let result = db
                .query(black_box("SELECT usage FROM cpu WHERE host = 'web1'"))
                .unwrap();
            black_box(result);
        });
    });

    // Aggregation
    group.bench_function("pulselang_avg", |b| {
        b.iter(|| {
            let result = db.query_lang(black_box("avg cpu.usage")).unwrap();
            black_box(result);
        });
    });

    group.bench_function("pulseql_avg", |b| {
        b.iter(|| {
            let result = db
                .query(black_box(
                    "SELECT mean(usage) FROM cpu WHERE host = 'web1'",
                ))
                .unwrap();
            black_box(result);
        });
    });

    group.finish();
}

fn bench_lang_primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("lang_primitives");
    let (db, _dir) = setup_db(1_000);

    group.bench_function("deltas", |b| {
        b.iter(|| {
            let result = db.query_lang(black_box("deltas cpu.usage")).unwrap();
            black_box(result);
        });
    });

    group.bench_function("mavg_10", |b| {
        b.iter(|| {
            let result = db.query_lang(black_box("mavg[10; cpu.usage]")).unwrap();
            black_box(result);
        });
    });

    group.bench_function("ema_0_1", |b| {
        b.iter(|| {
            let result = db.query_lang(black_box("ema[0.1; cpu.usage]")).unwrap();
            black_box(result);
        });
    });

    group.bench_function("pipeline_avg_deltas", |b| {
        b.iter(|| {
            let result = db.query_lang(black_box("avg deltas cpu.usage")).unwrap();
            black_box(result);
        });
    });

    group.finish();
}

fn bench_pure_interpreter(c: &mut Criterion) {
    let mut group = c.benchmark_group("lang_interpreter");

    group.bench_function("vector_arith_1000", |b| {
        use pulsedb::lang::interpreter::{eval_str_with_env, Env};
        let mut env = Env::new();
        let vec_str: String = (0..1000)
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        eval_str_with_env(&format!("x: {vec_str}"), &mut env).unwrap();
        eval_str_with_env(&format!("y: {vec_str}"), &mut env).unwrap();

        b.iter(|| {
            let result = eval_str_with_env(black_box("x + y"), &mut env).unwrap();
            black_box(result);
        });
    });

    group.bench_function("sum_1000", |b| {
        use pulsedb::lang::interpreter::{eval_str_with_env, Env};
        let mut env = Env::new();
        let vec_str: String = (0..1000)
            .map(|i| format!("{}.0", i))
            .collect::<Vec<_>>()
            .join(" ");
        eval_str_with_env(&format!("v: {vec_str}"), &mut env).unwrap();

        b.iter(|| {
            let result = eval_str_with_env(black_box("sum v"), &mut env).unwrap();
            black_box(result);
        });
    });

    group.bench_function("mavg_10_1000", |b| {
        use pulsedb::lang::interpreter::{eval_str_with_env, Env};
        let mut env = Env::new();
        let vec_str: String = (0..1000)
            .map(|i| format!("{}.0", i))
            .collect::<Vec<_>>()
            .join(" ");
        eval_str_with_env(&format!("v: {vec_str}"), &mut env).unwrap();

        b.iter(|| {
            let result = eval_str_with_env(black_box("mavg[10; v]"), &mut env).unwrap();
            black_box(result);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_lang_vs_sql,
    bench_lang_primitives,
    bench_pure_interpreter,
);
criterion_main!(benches);

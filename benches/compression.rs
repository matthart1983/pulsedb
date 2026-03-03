use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use pulsedb::encoding::{
    decode_booleans, decode_floats, decode_integers, decode_timestamps, encode_booleans,
    encode_floats, encode_integers, encode_timestamps,
};

/// Regular 10-second interval timestamps (typical metric data).
fn regular_timestamps(count: usize) -> Vec<i64> {
    let base = 1_700_000_000_000_000_000i64;
    let interval = 10_000_000_000i64; // 10s in ns
    (0..count).map(|i| base + i as i64 * interval).collect()
}

/// Slowly changing float values (typical CPU usage).
fn cpu_usage_floats(count: usize) -> Vec<f64> {
    (0..count)
        .map(|i| 98.2 - (i % 50) as f64 * 0.1 + (i % 7) as f64 * 0.01)
        .collect()
}

/// Monotonically increasing counters.
fn counter_integers(count: usize) -> Vec<i64> {
    (0..count).map(|i| 1_000_000 + i as i64 * 42).collect()
}

/// Alternating booleans.
fn alternating_booleans(count: usize) -> Vec<bool> {
    (0..count).map(|i| i % 2 == 0).collect()
}

fn bench_timestamp_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression/timestamps");

    for count in [1_000, 10_000, 100_000] {
        let data = regular_timestamps(count);
        let raw_bytes = count * 8;
        group.throughput(Throughput::Bytes(raw_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("encode", count),
            &data,
            |b, data| {
                b.iter(|| encode_timestamps(black_box(data)));
            },
        );

        let encoded = encode_timestamps(&data);
        let ratio = raw_bytes as f64 / encoded.len() as f64;

        group.bench_with_input(
            BenchmarkId::new("decode", count),
            &encoded,
            |b, encoded| {
                b.iter(|| decode_timestamps(black_box(encoded)).unwrap());
            },
        );

        // Print compression ratio (visible in benchmark output).
        println!(
            "  timestamps ({count} points): {raw_bytes} → {} bytes, ratio: {ratio:.1}×",
            encoded.len()
        );
    }

    group.finish();
}

fn bench_float_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression/floats");

    for count in [1_000, 10_000, 100_000] {
        let data = cpu_usage_floats(count);
        let raw_bytes = count * 8;
        group.throughput(Throughput::Bytes(raw_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("encode", count),
            &data,
            |b, data| {
                b.iter(|| encode_floats(black_box(data)));
            },
        );

        let encoded = encode_floats(&data);
        let ratio = raw_bytes as f64 / encoded.len() as f64;

        group.bench_with_input(
            BenchmarkId::new("decode", count),
            &(encoded.clone(), count),
            |b, (encoded, count)| {
                b.iter(|| decode_floats(black_box(encoded), *count).unwrap());
            },
        );

        println!(
            "  floats ({count} points): {raw_bytes} → {} bytes, ratio: {ratio:.1}×",
            encoded.len()
        );
    }

    group.finish();
}

fn bench_integer_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression/integers");

    for count in [1_000, 10_000, 100_000] {
        let data = counter_integers(count);
        let raw_bytes = count * 8;
        group.throughput(Throughput::Bytes(raw_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("encode", count),
            &data,
            |b, data| {
                b.iter(|| encode_integers(black_box(data)));
            },
        );

        let encoded = encode_integers(&data);
        let ratio = raw_bytes as f64 / encoded.len() as f64;

        group.bench_with_input(
            BenchmarkId::new("decode", count),
            &encoded,
            |b, encoded| {
                b.iter(|| decode_integers(black_box(encoded)).unwrap());
            },
        );

        println!(
            "  integers ({count} points): {raw_bytes} → {} bytes, ratio: {ratio:.1}×",
            encoded.len()
        );
    }

    group.finish();
}

fn bench_boolean_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression/booleans");

    for count in [1_000, 10_000, 100_000] {
        let data = alternating_booleans(count);
        let raw_bytes = count; // 1 byte per bool in Rust
        group.throughput(Throughput::Bytes(raw_bytes as u64));

        group.bench_with_input(
            BenchmarkId::new("encode", count),
            &data,
            |b, data| {
                b.iter(|| encode_booleans(black_box(data)));
            },
        );

        let encoded = encode_booleans(&data);
        let ratio = raw_bytes as f64 / encoded.len() as f64;

        group.bench_with_input(
            BenchmarkId::new("decode", count),
            &encoded,
            |b, encoded| {
                b.iter(|| decode_booleans(black_box(encoded)).unwrap());
            },
        );

        println!(
            "  booleans ({count} points): {raw_bytes} → {} bytes, ratio: {ratio:.1}×",
            encoded.len()
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_timestamp_codec,
    bench_float_codec,
    bench_integer_codec,
    bench_boolean_codec,
);
criterion_main!(benches);

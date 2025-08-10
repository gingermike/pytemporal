use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use pprof::criterion::{Output, PProfProfiler};
use bitemporal_timeseries::*;
use chrono::NaiveDate;
use arrow::array::{TimestampMicrosecondArray, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

// Helper function to create test batches
fn create_test_batch(
    data: Vec<(i32, &str, i32, i32, &str, &str, &str, &str)>,
) -> Result<RecordBatch, String> {
    let mut id_builder = Int32Array::builder(data.len());
    let mut field_builder = arrow::array::StringBuilder::new();
    let mut mv_builder = Int32Array::builder(data.len());
    let mut price_builder = Int32Array::builder(data.len());
    let mut eff_from_builder = TimestampMicrosecondArray::builder(data.len());
    let mut eff_to_builder = TimestampMicrosecondArray::builder(data.len());
    let mut as_of_from_builder = TimestampMicrosecondArray::builder(data.len());
    let mut as_of_to_builder = TimestampMicrosecondArray::builder(data.len());
    let mut value_hash_builder = Int64Array::builder(data.len());

    // Constants for date conversion
    const MAX_DATE: NaiveDate = match NaiveDate::from_ymd_opt(2262, 4, 11) {
        Some(date) => date,
        None => panic!("Invalid max date"),
    };
    let _epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    for (id, field, mv, price, eff_from, eff_to, as_of_from, as_of_to) in data {
        id_builder.append_value(id);
        field_builder.append_value(field);
        mv_builder.append_value(mv);
        price_builder.append_value(price);
        
        let eff_from_date = NaiveDate::parse_from_str(eff_from, "%Y-%m-%d")
            .map_err(|e| e.to_string())?;
        let eff_from_datetime = eff_from_date.and_hms_opt(0, 0, 0).unwrap();
        let epoch_datetime = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
        let eff_from_micros = (eff_from_datetime - epoch_datetime).num_microseconds().unwrap();
        eff_from_builder.append_value(eff_from_micros);
        
        let eff_to_date = if eff_to == "max" {
            MAX_DATE
        } else {
            NaiveDate::parse_from_str(eff_to, "%Y-%m-%d")
                .map_err(|e| e.to_string())?
        };
        let eff_to_datetime = eff_to_date.and_hms_opt(0, 0, 0).unwrap();
        let eff_to_micros = (eff_to_datetime - epoch_datetime).num_microseconds().unwrap();
        eff_to_builder.append_value(eff_to_micros);
        
        let as_of_from_date = NaiveDate::parse_from_str(as_of_from, "%Y-%m-%d")
            .map_err(|e| e.to_string())?
            .and_hms_opt(0, 0, 0).unwrap();
        let epoch_datetime = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
        let as_of_from_microseconds = (as_of_from_date - epoch_datetime).num_microseconds().unwrap();
        as_of_from_builder.append_value(as_of_from_microseconds);
        
        let as_of_to_date = if as_of_to == "max" {
            MAX_DATE.and_hms_opt(23, 59, 59).unwrap()
        } else {
            NaiveDate::parse_from_str(as_of_to, "%Y-%m-%d")
                .map_err(|e| e.to_string())?
                .and_hms_opt(23, 59, 59).unwrap()
        };
        let epoch_datetime = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
        let as_of_to_microseconds = (as_of_to_date - epoch_datetime).num_microseconds().unwrap();
        as_of_to_builder.append_value(as_of_to_microseconds);
        
        value_hash_builder.append_value(0); // Placeholder, will be computed
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("field", DataType::Utf8, false),
        Field::new("mv", DataType::Int32, false),
        Field::new("price", DataType::Int32, false),
        Field::new("effective_from", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("effective_to", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("as_of_from", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("as_of_to", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("value_hash", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(field_builder.finish()),
            Arc::new(mv_builder.finish()),
            Arc::new(price_builder.finish()),
            Arc::new(eff_from_builder.finish()),
            Arc::new(eff_to_builder.finish()),
            Arc::new(as_of_from_builder.finish()),
            Arc::new(as_of_to_builder.finish()),
            Arc::new(value_hash_builder.finish()),
        ],
    )
    .map_err(|e| e.to_string())
}

fn bench_small_dataset(c: &mut Criterion) {
    let current_state = create_test_batch(vec![
        (1, "A", 100, 1000, "2024-01-01", "2024-04-01", "2024-01-01", "max"),
        (1, "A", 200, 2000, "2024-04-01", "2024-08-01", "2024-01-01", "max"),
        (1, "A", 300, 3000, "2024-08-01", "2024-12-31", "2024-01-01", "max"),
        (2, "B", 150, 1500, "2024-01-01", "2024-06-01", "2024-01-01", "max"),
        (2, "B", 250, 2500, "2024-06-01", "2024-12-31", "2024-01-01", "max"),
    ]).unwrap();

    let updates = create_test_batch(vec![
        (1, "A", 999, 9999, "2024-03-01", "2024-09-01", "2024-07-21", "max"),
        (2, "B", 888, 8888, "2024-05-01", "2024-07-01", "2024-07-21", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2024, 7, 21).unwrap();
    let id_columns = vec!["id".to_string(), "field".to_string()];
    let value_columns = vec!["mv".to_string(), "price".to_string()];

    c.bench_function("small_dataset", |b| {
        b.iter(|| {
            black_box(process_updates(
                black_box(current_state.clone()),
                black_box(updates.clone()),
                black_box(id_columns.clone()),
                black_box(value_columns.clone()),
                black_box(system_date),
                black_box(UpdateMode::Delta),
            ).unwrap())
        })
    });
}

fn bench_medium_dataset(c: &mut Criterion) {
    // Create a medium-sized dataset with 100 current records and 20 updates
    let mut current_data = Vec::new();
    for i in 0..100 {
        current_data.push((
            i / 10,  // 10 records per ID
            "field",
            100 + i,
            1000 + i,
            "2024-01-01",
            "2024-12-31", 
            "2024-01-01",
            "max"
        ));
    }

    let mut update_data = Vec::new();
    for i in 0..20 {
        update_data.push((
            i / 2,  // 2 updates per ID
            "field",
            999,
            9999,
            "2024-06-01",
            "2024-08-01",
            "2024-07-21",
            "max"
        ));
    }

    let current_state = create_test_batch(current_data).unwrap();
    let updates = create_test_batch(update_data).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2024, 7, 21).unwrap();
    let id_columns = vec!["id".to_string(), "field".to_string()];
    let value_columns = vec!["mv".to_string(), "price".to_string()];

    c.bench_function("medium_dataset", |b| {
        b.iter(|| {
            black_box(process_updates(
                black_box(current_state.clone()),
                black_box(updates.clone()),
                black_box(id_columns.clone()),
                black_box(value_columns.clone()),
                black_box(system_date),
                black_box(UpdateMode::Delta),
            ).unwrap())
        })
    });
}

fn bench_conflation_effectiveness(c: &mut Criterion) {
    // Test conflation effectiveness with many adjacent same-value segments
    let current_state = create_test_batch(vec![
        (1, "A", 100, 1000, "2024-01-01", "2024-02-01", "2024-01-01", "max"),
        (1, "A", 100, 1000, "2024-02-01", "2024-03-01", "2024-01-01", "max"),
        (1, "A", 100, 1000, "2024-03-01", "2024-04-01", "2024-01-01", "max"),
        (1, "A", 100, 1000, "2024-04-01", "2024-05-01", "2024-01-01", "max"),
        (1, "A", 100, 1000, "2024-05-01", "2024-06-01", "2024-01-01", "max"),
    ]).unwrap();

    let updates = create_test_batch(vec![
        (1, "A", 999, 9999, "2024-01-15", "2024-05-15", "2024-07-21", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2024, 7, 21).unwrap();
    let id_columns = vec!["id".to_string(), "field".to_string()];
    let value_columns = vec!["mv".to_string(), "price".to_string()];

    c.bench_function("conflation_effectiveness", |b| {
        b.iter(|| {
            black_box(process_updates(
                black_box(current_state.clone()),
                black_box(updates.clone()),
                black_box(id_columns.clone()),
                black_box(value_columns.clone()),
                black_box(system_date),
                black_box(UpdateMode::Delta),
            ).unwrap())
        })
    });
}

fn bench_scaling_by_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling_by_dataset_size");

    for size in [10, 50, 100, 500, 500_000].iter() {
        // Adjust the sample size based on the dataset size. Larger datasets
        // take longer per iteration, so we reduce the number of samples to
        // keep total benchmark time reasonable. Tune these values as needed.
        let sample_size = match *size {
            500_000 => 10,
            500     => 10,
            100     => 20,
            _       => 30,
        };
        group.sample_size(sample_size);

        let mut current_data = Vec::new();
        for i in 0..*size {
            current_data.push((
                i / 10,  // Multiple records per ID
                "field",
                100 + i,
                1000 + i,
                "2024-01-01",
                "2024-12-31",
                "2024-01-01",
                "max"
            ));
        }

        let mut update_data = Vec::new();
        let num_updates = size / 5; // 20% of records get updates
        for i in 0..num_updates {
            update_data.push((
                i / 2,
                "field",
                999,
                9999,
                "2024-06-01",
                "2024-08-01",
                "2024-07-21",
                "max"
            ));
        }

        let current_state = create_test_batch(current_data).unwrap();
        let updates = create_test_batch(update_data).unwrap();
        let system_date = NaiveDate::from_ymd_opt(2024, 7, 21).unwrap();
        let id_columns = vec!["id".to_string(), "field".to_string()];
        let value_columns = vec!["mv".to_string(), "price".to_string()];

        group.bench_with_input(BenchmarkId::new("records", size), size, |b, _size| {
            b.iter(|| {
                black_box(process_updates(
                    black_box(current_state.clone()),
                    black_box(updates.clone()),
                    black_box(id_columns.clone()),
                    black_box(value_columns.clone()),
                    black_box(system_date),
                    black_box(UpdateMode::Delta),
                ).unwrap())
            })
        });
    }

    group.finish();
}

fn bench_parallel_effectiveness(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_effectiveness");
    
    // Test scenarios with different ID distributions
    for (scenario, num_ids, records_per_id) in [
        ("few_ids_many_records", 10, 1000),    // Low parallelism
        ("many_ids_few_records", 1000, 10),    // High parallelism  
        ("balanced_workload", 100, 100),       // Balanced
    ].iter() {
        let mut current_data = Vec::new();
        let mut update_data = Vec::new();
        
        // Create test data with specified ID distribution
        for id in 0..*num_ids {
            for record in 0..*records_per_id {
                current_data.push((
                    id,
                    "field",
                    100 + record,
                    1000 + record,
                    "2024-01-01",
                    "2024-12-31",
                    "2024-01-01",
                    "max"
                ));
            }
            
            // Add some updates for each ID
            for update in 0..(*records_per_id / 10).max(1) {
                update_data.push((
                    id,
                    "field", 
                    999 + update,
                    9999 + update,
                    "2024-06-01",
                    "2024-08-01",
                    "2024-07-21",
                    "max"
                ));
            }
        }

        let current_state = create_test_batch(current_data).unwrap();
        let updates = create_test_batch(update_data).unwrap();

        let system_date = NaiveDate::from_ymd_opt(2024, 7, 21).unwrap();
        let id_columns = vec!["id".to_string(), "field".to_string()];
        let value_columns = vec!["mv".to_string(), "price".to_string()];

        group.bench_with_input(BenchmarkId::new("scenario", scenario), scenario, |b, _scenario| {
            b.iter(|| {
                black_box(process_updates(
                    black_box(current_state.clone()),
                    black_box(updates.clone()),
                    black_box(id_columns.clone()),
                    black_box(value_columns.clone()),
                    black_box(system_date),
                    black_box(UpdateMode::Delta),
                ).unwrap())
            })
        });
    }
    group.finish();
}

fn profiled() -> Criterion {
    Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
}

criterion_group! {
    name = benches;
    config = profiled();
    targets = bench_small_dataset, bench_medium_dataset, bench_conflation_effectiveness, bench_scaling_by_size, bench_parallel_effectiveness
}
criterion_main!(benches);
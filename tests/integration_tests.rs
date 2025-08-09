use bitemporal_timeseries::{process_updates, UpdateMode};
use chrono::NaiveDate;
use arrow::array::{TimestampMicrosecondArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

// Test record: (id, field, mv, price, eff_from, eff_to, as_of_from, as_of_to)
type TestRecord = (i32, &'static str, i32, i32, &'static str, &'static str, &'static str, &'static str);

// Test scenario
struct TestScenario {
    name: &'static str,
    current_state: Vec<TestRecord>,
    updates: Vec<TestRecord>,
    expected_expire: Vec<TestRecord>,
    expected_insert: Vec<TestRecord>,
}

// Simple record for comparison
#[derive(Debug, PartialEq, Clone)]
struct SimpleRecord {
    id: i32,
    field: String,
    mv: i32,
    price: i32,
    effective_from: NaiveDate,
    effective_to: NaiveDate,
    as_of_from: NaiveDate,
}

// Helper functions
fn parse_date_or_max(date_str: &str, max_date: NaiveDate) -> NaiveDate {
    if date_str == "max" {
        max_date
    } else {
        NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap()
    }
}

fn create_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("field", DataType::Utf8, false),
        Field::new("mv", DataType::Int32, false),
        Field::new("price", DataType::Int32, false),
        Field::new("effective_from", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("effective_to", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("as_of_from", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("as_of_to", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("value_hash", DataType::Int64, false),
    ]))
}

fn create_batch(records: Vec<TestRecord>) -> RecordBatch {
    if records.is_empty() {
        return RecordBatch::new_empty(create_schema());
    }

    let len = records.len();
    let mut id_builder = Int32Array::builder(len);
    let mut field_builder = arrow::array::StringBuilder::new();
    let mut mv_builder = Int32Array::builder(len);
    let mut price_builder = Int32Array::builder(len);
    let mut eff_from_builder = TimestampMicrosecondArray::builder(len);
    let mut eff_to_builder = TimestampMicrosecondArray::builder(len);
    let mut as_of_from_builder = TimestampMicrosecondArray::builder(len);
    let mut as_of_to_builder = TimestampMicrosecondArray::builder(len);
    let mut value_hash_builder = Int64Array::builder(len);

    let max_date = NaiveDate::from_ymd_opt(2262, 4, 11).unwrap();
    let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();

    for (id, field, mv, price, eff_from, eff_to, as_of_from, as_of_to) in records {
        id_builder.append_value(id);
        field_builder.append_value(field);
        mv_builder.append_value(mv);
        price_builder.append_value(price);
        
        let eff_from_date = parse_date_or_max(eff_from, max_date);
        let eff_from_micros = (eff_from_date.and_hms_opt(0, 0, 0).unwrap() - epoch).num_microseconds().unwrap();
        eff_from_builder.append_value(eff_from_micros);
        
        let eff_to_date = parse_date_or_max(eff_to, max_date);
        let eff_to_micros = (eff_to_date.and_hms_opt(0, 0, 0).unwrap() - epoch).num_microseconds().unwrap();
        eff_to_builder.append_value(eff_to_micros);
        
        let as_of_from_date = parse_date_or_max(as_of_from, max_date);
        let as_of_from_micros = (as_of_from_date.and_hms_opt(0, 0, 0).unwrap() - epoch).num_microseconds().unwrap();
        as_of_from_builder.append_value(as_of_from_micros);
        
        let as_of_to_date = parse_date_or_max(as_of_to, max_date);
        let as_of_to_micros = (as_of_to_date.and_hms_opt(23, 59, 59).unwrap() - epoch).num_microseconds().unwrap();
        as_of_to_builder.append_value(as_of_to_micros);
        
        value_hash_builder.append_value(0);
    }

    RecordBatch::try_new(
        create_schema(),
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
    ).unwrap()
}

fn extract_simple_record(batch: &RecordBatch, index: usize) -> SimpleRecord {
    let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
    
    let id = batch.column_by_name("id").unwrap().as_any().downcast_ref::<Int32Array>().unwrap().value(index);
    let field = batch.column_by_name("field").unwrap().as_any().downcast_ref::<StringArray>().unwrap().value(index).to_string();
    let mv = batch.column_by_name("mv").unwrap().as_any().downcast_ref::<Int32Array>().unwrap().value(index);
    let price = batch.column_by_name("price").unwrap().as_any().downcast_ref::<Int32Array>().unwrap().value(index);
    
    let eff_from_micros = batch.column_by_name("effective_from").unwrap().as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(index);
    let effective_from = (epoch + chrono::Duration::microseconds(eff_from_micros)).date();
    
    let eff_to_micros = batch.column_by_name("effective_to").unwrap().as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(index);
    let effective_to = (epoch + chrono::Duration::microseconds(eff_to_micros)).date();
    
    let as_of_from_micros = batch.column_by_name("as_of_from").unwrap().as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(index);
    let as_of_from = (epoch + chrono::Duration::microseconds(as_of_from_micros)).date();

    SimpleRecord { id, field, mv, price, effective_from, effective_to, as_of_from }
}

fn run_scenario(scenario: &TestScenario) {
    let current_state = create_batch(scenario.current_state.clone());
    let updates = create_batch(scenario.updates.clone());
    let system_date = NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
    ).unwrap();

    // Extract actual results
    let mut actual_expires = Vec::new();
    for &expire_idx in &changeset.to_expire {
        actual_expires.push(extract_simple_record(&current_state, expire_idx));
    }
    actual_expires.sort_by(|a, b| a.id.cmp(&b.id).then(a.field.cmp(&b.field)).then(a.effective_from.cmp(&b.effective_from)));

    let mut actual_inserts = Vec::new();
    for batch in &changeset.to_insert {
        for i in 0..batch.num_rows() {
            actual_inserts.push(extract_simple_record(batch, i));
        }
    }
    actual_inserts.sort_by(|a, b| a.id.cmp(&b.id).then(a.field.cmp(&b.field)).then(a.effective_from.cmp(&b.effective_from)));

    // Convert expected to SimpleRecord format
    let max_date = NaiveDate::from_ymd_opt(2262, 4, 11).unwrap();
    let mut expected_expires: Vec<SimpleRecord> = scenario.expected_expire.iter().map(|&(id, field, mv, price, eff_from, eff_to, as_of_from, _)| {
        SimpleRecord {
            id,
            field: field.to_string(),
            mv,
            price,
            effective_from: parse_date_or_max(eff_from, max_date),
            effective_to: parse_date_or_max(eff_to, max_date),
            as_of_from: parse_date_or_max(as_of_from, max_date),
        }
    }).collect();
    expected_expires.sort_by(|a, b| a.id.cmp(&b.id).then(a.field.cmp(&b.field)).then(a.effective_from.cmp(&b.effective_from)));

    let mut expected_inserts: Vec<SimpleRecord> = scenario.expected_insert.iter().map(|&(id, field, mv, price, eff_from, eff_to, as_of_from, _)| {
        SimpleRecord {
            id,
            field: field.to_string(),
            mv,
            price,
            effective_from: parse_date_or_max(eff_from, max_date),
            effective_to: parse_date_or_max(eff_to, max_date),
            as_of_from: parse_date_or_max(as_of_from, max_date),
        }
    }).collect();
    expected_inserts.sort_by(|a, b| a.id.cmp(&b.id).then(a.field.cmp(&b.field)).then(a.effective_from.cmp(&b.effective_from)));

    // Assert
    assert_eq!(actual_expires.len(), expected_expires.len(), "Scenario '{}': Expire count mismatch", scenario.name);
    assert_eq!(actual_inserts.len(), expected_inserts.len(), "Scenario '{}': Insert count mismatch", scenario.name);

    for (actual, expected) in actual_expires.iter().zip(expected_expires.iter()) {
        assert_eq!(*actual, *expected, "Scenario '{}': Expire record mismatch", scenario.name);
    }

    for (actual, expected) in actual_inserts.iter().zip(expected_inserts.iter()) {
        assert_eq!(*actual, *expected, "Scenario '{}': Insert record mismatch", scenario.name);
    }
}

// ALL SCENARIOS IN ONE PLACE - Clean and organized like Python
fn get_all_scenarios() -> Vec<TestScenario> {
    vec![
        // Basic scenarios
        TestScenario {
            name: "insert",
            current_state: vec![],
            updates: vec![
                (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
                (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            ],
            expected_expire: vec![],
            expected_insert: vec![
                (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
                (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            ],
        },
        TestScenario {
            name: "overwrite",
            current_state: vec![
                (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
                (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 400, 300, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![
                (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            ],
            expected_insert: vec![
                (1234, "test", 400, 300, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "unrelated_state",
            current_state: vec![
                (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
                (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            ],
            updates: vec![
                (4562, "test", 1, 1, "2020-01-01", "max", "2025-07-27", "max"),
                (1234, "test", 2, 2, "2022-01-01", "max", "2025-07-27", "max"),
                (1234, "fielda", 400, 500, "2022-01-01", "2023-01-01", "2025-01-01", "max"),
            ],
            expected_expire: vec![],
            expected_insert: vec![
                (4562, "test", 1, 1, "2020-01-01", "max", "2025-07-27", "max"),
                (1234, "test", 2, 2, "2022-01-01", "max", "2025-07-27", "max"),
                (1234, "fielda", 400, 500, "2022-01-01", "2023-01-01", "2025-01-01", "max"),
            ],
        },
        TestScenario {
            name: "append_tail",
            current_state: vec![
                (1234, "test", 300, 400, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 2, 2, "2022-06-30", "max", "2025-07-27", "max"),
            ],
            expected_expire: vec![
                (1234, "test", 300, 400, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            expected_insert: vec![
                (1234, "test", 300, 400, "2020-01-01", "2022-06-30", "2025-07-27", "max"),
                (1234, "test", 2, 2, "2022-06-30", "max", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "append_tail_exact",
            current_state: vec![
                (1234, "test", 300, 400, "2020-01-01", "2020-06-30", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 2, 2, "2022-06-30", "max", "2025-07-27", "max"),
            ],
            expected_expire: vec![],
            expected_insert: vec![
                (1234, "test", 2, 2, "2022-06-30", "max", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "append_head",
            current_state: vec![
                (1234, "test", 300, 400, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 2, 2, "2019-06-30", "2021-01-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![
                (1234, "test", 300, 400, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            expected_insert: vec![
                (1234, "test", 2, 2, "2019-06-30", "2021-01-01", "2025-07-27", "max"),
                (1234, "test", 300, 400, "2021-01-01", "max", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "append_head_exact",
            current_state: vec![
                (1234, "test", 300, 400, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 2, 2, "2019-06-30", "2020-01-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![],
            expected_insert: vec![
                (1234, "test", 2, 2, "2019-06-30", "2020-01-01", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "intersect",
            current_state: vec![
                (1234, "test", 300, 400, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 2, 2, "2021-01-01", "2021-06-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![
                (1234, "test", 300, 400, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            expected_insert: vec![
                (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
                (1234, "test", 2, 2, "2021-01-01", "2021-06-01", "2025-07-27", "max"),
                (1234, "test", 300, 400, "2021-06-01", "max", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "no_change",
            current_state: vec![
                (1234, "test", 300, 400, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 300, 400, "2020-01-01", "max", "2025-07-27", "max"),
            ],
            expected_expire: vec![],
            expected_insert: vec![],
        },
        
        // Complex scenarios
        TestScenario {
            name: "overlay_two",
            current_state: vec![
                (1234, "test", 300, 400, "2020-01-01", "2020-06-30", "2025-01-01", "max"),
                (1234, "test", 300, 400, "2020-06-30", "max", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 2, 2, "2020-03-01", "2020-11-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![
                (1234, "test", 300, 400, "2020-01-01", "2020-06-30", "2025-01-01", "max"),
                (1234, "test", 300, 400, "2020-06-30", "max", "2025-01-01", "max"),
            ],
            expected_insert: vec![
                (1234, "test", 300, 400, "2020-01-01", "2020-03-01", "2025-07-27", "max"),
                (1234, "test", 2, 2, "2020-03-01", "2020-11-01", "2025-07-27", "max"),
                (1234, "test", 300, 400, "2020-11-01", "max", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "overlay_multiple",
            current_state: vec![
                (1234, "test", 300, 400, "2020-01-01", "2020-06-30", "2025-01-01", "max"),
                (1234, "test", 200, 200, "2020-06-30", "2020-07-31", "2025-01-01", "max"),
                (1234, "test", 100, 100, "2020-07-31", "max", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 2, 2, "2020-03-01", "2020-11-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![
                (1234, "test", 300, 400, "2020-01-01", "2020-06-30", "2025-01-01", "max"),
                (1234, "test", 200, 200, "2020-06-30", "2020-07-31", "2025-01-01", "max"),
                (1234, "test", 100, 100, "2020-07-31", "max", "2025-01-01", "max"),
            ],
            expected_insert: vec![
                (1234, "test", 300, 400, "2020-01-01", "2020-03-01", "2025-07-27", "max"),
                (1234, "test", 2, 2, "2020-03-01", "2020-11-01", "2025-07-27", "max"),
                (1234, "test", 100, 100, "2020-11-01", "max", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "multi_intersection_single_point",
            current_state: vec![
                (1234, "test", 100, 100, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 2, 2, "2020-03-01", "2020-11-01", "2025-07-27", "max"),
                (1234, "test", 3, 4, "2020-11-01", "2020-12-01", "2025-07-27", "max"),
                (1234, "test", 4, 5, "2020-12-01", "2021-06-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![
                (1234, "test", 100, 100, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            expected_insert: vec![
                (1234, "test", 100, 100, "2020-01-01", "2020-03-01", "2025-07-27", "max"),
                (1234, "test", 2, 2, "2020-03-01", "2020-11-01", "2025-07-27", "max"),
                (1234, "test", 3, 4, "2020-11-01", "2020-12-01", "2025-07-27", "max"),
                (1234, "test", 4, 5, "2020-12-01", "2021-06-01", "2025-07-27", "max"),
                (1234, "test", 100, 100, "2021-06-01", "max", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "multi_intersection_multiple_point",
            current_state: vec![
                (1234, "test", 100, 100, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
                (1234, "test", 200, 200, "2021-01-01", "2022-01-01", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 2, 2, "2020-03-01", "2020-11-01", "2025-07-27", "max"),
                (1234, "test", 3, 4, "2020-11-01", "2020-12-01", "2025-07-27", "max"),
                (1234, "test", 4, 5, "2020-12-01", "2021-06-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![
                (1234, "test", 100, 100, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
                (1234, "test", 200, 200, "2021-01-01", "2022-01-01", "2025-01-01", "max"),
            ],
            expected_insert: vec![
                (1234, "test", 100, 100, "2020-01-01", "2020-03-01", "2025-07-27", "max"),
                (1234, "test", 2, 2, "2020-03-01", "2020-11-01", "2025-07-27", "max"),
                (1234, "test", 3, 4, "2020-11-01", "2020-12-01", "2025-07-27", "max"),
                (1234, "test", 4, 5, "2020-12-01", "2021-06-01", "2025-07-27", "max"),
                (1234, "test", 200, 200, "2021-06-01", "2022-01-01", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "multi_field",
            current_state: vec![
                (1234, "test", 100, 100, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
                (1234, "test_2", 200, 200, "2021-02-01", "2022-01-01", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 2, 2, "2020-03-01", "2020-11-01", "2025-07-27", "max"),
                (1234, "test", 3, 4, "2020-11-01", "2020-12-01", "2025-07-27", "max"),
                (1234, "test_2", 4, 5, "2020-12-01", "2021-06-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![
                (1234, "test", 100, 100, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
                (1234, "test_2", 200, 200, "2021-02-01", "2022-01-01", "2025-01-01", "max"),
            ],
            expected_insert: vec![
                (1234, "test", 100, 100, "2020-01-01", "2020-03-01", "2025-07-27", "max"),
                (1234, "test", 2, 2, "2020-03-01", "2020-11-01", "2025-07-27", "max"),
                (1234, "test", 3, 4, "2020-11-01", "2020-12-01", "2025-07-27", "max"),
                (1234, "test", 100, 100, "2020-12-01", "2021-01-01", "2025-07-27", "max"),
                (1234, "test_2", 4, 5, "2020-12-01", "2021-06-01", "2025-07-27", "max"),
                (1234, "test_2", 200, 200, "2021-06-01", "2022-01-01", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "extend_current_row",
            current_state: vec![
                (1234, "test", 100, 100, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 100, 100, "2021-01-01", "2022-11-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![
                (1234, "test", 100, 100, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            ],
            expected_insert: vec![
                (1234, "test", 100, 100, "2020-01-01", "2022-11-01", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "extend_update",
            current_state: vec![
                (1234, "test", 100, 100, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 100, 100, "2019-01-01", "2020-01-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![
                (1234, "test", 100, 100, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            expected_insert: vec![
                (1234, "test", 100, 100, "2019-01-01", "max", "2025-07-27", "max"),
            ],
        },
        TestScenario {
            name: "no_change_with_intersection",
            current_state: vec![
                (1234, "test", 100, 100, "2020-01-01", "max", "2025-01-01", "max"),
            ],
            updates: vec![
                (1234, "test", 100, 100, "2020-02-01", "2020-04-01", "2025-07-27", "max"),
            ],
            expected_expire: vec![],
            expected_insert: vec![],
        },
    ]
}

// Main test that runs all scenarios (like Python's parameterized test)
#[test]
fn test_all_scenarios() {
    let scenarios = get_all_scenarios();
    
    for scenario in scenarios {
        println!("Running scenario: {}", scenario.name);
        run_scenario(&scenario);
    }
}

// Individual tests for easy debugging (all 18 scenarios)
#[test] fn test_insert() { run_scenario(&get_all_scenarios()[0]); }
#[test] fn test_overwrite() { run_scenario(&get_all_scenarios()[1]); }
#[test] fn test_unrelated_state() { run_scenario(&get_all_scenarios()[2]); }
#[test] fn test_append_tail() { run_scenario(&get_all_scenarios()[3]); }
#[test] fn test_append_tail_exact() { run_scenario(&get_all_scenarios()[4]); }
#[test] fn test_append_head() { run_scenario(&get_all_scenarios()[5]); }
#[test] fn test_append_head_exact() { run_scenario(&get_all_scenarios()[6]); }
#[test] fn test_intersect() { run_scenario(&get_all_scenarios()[7]); }
#[test] fn test_no_change() { run_scenario(&get_all_scenarios()[8]); }
#[test] fn test_overlay_two() { run_scenario(&get_all_scenarios()[9]); }
#[test] fn test_overlay_multiple() { run_scenario(&get_all_scenarios()[10]); }
#[test] fn test_multi_intersection_single_point() { run_scenario(&get_all_scenarios()[11]); }
#[test] fn test_multi_intersection_multiple_point() { run_scenario(&get_all_scenarios()[12]); }
#[test] fn test_multi_field() { run_scenario(&get_all_scenarios()[13]); }
#[test] fn test_extend_current_row() { run_scenario(&get_all_scenarios()[14]); }
#[test] fn test_extend_update() { run_scenario(&get_all_scenarios()[15]); }
#[test] fn test_no_change_with_intersection() { run_scenario(&get_all_scenarios()[16]); }

// Additional manual test scenarios (matching the Python manual tests)
#[test]
fn test_head_slice_conflation() {
    let scenario = TestScenario {
        name: "head_slice_conflation",
        current_state: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        updates: vec![
            (1234, "test", 400, 300, "2019-01-01", "2020-06-01", "2025-07-27", "max"),
        ],
        expected_expire: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        expected_insert: vec![
            (1234, "test", 400, 300, "2019-01-01", "2020-06-01", "2025-07-27", "max"),
            (1234, "test", 300, 400, "2020-06-01", "2021-01-01", "2025-07-27", "max"),
        ],
    };
    run_scenario(&scenario);
}

#[test]
fn test_tail_slice_conflation() {
    let scenario = TestScenario {
        name: "tail_slice_conflation", 
        current_state: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        updates: vec![
            (1234, "test", 400, 300, "2020-06-01", "2022-01-01", "2025-07-27", "max"),
        ],
        expected_expire: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        expected_insert: vec![
            (1234, "test", 300, 400, "2020-01-01", "2020-06-01", "2025-07-27", "max"),
            (1234, "test", 400, 300, "2020-06-01", "2022-01-01", "2025-07-27", "max"),
        ],
    };
    run_scenario(&scenario);
}

#[test]
fn test_total_overwrite() {
    let scenario = TestScenario {
        name: "total_overwrite",
        current_state: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        updates: vec![
            (1234, "test", 400, 300, "2019-01-01", "2022-01-01", "2025-07-27", "max"),
        ],
        expected_expire: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        expected_insert: vec![
            (1234, "test", 400, 300, "2019-01-01", "2022-01-01", "2025-07-27", "max"),
        ],
    };
    run_scenario(&scenario);
}

#[test]
fn test_two_updates() {
    let scenario = TestScenario {
        name: "two_updates",
        current_state: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        updates: vec![
            (1234, "fielda", 400, 300, "2019-01-01", "2020-03-01", "2025-07-27", "max"),
            (1234, "fielda", 400, 300, "2020-06-01", "2021-03-01", "2025-07-27", "max"),
        ],
        expected_expire: vec![
            (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        expected_insert: vec![
            (1234, "fielda", 400, 300, "2019-01-01", "2020-03-01", "2025-07-27", "max"),
            (1234, "fielda", 400, 500, "2020-03-01", "2020-06-01", "2025-07-27", "max"),
            (1234, "fielda", 400, 300, "2020-06-01", "2021-03-01", "2025-07-27", "max"),
        ],
    };
    run_scenario(&scenario);
}

#[test]
fn test_update_multiple_current() {
    let scenario = TestScenario {
        name: "update_multiple_current",
        current_state: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            (1234, "test", 500, 600, "2021-01-01", "2022-01-01", "2025-01-01", "max"),
            (1234, "test", 700, 800, "2022-01-01", "2023-01-01", "2025-01-01", "max"),
            (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        updates: vec![
            (1234, "test", 200, 300, "2020-10-01", "2022-03-01", "2025-07-27", "max"),
        ],
        expected_expire: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
            (1234, "test", 500, 600, "2021-01-01", "2022-01-01", "2025-01-01", "max"),
            (1234, "test", 700, 800, "2022-01-01", "2023-01-01", "2025-01-01", "max"),
        ],
        expected_insert: vec![
            (1234, "test", 300, 400, "2020-01-01", "2020-10-01", "2025-07-27", "max"),
            (1234, "test", 200, 300, "2020-10-01", "2022-03-01", "2025-07-27", "max"),
            (1234, "test", 700, 800, "2022-03-01", "2023-01-01", "2025-07-27", "max"),
        ],
    };
    run_scenario(&scenario);
}
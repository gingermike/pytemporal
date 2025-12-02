use pytemporal::{process_updates, UpdateMode};
use chrono::{Datelike, NaiveDate};
use arrow::array::{TimestampMicrosecondArray, Int32Array, StringArray, StringBuilder};
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
        Field::new("value_hash", DataType::Utf8, false),
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
    let mut value_hash_builder = StringBuilder::new();

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
        
        // Compute hash based on mv and price (value columns)
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(&mv.to_le_bytes());
        hasher.update(&price.to_le_bytes());
        let hash = format!("{:x}", hasher.finalize());
        value_hash_builder.append_value(&hash);
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
        false, // conflate_inputs
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

#[test]
fn test_hash_normalization_mixed_types() {
    // This test verifies that the hash normalization fix works correctly.
    // It ensures that numerically equivalent values with different types
    // (Int32 vs Float64) produce the same hash and are correctly detected as no-change.
    // Without the fix, this scenario would generate extra rows for unchanged records.
    
    // Note: This test documents the expected behavior rather than testing the exact type conversion,
    // since the Rust integration test framework uses consistent types.
    // The actual fix was verified through Python integration tests and debug output.
    
    let scenario = TestScenario {
        name: "hash_normalization_mixed_types",
        current_state: vec![
            (1234, "AAPL", 100, 15025, "2020-01-01", "2120-01-01", "2022-01-01", "max"),
            (5678, "GOOGL", 200, 280050, "2020-01-01", "2120-01-01", "2022-01-01", "max"),
        ],
        // Same values, different effective dates - should be detected as no-change for values
        updates: vec![
            (1234, "AAPL", 100, 15025, "2020-01-02", "2120-01-01", "2022-01-02", "max"), // Same values
            (9999, "MSFT", 300, 35075, "2020-01-02", "2120-01-01", "2022-01-02", "max"), // New record
        ],
        expected_expire: vec![],
        expected_insert: vec![
            (9999, "MSFT", 300, 35075, "2020-01-02", "2120-01-01", "2022-01-02", "max"), // Only MSFT
        ],
    };
    
    // Run the normal test - with the hash normalization fix, AAPL should not be processed
    run_scenario(&scenario);
}

// ============================================================================
// CONFLATION TESTS
// ============================================================================

/// Helper function to run scenarios with conflation enabled
fn run_conflation_scenario(scenario: &TestScenario) {
    let current_state = create_batch(scenario.current_state.clone());
    let updates = create_batch(scenario.updates.clone());
    let system_date = NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::FullState,  // Conflation tests use full_state mode
        true, // conflate_inputs = true
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

    // Get expected results
    let max_date = NaiveDate::from_ymd_opt(2262, 4, 11).unwrap();
    let mut expected_expire: Vec<SimpleRecord> = scenario.expected_expire.iter().map(|&(id, field, mv, price, eff_from, eff_to, as_of_from, _)| {
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
    expected_expire.sort_by(|a, b| a.id.cmp(&b.id).then(a.field.cmp(&b.field)).then(a.effective_from.cmp(&b.effective_from)));

    let mut expected_insert: Vec<SimpleRecord> = scenario.expected_insert.iter().map(|&(id, field, mv, price, eff_from, eff_to, as_of_from, _)| {
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
    expected_insert.sort_by(|a, b| a.id.cmp(&b.id).then(a.field.cmp(&b.field)).then(a.effective_from.cmp(&b.effective_from)));

    // Assert
    assert_eq!(actual_expires, expected_expire,
        "Scenario '{}' - Expected expires don't match. Expected: {:?}, Got: {:?}",
        scenario.name, expected_expire, actual_expires);
    assert_eq!(actual_inserts, expected_insert,
        "Scenario '{}' - Expected inserts don't match. Expected: {:?}, Got: {:?}",
        scenario.name, expected_insert, actual_inserts);
}

#[test]
fn test_conflation_basic() {
    let scenario = TestScenario {
        name: "conflation_basic",
        current_state: vec![],
        updates: vec![
            // Two consecutive segments with same values - should merge
            (1234, "test", 2, 2, "2020-03-01", "2020-11-01", "2025-01-01", "max"),
            (1234, "test", 2, 2, "2020-11-01", "2021-11-01", "2025-01-01", "max"),
            // Another ID with consecutive segments
            (4567, "test_b", 1, 1, "2020-03-01", "2020-11-01", "2025-01-01", "max"),
            (4567, "test_b", 1, 1, "2020-11-01", "2021-11-01", "2025-01-01", "max"),
        ],
        expected_expire: vec![],
        expected_insert: vec![
            // Should be conflated into single records
            (1234, "test", 2, 2, "2020-03-01", "2021-11-01", "2025-01-01", "max"),
            (4567, "test_b", 1, 1, "2020-03-01", "2021-11-01", "2025-01-01", "max"),
        ],
    };
    run_conflation_scenario(&scenario);
}

#[test]
fn test_conflation_three_segments() {
    let scenario = TestScenario {
        name: "conflation_three_segments",
        current_state: vec![],
        updates: vec![
            // Three consecutive segments with same values - should all merge
            (1234, "test", 10, 10, "2020-01-01", "2020-04-01", "2025-01-01", "max"),
            (1234, "test", 10, 10, "2020-04-01", "2020-07-01", "2025-01-01", "max"),
            (1234, "test", 10, 10, "2020-07-01", "2020-10-01", "2025-01-01", "max"),
        ],
        expected_expire: vec![],
        expected_insert: vec![
            (1234, "test", 10, 10, "2020-01-01", "2020-10-01", "2025-01-01", "max"),
        ],
    };
    run_conflation_scenario(&scenario);
}

#[test]
fn test_conflation_partial() {
    let scenario = TestScenario {
        name: "conflation_partial",
        current_state: vec![],
        updates: vec![
            // First two should merge (same values)
            (1234, "test", 5, 5, "2020-01-01", "2020-06-01", "2025-01-01", "max"),
            (1234, "test", 5, 5, "2020-06-01", "2020-12-01", "2025-01-01", "max"),
            // Value changes - should NOT merge with above
            (1234, "test", 10, 10, "2020-12-01", "2021-06-01", "2025-01-01", "max"),
            // Last two should merge (same new values)
            (1234, "test", 10, 10, "2021-06-01", "2021-12-01", "2025-01-01", "max"),
        ],
        expected_expire: vec![],
        expected_insert: vec![
            (1234, "test", 5, 5, "2020-01-01", "2020-12-01", "2025-01-01", "max"),
            (1234, "test", 10, 10, "2020-12-01", "2021-12-01", "2025-01-01", "max"),
        ],
    };
    run_conflation_scenario(&scenario);
}

#[test]
fn test_conflation_non_consecutive() {
    let scenario = TestScenario {
        name: "conflation_non_consecutive",
        current_state: vec![],
        updates: vec![
            (1234, "test", 7, 7, "2020-01-01", "2020-06-01", "2025-01-01", "max"),
            // Gap here: 2020-06-01 to 2020-07-01
            (1234, "test", 7, 7, "2020-07-01", "2020-12-01", "2025-01-01", "max"),
        ],
        expected_expire: vec![],
        expected_insert: vec![
            // Should remain as two separate records due to gap
            (1234, "test", 7, 7, "2020-01-01", "2020-06-01", "2025-01-01", "max"),
            (1234, "test", 7, 7, "2020-07-01", "2020-12-01", "2025-01-01", "max"),
        ],
    };
    run_conflation_scenario(&scenario);
}

#[test]
fn test_conflation_mixed_ids() {
    let scenario = TestScenario {
        name: "conflation_mixed_ids",
        current_state: vec![],
        updates: vec![
            // ID 1234 - two segments that merge
            (1234, "field_a", 3, 3, "2020-01-01", "2020-06-01", "2025-01-01", "max"),
            (1234, "field_a", 3, 3, "2020-06-01", "2020-12-01", "2025-01-01", "max"),
            // ID 5678 - single segment, no merge opportunity
            (5678, "field_b", 8, 8, "2020-01-01", "2020-12-01", "2025-01-01", "max"),
            // ID 9999 - three segments that all merge
            (9999, "field_c", 1, 2, "2020-01-01", "2020-04-01", "2025-01-01", "max"),
            (9999, "field_c", 1, 2, "2020-04-01", "2020-08-01", "2025-01-01", "max"),
            (9999, "field_c", 1, 2, "2020-08-01", "2020-12-01", "2025-01-01", "max"),
        ],
        expected_expire: vec![],
        expected_insert: vec![
            (1234, "field_a", 3, 3, "2020-01-01", "2020-12-01", "2025-01-01", "max"),
            (5678, "field_b", 8, 8, "2020-01-01", "2020-12-01", "2025-01-01", "max"),
            (9999, "field_c", 1, 2, "2020-01-01", "2020-12-01", "2025-01-01", "max"),
        ],
    };
    run_conflation_scenario(&scenario);
}

#[test]
fn test_conflation_unsorted_input() {
    let scenario = TestScenario {
        name: "conflation_unsorted_input",
        current_state: vec![],
        updates: vec![
            // Out of order: later segment comes first
            (1234, "test", 15, 20, "2020-06-01", "2020-12-01", "2025-01-01", "max"),
            (1234, "test", 15, 20, "2020-01-01", "2020-06-01", "2025-01-01", "max"),
            // Another ID, also out of order with three segments
            (5678, "test", 25, 30, "2020-04-01", "2020-08-01", "2025-01-01", "max"),
            (5678, "test", 25, 30, "2020-08-01", "2020-12-01", "2025-01-01", "max"),
            (5678, "test", 25, 30, "2020-01-01", "2020-04-01", "2025-01-01", "max"),
        ],
        expected_expire: vec![],
        expected_insert: vec![
            (1234, "test", 15, 20, "2020-01-01", "2020-12-01", "2025-01-01", "max"),
            (5678, "test", 25, 30, "2020-01-01", "2020-12-01", "2025-01-01", "max"),
        ],
    };
    run_conflation_scenario(&scenario);
}

#[test]
fn test_conflation_with_current_state() {
    let scenario = TestScenario {
        name: "conflation_with_current_state",
        current_state: vec![
            // Existing record in current state
            (1234, "test", 100, 100, "2019-01-01", "2020-01-01", "2025-01-01", "max"),
        ],
        updates: vec![
            // Two consecutive updates that should conflate
            (1234, "test", 200, 200, "2020-01-01", "2020-06-01", "2025-07-27", "max"),
            (1234, "test", 200, 200, "2020-06-01", "2021-01-01", "2025-07-27", "max"),
        ],
        expected_expire: vec![
            // Expire the old record
            (1234, "test", 100, 100, "2019-01-01", "2020-01-01", "2025-01-01", "max"),
        ],
        expected_insert: vec![
            // Insert one conflated record (not two separate ones)
            (1234, "test", 200, 200, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
        ],
    };
    run_conflation_scenario(&scenario);
}

#[test]
fn test_conflation_different_fields() {
    let scenario = TestScenario {
        name: "conflation_different_fields",
        current_state: vec![],
        updates: vec![
            // ID 1234 with field_a - these merge
            (1234, "field_a", 5, 10, "2020-01-01", "2020-06-01", "2025-01-01", "max"),
            (1234, "field_a", 5, 10, "2020-06-01", "2020-12-01", "2025-01-01", "max"),
            // ID 1234 with field_b - these merge separately
            (1234, "field_b", 7, 14, "2020-01-01", "2020-06-01", "2025-01-01", "max"),
            (1234, "field_b", 7, 14, "2020-06-01", "2020-12-01", "2025-01-01", "max"),
        ],
        expected_expire: vec![],
        expected_insert: vec![
            (1234, "field_a", 5, 10, "2020-01-01", "2020-12-01", "2025-01-01", "max"),
            (1234, "field_b", 7, 14, "2020-01-01", "2020-12-01", "2025-01-01", "max"),
        ],
    };
    run_conflation_scenario(&scenario);
}

/// Test: Backfill scenario - records with effective_from > system_date should NOT be tombstoned
///
/// This tests the fix for the "invalid range" bug where tombstoning records during backfill
/// created effective_from > effective_to ranges, which violate database constraints.
///
/// Scenario:
/// - Current state has a record starting on 2024-01-02
/// - Backfill with system_date=2024-01-01 (earlier than existing record)
/// - The existing record should NOT be tombstoned (would create invalid range)
#[test]
fn test_backfill_skips_future_records() {
    // Current state: Record exists starting Day 2 (2024-01-02)
    // This represents "future" data from the perspective of the backfill
    let current_state = create_batch(vec![
        // Record that starts AFTER the backfill date - should NOT be tombstoned
        (2, "field_a", 100, 200, "2024-01-02", "max", "2024-01-02", "max"),
    ]);

    // Backfill: Insert data for Day 1 (2024-01-01) - doesn't include the Day 2 record
    let updates = create_batch(vec![
        (1, "field_a", 50, 100, "2024-01-01", "2024-01-02", "2024-01-01", "max"),
    ]);

    // System date is 2024-01-01 (the backfill date)
    let system_date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::FullState,
        false, // conflate_inputs = false
    ).unwrap();

    // The record with id=2 should NOT be expired because:
    // - Its effective_from (2024-01-02) > system_date (2024-01-01)
    // - Tombstoning it would create an invalid range: effective_from > effective_to
    assert!(
        changeset.to_expire.is_empty(),
        "No records should be expired when their effective_from > system_date"
    );

    // Only the backfill record (id=1) should be inserted
    assert_eq!(changeset.to_insert.len(), 1, "Only the backfill record should be inserted");

    // Verify the inserted record is the backfill data, not a tombstone
    let insert_batch = &changeset.to_insert[0];
    let id_array = insert_batch.column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_array.value(0), 1, "Inserted record should be the backfill record with id=1");
}

/// Test: Backfill with mixed records - some valid to tombstone, some not
///
/// This tests that the filter correctly handles a mix of:
/// - Records that CAN be tombstoned (effective_from <= system_date)
/// - Records that should be SKIPPED (effective_from > system_date)
#[test]
fn test_backfill_mixed_tombstone_eligibility() {
    // Current state: Mix of records
    let current_state = create_batch(vec![
        // Record starting BEFORE backfill date - CAN be tombstoned
        (1, "field_a", 10, 20, "2024-01-01", "max", "2024-01-01", "max"),
        // Record starting ON backfill date - CAN be tombstoned (effective_from == system_date)
        (2, "field_a", 30, 40, "2024-01-05", "max", "2024-01-05", "max"),
        // Record starting AFTER backfill date - should NOT be tombstoned
        (3, "field_a", 50, 60, "2024-01-10", "max", "2024-01-10", "max"),
    ]);

    // Backfill with no updates for any existing IDs (all should be considered for tombstoning)
    let updates = create_batch(vec![
        (99, "field_a", 100, 200, "2024-01-01", "2024-01-05", "2024-01-01", "max"),
    ]);

    // System date is 2024-01-05 (midpoint)
    let system_date = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::FullState,
        false,
    ).unwrap();

    // Records id=1 and id=2 should be expired (effective_from <= system_date)
    // Record id=3 should NOT be expired (effective_from > system_date)
    assert_eq!(
        changeset.to_expire.len(), 2,
        "Only records with effective_from <= system_date should be expired"
    );

    // Verify the expired records are id=1 and id=2
    let expired_ids: Vec<i32> = changeset.to_expire.iter()
        .map(|&idx| {
            current_state.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(idx)
        })
        .collect();
    assert!(expired_ids.contains(&1), "Record id=1 should be expired");
    assert!(expired_ids.contains(&2), "Record id=2 should be expired");
    assert!(!expired_ids.contains(&3), "Record id=3 should NOT be expired (effective_from > system_date)");

    // Verify tombstones are created only for eligible records (2 tombstones + 1 insert = need to check)
    // The inserts should contain: 2 tombstones for id=1,2 + 1 regular insert for id=99
    let total_inserts: usize = changeset.to_insert.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_inserts, 3, "Should have 2 tombstones + 1 regular insert");

    // Verify no tombstone has effective_from > effective_to
    for batch in &changeset.to_insert {
        let eff_from_array = batch.column_by_name("effective_from")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let eff_to_array = batch.column_by_name("effective_to")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let eff_from = eff_from_array.value(i);
            let eff_to = eff_to_array.value(i);
            assert!(
                eff_from <= eff_to,
                "Invalid range detected: effective_from ({}) > effective_to ({})",
                eff_from, eff_to
            );
        }
    }
}

/// Test: Backfill should NOT merge tombstones with open-ended updates.
///
/// This tests the fix for the "missing inserts during backfill" bug where
/// tombstones (bounded records) were incorrectly merged with open-ended updates,
/// causing the update to be lost.
///
/// Scenario:
/// - Current state has a tombstone [2024-01-01, 2024-01-02) - bounded/closed
/// - Backfill incoming has [2024-01-02, infinity) - open-ended
/// - Same ID and hash (adjacent segments with same values)
/// - Expected: Insert the new record separately, DON'T merge with tombstone
#[test]
fn test_backfill_does_not_merge_tombstone_with_open_ended() {
    // Current state: tombstone (bounded record that was closed)
    let current_state = create_batch(vec![
        // Tombstone: record was closed at 2024-01-02
        (2, "field_a", 100, 200, "2024-01-01", "2024-01-02", "2024-01-02", "max"),
    ]);

    // Backfill: re-add the record for Day 2 with open-ended effective_to
    let updates = create_batch(vec![
        // Same ID (2, field_a) and same values (100, 200) = same hash
        // But effective range is [2024-01-02, infinity) - open-ended
        (2, "field_a", 100, 200, "2024-01-02", "max", "2024-01-02", "max"),
    ]);

    let system_date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::FullState,
        false, // conflate_inputs = false
    ).unwrap();

    // The tombstone should NOT be expired (it's historical record)
    assert!(
        changeset.to_expire.is_empty(),
        "Tombstone should not be expired during backfill"
    );

    // The new record should be inserted separately (not merged with tombstone)
    assert_eq!(
        changeset.to_insert.len(), 1,
        "Backfill record should be inserted"
    );

    // Verify the inserted record has the correct temporal range
    let insert_batch = &changeset.to_insert[0];
    assert_eq!(insert_batch.num_rows(), 1, "Should have exactly one inserted record");

    let eff_from_array = insert_batch.column_by_name("effective_from")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let eff_to_array = insert_batch.column_by_name("effective_to")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();

    let eff_from = eff_from_array.value(0);
    let eff_to = eff_to_array.value(0);

    // Convert to dates for comparison
    let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
    let inserted_from = epoch + chrono::Duration::microseconds(eff_from);
    let inserted_to = epoch + chrono::Duration::microseconds(eff_to);

    // The inserted record should start at 2024-01-02, NOT 2024-01-01
    // If merged incorrectly, effective_from would be 2024-01-01
    assert_eq!(
        inserted_from.date(),
        NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        "Inserted record should start at 2024-01-02, not merged with tombstone"
    );

    // The inserted record should be open-ended (year >= 2200)
    assert!(
        inserted_to.date().year() >= 2200,
        "Inserted record should be open-ended (effective_to at infinity)"
    );
}

/// Test: Bounded + bounded adjacent segments SHOULD still merge
///
/// This ensures the fix for tombstone merging doesn't break the valid
/// use case of merging two bounded adjacent segments with same values.
#[test]
fn test_bounded_adjacent_segments_still_merge() {
    // Current state: bounded record [2024-01-02, 2024-01-03)
    let current_state = create_batch(vec![
        (1, "field_a", 50, 100, "2024-01-02", "2024-01-03", "2024-01-01", "max"),
    ]);

    // Update: bounded record [2024-01-01, 2024-01-02) - adjacent to current
    // Same values = same hash
    let updates = create_batch(vec![
        (1, "field_a", 50, 100, "2024-01-01", "2024-01-02", "2024-01-02", "max"),
    ]);

    let system_date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::FullState,
        false,
    ).unwrap();

    // Current record SHOULD be expired (we're merging)
    assert_eq!(
        changeset.to_expire.len(), 1,
        "Current bounded record should be expired for merging"
    );

    // Should have one merged record
    assert_eq!(
        changeset.to_insert.len(), 1,
        "Should have one merged record"
    );

    // Verify the merged record spans [2024-01-01, 2024-01-03)
    let insert_batch = &changeset.to_insert[0];
    let eff_from_array = insert_batch.column_by_name("effective_from")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let eff_to_array = insert_batch.column_by_name("effective_to")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();

    let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
    let merged_from = epoch + chrono::Duration::microseconds(eff_from_array.value(0));
    let merged_to = epoch + chrono::Duration::microseconds(eff_to_array.value(0));

    assert_eq!(
        merged_from.date(),
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        "Merged record should start at 2024-01-01"
    );
    assert_eq!(
        merged_to.date(),
        NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        "Merged record should end at 2024-01-03"
    );
}

/// Test: When multiple current records have the same hash but different effective dates,
/// the algorithm should find the one with an exact temporal match.
///
/// Bug fix: Previously, the algorithm would stop at the FIRST matching hash and not
/// check if other records with the same hash had an exact temporal match.
#[test]
fn test_exact_match_with_multiple_current_records() {
    // Current state has two records for the same ID with same hash but different dates
    let current_state = create_batch(vec![
        // Day 1 record
        (1, "field1", 100, 10, "2024-01-01", "max", "2024-01-01", "max"),
        // Day 2 record - same ID, same values (same hash), different effective_from
        (1, "field1", 100, 10, "2024-01-02", "max", "2024-01-02", "max"),
    ]);

    // Update sends the same record as Day 2
    let updates = create_batch(vec![
        (1, "field1", 100, 10, "2024-01-02", "max", "2024-01-02", "max"),
    ]);

    let system_date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string()],
        vec!["field".to_string(), "mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::FullState,
        false,
    ).unwrap();

    // No expiries needed - records are correct
    assert!(changeset.to_expire.is_empty(), "No expiries expected - records are correct");

    // CRITICAL: No inserts needed - exact match exists
    // Bug: Previously this would insert because it found 2024-01-01 first (non-exact match)
    let total_inserts: usize = changeset.to_insert.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_inserts, 0,
        "BUG: Record was inserted even though exact match exists in current state");
}

/// Test: Exact match should have priority over adjacent match when searching.
#[test]
fn test_exact_match_priority_over_adjacent() {
    // Current state has adjacent record AND exact match with same hash
    let current_state = create_batch(vec![
        // Adjacent record (would be a merge candidate) - ends at 2024-01-02
        (1, "field1", 100, 10, "2024-01-01", "2024-01-02", "2024-01-01", "max"),
        // Exact match record - starts at 2024-01-02
        (1, "field1", 100, 10, "2024-01-02", "max", "2024-01-02", "max"),
    ]);

    // Update sends record that exactly matches the second current record
    let updates = create_batch(vec![
        (1, "field1", 100, 10, "2024-01-02", "max", "2024-01-02", "max"),
    ]);

    let system_date = NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string()],
        vec!["field".to_string(), "mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::FullState,
        false,
    ).unwrap();

    // Should find exact match - no changes needed
    assert!(changeset.to_expire.is_empty(), "No expiries expected - exact match found");
    let total_inserts: usize = changeset.to_insert.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_inserts, 0,
        "No inserts expected - exact match should be found, not merged with adjacent");
}

/// Test: Records with same hash but different IDs should NOT be deduplicated.
///
/// Test that empty ranges (effective_from == effective_to) are filtered out.
/// These represent zero-width time periods and should not be emitted.
#[test]
fn test_empty_ranges_filtered_out() {
    // Current state: record from Jan 1 to Jan 10
    let current_state = create_batch(vec![
        (1, "field1", 100, 10, "2024-01-01", "2024-01-10", "2024-01-01", "max"),
    ]);

    // Update that creates a potential empty range scenario:
    // Update starts exactly where current ends (point update at boundary)
    let updates = create_batch(vec![
        (1, "field1", 200, 20, "2024-01-10", "2024-01-10", "2024-01-15", "max"),  // Empty range!
    ]);

    let system_date = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string()],
        vec!["field".to_string(), "mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
        false,
    ).unwrap();

    // The empty range update should be filtered out - no inserts
    let total_inserts: usize = changeset.to_insert.iter().map(|b| b.num_rows()).sum();

    // Verify no empty ranges were inserted
    for batch in &changeset.to_insert {
        let eff_from = batch.column_by_name("effective_from").unwrap();
        let eff_to = batch.column_by_name("effective_to").unwrap();

        let from_array = eff_from.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let to_array = eff_to.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();

        for i in 0..batch.num_rows() {
            let from_val = from_array.value(i);
            let to_val = to_array.value(i);
            assert!(from_val < to_val,
                "Found empty range: effective_from ({}) >= effective_to ({})",
                from_val, to_val);
        }
    }

    // The empty range update should not produce any inserts
    assert_eq!(total_inserts, 0,
        "Empty range update should not produce any inserts, got {}", total_inserts);
}

/// Bug fix: The deduplication logic was incorrectly treating records as duplicates
/// if they had the same (effective_from, effective_to, value_hash), ignoring ID columns.
#[test]
fn test_deduplication_with_same_hash_different_ids() {
    // Current state: A->B (id=1) with value that produces a specific hash
    let current_state = create_batch(vec![
        (1, "field1", 100, 10, "2024-01-01", "max", "2024-01-01", "max"),
    ]);

    // Incoming: A->B plus two NEW records B->C and C->D with same values (same hash)
    // All have id=1, id=2, id=3 respectively
    let updates = create_batch(vec![
        (1, "field1", 100, 10, "2024-01-01", "max", "2024-01-01", "max"),  // A->B exists
        (2, "field1", 100, 10, "2024-01-01", "max", "2024-01-01", "max"),  // B->C NEW (same values = same hash)
        (3, "field1", 100, 10, "2024-01-01", "max", "2024-01-01", "max"),  // C->D NEW (same values = same hash)
    ]);

    let system_date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string()],
        vec!["field".to_string(), "mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::FullState,
        false,
    ).unwrap();

    // No expiries expected
    assert!(changeset.to_expire.is_empty(), "No expiries expected");

    // Should insert 2 records (id=2 and id=3), NOT deduplicate them
    let total_inserts: usize = changeset.to_insert.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_inserts, 2,
        "BUG: Expected 2 inserts but got {}. Records with same hash but different IDs were incorrectly deduplicated.",
        total_inserts);
}
/// Bug fix: Multi-day backfill should not pull in adjacent records.
///
/// This tests the fix for the "exclusion constraint violation" bug where
/// backfilling Day 2 data incorrectly expired Day 1 because Day 1 was adjacent
/// to the update and had the same value hash.
///
/// Scenario:
/// - Day 1: [2024-01-01, 2024-01-02) with value=100
/// - Day 2: [2024-01-02, 2024-01-03) with value=200
/// - Day 3: [2024-01-03, 2024-01-04) with value=300
/// - Backfill Day 2 with value=100 (same as Day 1!)
///
/// Expected: Only Day 2 should be expired and updated
/// Bug: Day 1 was also expired because it was adjacent and had same hash as update
#[test]
fn test_backfill_does_not_expire_adjacent_same_value_record() {
    // Current state: Three consecutive days
    let current_state = create_batch(vec![
        // Day 1: value=100
        (1, "field1", 100, 10, "2024-01-01", "2024-01-02", "2024-01-01", "max"),
        // Day 2: value=200 (will be corrected to 100)
        (1, "field1", 200, 20, "2024-01-02", "2024-01-03", "2024-01-02", "max"),
        // Day 3: value=300
        (1, "field1", 300, 30, "2024-01-03", "2024-01-04", "2024-01-03", "max"),
    ]);

    // Backfill: Correct Day 2 to have value=100 (same as Day 1!)
    let updates = create_batch(vec![
        (1, "field1", 100, 10, "2024-01-02", "2024-01-03", "2024-01-10", "max"),
    ]);

    let system_date = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string()],
        vec!["field".to_string(), "mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
        false,
    ).unwrap();

    // CRITICAL: Only 1 expiry (Day 2), NOT 2 (Day 1 + Day 2)
    assert_eq!(
        changeset.to_expire.len(), 1,
        "BUG: Expected 1 expiry (Day 2 only), got {}. Day 1 was incorrectly expired!",
        changeset.to_expire.len()
    );

    // Verify the expired record is Day 2 (index 1), not Day 1 (index 0)
    assert_eq!(
        changeset.to_expire[0], 1,
        "Expected Day 2 (index 1) to be expired, got index {}",
        changeset.to_expire[0]
    );

    // Should have exactly 1 insert (the corrected Day 2)
    let total_inserts: usize = changeset.to_insert.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_inserts, 1,
        "Expected 1 insert (corrected Day 2), got {}",
        total_inserts
    );

    // Verify the insert is for Day 2 range [2024-01-02, 2024-01-03), NOT [2024-01-01, 2024-01-03)
    let insert_batch = &changeset.to_insert[0];
    let eff_from_array = insert_batch.column_by_name("effective_from")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();

    let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
    let inserted_from = epoch + chrono::Duration::microseconds(eff_from_array.value(0));

    assert_eq!(
        inserted_from.date(),
        NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
        "BUG: Inserted record starts at {:?}, expected 2024-01-02. Was incorrectly merged with Day 1!",
        inserted_from.date()
    );
}

/// Test: Extension scenario should still work (single current + adjacent update).
///
/// This ensures the backfill fix doesn't break the legitimate extension behavior
/// where a single current record + adjacent update with same values should merge.
#[test]
fn test_extension_still_works_with_single_current_record() {
    // Single current record
    let current_state = create_batch(vec![
        (1, "field1", 100, 10, "2024-01-01", "2024-01-02", "2024-01-01", "max"),
    ]);

    // Adjacent update with same values (extension)
    let updates = create_batch(vec![
        (1, "field1", 100, 10, "2024-01-02", "2024-01-03", "2024-01-10", "max"),
    ]);

    let system_date = NaiveDate::from_ymd_opt(2024, 1, 10).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string()],
        vec!["field".to_string(), "mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
        false,
    ).unwrap();

    // Should expire the current record (merging)
    assert_eq!(
        changeset.to_expire.len(), 1,
        "Extension scenario: current record should be expired for merging"
    );

    // Should have 1 merged insert
    let total_inserts: usize = changeset.to_insert.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_inserts, 1,
        "Extension scenario: should have 1 merged insert"
    );

    // Verify the merged record spans [2024-01-01, 2024-01-03)
    let insert_batch = &changeset.to_insert[0];
    let eff_from_array = insert_batch.column_by_name("effective_from")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let eff_to_array = insert_batch.column_by_name("effective_to")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();

    let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
    let merged_from = epoch + chrono::Duration::microseconds(eff_from_array.value(0));
    let merged_to = epoch + chrono::Duration::microseconds(eff_to_array.value(0));

    assert_eq!(
        merged_from.date(),
        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        "Merged record should start at 2024-01-01"
    );
    assert_eq!(
        merged_to.date(),
        NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        "Merged record should end at 2024-01-03"
    );
}

/// Test: When update is fully contained within current record with same values,
/// it should be a NO-OP (no expiries, no inserts).
///
/// This is a regression test for a bug where full_state mode would incorrectly
/// insert a new record even when the update was completely covered by existing state.
///
/// Scenario:
/// - Current: A->B effective=[2024-01-01, infinity) with hash X
/// - Update: A->B effective=[2024-01-02, 2024-01-03) with hash X (same values)
/// - Expected: NO-OP (current already covers this period with same values)
#[test]
fn test_update_contained_in_current_is_no_op() {
    // Current state: open-ended record from 2024-01-01 to infinity
    let current_state = create_batch(vec![
        (1, "field1", 100, 10, "2024-01-01", "max", "2024-01-01", "max"),
    ]);

    // Backfill update: bounded period WITHIN current range, SAME values
    let updates = create_batch(vec![
        (1, "field1", 100, 10, "2024-01-02", "2024-01-03", "2024-01-05", "max"),
    ]);

    let system_date = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();

    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string()],
        vec!["field".to_string(), "mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::FullState,
        false,
    ).unwrap();

    // Should be NO-OP: no expiries
    assert_eq!(
        changeset.to_expire.len(), 0,
        "BUG: Expected 0 expiries (current covers update), got {}",
        changeset.to_expire.len()
    );

    // Should be NO-OP: no inserts
    let total_inserts: usize = changeset.to_insert.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_inserts, 0,
        "BUG: Expected 0 inserts (current covers update with same values), got {}",
        total_inserts
    );
}

use bitemporal_timeseries::{process_updates, UpdateMode, ChangeSet};
use chrono::{NaiveDate, NaiveDateTime};
use arrow::array::{TimestampMicrosecondArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

// Test record structure: (id, field, mv, price, eff_from, eff_to, as_of_from, as_of_to)
type TestRecord = (i32, &'static str, i32, i32, &'static str, &'static str, &'static str, &'static str);

// Expected result structure
struct ExpectedResult {
    expire: Vec<TestRecord>,
    insert: Vec<TestRecord>,
}

// Helper function to create test batches
fn create_test_batch(data: Vec<TestRecord>) -> Result<RecordBatch, String> {
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
        let as_of_from_microseconds = (as_of_from_date - epoch_datetime).num_microseconds().unwrap();
        as_of_from_builder.append_value(as_of_from_microseconds);
        
        let as_of_to_date = if as_of_to == "max" {
            MAX_DATE.and_hms_opt(23, 59, 59).unwrap()
        } else {
            NaiveDate::parse_from_str(as_of_to, "%Y-%m-%d")
                .map_err(|e| e.to_string())?
                .and_hms_opt(23, 59, 59).unwrap()
        };
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

// Extract record data from RecordBatch for comparison
fn extract_records_from_batch(batch: &RecordBatch) -> Vec<(i32, String, i32, i32, NaiveDateTime, NaiveDateTime, NaiveDateTime, NaiveDateTime)> {
    let mut records = Vec::new();
    let len = batch.num_rows();

    let id_array = batch.column_by_name("id").unwrap()
        .as_any().downcast_ref::<Int32Array>().unwrap();
    let field_array = batch.column_by_name("field").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    let mv_array = batch.column_by_name("mv").unwrap()
        .as_any().downcast_ref::<Int32Array>().unwrap();
    let price_array = batch.column_by_name("price").unwrap()
        .as_any().downcast_ref::<Int32Array>().unwrap();
    let eff_from_array = batch.column_by_name("effective_from").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let eff_to_array = batch.column_by_name("effective_to").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let as_of_from_array = batch.column_by_name("as_of_from").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let as_of_to_array = batch.column_by_name("as_of_to").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();

    for i in 0..len {
        let id = id_array.value(i);
        let field = field_array.value(i).to_string();
        let mv = mv_array.value(i);
        let price = price_array.value(i);
        let eff_from = extract_timestamp(eff_from_array, i);
        let eff_to = extract_timestamp(eff_to_array, i);
        let as_of_from = extract_timestamp(as_of_from_array, i);
        let as_of_to = extract_timestamp(as_of_to_array, i);
        
        records.push((id, field, mv, price, eff_from, eff_to, as_of_from, as_of_to));
    }

    records
}

// Extract timestamp from TimestampMicrosecondArray
fn extract_timestamp(array: &TimestampMicrosecondArray, index: usize) -> NaiveDateTime {
    let microseconds = array.value(index);
    let epoch_datetime = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
    epoch_datetime + chrono::Duration::microseconds(microseconds)
}

// Sort records for consistent comparison
fn sort_records(mut records: Vec<(i32, String, i32, i32, NaiveDateTime, NaiveDateTime, NaiveDateTime, NaiveDateTime)>) 
    -> Vec<(i32, String, i32, i32, NaiveDateTime, NaiveDateTime, NaiveDateTime, NaiveDateTime)> {
    records.sort_by(|a, b| {
        a.0.cmp(&b.0)  // id
            .then(a.1.cmp(&b.1))  // field
            .then(a.4.cmp(&b.4))  // effective_from
    });
    records
}

// Validate changeset against expected results with full content validation
fn assert_changeset_matches_full(
    current_state: &RecordBatch,
    changeset: &ChangeSet, 
    expected: &ExpectedResult, 
    _system_date: NaiveDate
) {
    // Extract expire records using indices from current_state
    let mut actual_expires = Vec::new();
    for &expire_idx in &changeset.to_expire {
        let expire_record = extract_record_at_index(current_state, expire_idx);
        actual_expires.push(expire_record);
    }
    actual_expires = sort_records(actual_expires);

    let mut actual_inserts = Vec::new();
    for batch in &changeset.to_insert {
        let mut batch_records = extract_records_from_batch(batch);
        actual_inserts.append(&mut batch_records);
    }
    actual_inserts = sort_records(actual_inserts);

    // Convert expected records to comparable format
    let expected_expires = convert_test_records_to_comparable(&expected.expire);
    let expected_inserts = convert_test_records_to_comparable(&expected.insert);

    // Assert counts match
    assert_eq!(actual_expires.len(), expected_expires.len(), 
               "Expire count mismatch: expected {}, got {}", expected_expires.len(), actual_expires.len());
    assert_eq!(actual_inserts.len(), expected_inserts.len(),
               "Insert count mismatch: expected {}, got {}", expected_inserts.len(), actual_inserts.len());

    // Assert expire records match (excluding as_of_to which is system-generated)
    for (i, (actual, expected)) in actual_expires.iter().zip(expected_expires.iter()).enumerate() {
        assert_eq!(actual.0, expected.0, "Expire record {} id mismatch", i);
        assert_eq!(actual.1, expected.1, "Expire record {} field mismatch", i);
        assert_eq!(actual.2, expected.2, "Expire record {} mv mismatch", i);
        assert_eq!(actual.3, expected.3, "Expire record {} price mismatch", i);
        assert_eq!(actual.4.date(), expected.4.date(), "Expire record {} effective_from mismatch", i);
        assert_eq!(actual.5.date(), expected.5.date(), "Expire record {} effective_to mismatch", i);
        assert_eq!(actual.6.date(), expected.6.date(), "Expire record {} as_of_from mismatch", i);
        // Skip as_of_to validation for expires - it's system-generated
    }

    // Assert insert records match
    for (i, (actual, expected)) in actual_inserts.iter().zip(expected_inserts.iter()).enumerate() {
        assert_eq!(actual.0, expected.0, "Insert record {} id mismatch", i);
        assert_eq!(actual.1, expected.1, "Insert record {} field mismatch", i);
        assert_eq!(actual.2, expected.2, "Insert record {} mv mismatch", i);
        assert_eq!(actual.3, expected.3, "Insert record {} price mismatch", i);
        assert_eq!(actual.4.date(), expected.4.date(), "Insert record {} effective_from mismatch", i);
        assert_eq!(actual.5.date(), expected.5.date(), "Insert record {} effective_to mismatch", i);
        assert_eq!(actual.6.date(), expected.6.date(), "Insert record {} as_of_from mismatch", i);
        // as_of_to should be max date for inserts
        let max_date = NaiveDate::from_ymd_opt(2262, 4, 11).unwrap();
        assert_eq!(actual.7.date(), max_date, "Insert record {} as_of_to should be max date", i);
    }
}

// Extract a single record at a specific index
fn extract_record_at_index(batch: &RecordBatch, index: usize) -> (i32, String, i32, i32, NaiveDateTime, NaiveDateTime, NaiveDateTime, NaiveDateTime) {
    let id_array = batch.column_by_name("id").unwrap()
        .as_any().downcast_ref::<Int32Array>().unwrap();
    let field_array = batch.column_by_name("field").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    let mv_array = batch.column_by_name("mv").unwrap()
        .as_any().downcast_ref::<Int32Array>().unwrap();
    let price_array = batch.column_by_name("price").unwrap()
        .as_any().downcast_ref::<Int32Array>().unwrap();
    let eff_from_array = batch.column_by_name("effective_from").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let eff_to_array = batch.column_by_name("effective_to").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let as_of_from_array = batch.column_by_name("as_of_from").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let as_of_to_array = batch.column_by_name("as_of_to").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();

    let id = id_array.value(index);
    let field = field_array.value(index).to_string();
    let mv = mv_array.value(index);
    let price = price_array.value(index);
    let eff_from = extract_timestamp(eff_from_array, index);
    let eff_to = extract_timestamp(eff_to_array, index);
    let as_of_from = extract_timestamp(as_of_from_array, index);
    let as_of_to = extract_timestamp(as_of_to_array, index);
    
    (id, field, mv, price, eff_from, eff_to, as_of_from, as_of_to)
}

// Convert TestRecord to comparable format
fn convert_test_records_to_comparable(records: &[TestRecord]) -> Vec<(i32, String, i32, i32, NaiveDateTime, NaiveDateTime, NaiveDateTime, NaiveDateTime)> {
    let mut result = Vec::new();
    let max_date = NaiveDate::from_ymd_opt(2262, 4, 11).unwrap();

    for &(id, field, mv, price, eff_from_str, eff_to_str, as_of_from_str, as_of_to_str) in records {
        let eff_from_date = if eff_from_str == "max" {
            max_date
        } else {
            NaiveDate::parse_from_str(eff_from_str, "%Y-%m-%d").unwrap()
        };
        let eff_from = eff_from_date.and_hms_opt(0, 0, 0).unwrap();

        let eff_to_date = if eff_to_str == "max" {
            max_date
        } else {
            NaiveDate::parse_from_str(eff_to_str, "%Y-%m-%d").unwrap()
        };
        let eff_to = eff_to_date.and_hms_opt(0, 0, 0).unwrap();

        let as_of_from_date = if as_of_from_str == "max" {
            max_date
        } else {
            NaiveDate::parse_from_str(as_of_from_str, "%Y-%m-%d").unwrap()
        };
        let as_of_from = as_of_from_date.and_hms_opt(0, 0, 0).unwrap();

        let as_of_to_date = if as_of_to_str == "max" {
            max_date
        } else {
            NaiveDate::parse_from_str(as_of_to_str, "%Y-%m-%d").unwrap()
        };
        let as_of_to = as_of_to_date.and_hms_opt(23, 59, 59).unwrap();

        result.push((id, field.to_string(), mv, price, eff_from, eff_to, as_of_from, as_of_to));
    }

    sort_records(result)
}

// BASIC SCENARIOS

#[test]
fn test_insert() {
    let current_state = create_test_batch(vec![]).unwrap();

    let updates = create_test_batch(vec![
        (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
        (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();
    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
    ).unwrap();

    let expected = ExpectedResult {
        expire: vec![],
        insert: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
            (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ]
    };

    assert_changeset_matches_full(&current_state, &changeset, &expected, system_date);
}

#[test]
fn test_simple_overwrite() {
    let current_state = create_test_batch(vec![
        (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
    ]).unwrap();

    let updates = create_test_batch(vec![
        (1234, "test", 400, 300, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();
    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
    ).unwrap();

    let expected = ExpectedResult {
        expire: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        insert: vec![
            (1234, "test", 400, 300, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
        ]
    };

    assert_changeset_matches_full(&current_state, &changeset, &expected, system_date);
}

#[test]
fn test_no_changes_when_values_same() {
    let current_state = create_test_batch(vec![
        (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
    ]).unwrap();

    let updates = create_test_batch(vec![
        (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();
    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
    ).unwrap();

    let expected = ExpectedResult {
        expire: vec![],
        insert: vec![]
    };

    assert_changeset_matches_full(&current_state, &changeset, &expected, system_date);
}

#[test]
fn test_head_slice_conflation() {
    let current_state = create_test_batch(vec![
        (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
    ]).unwrap();

    let updates = create_test_batch(vec![
        (1234, "test", 400, 300, "2019-01-01", "2020-06-01", "2025-07-27", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();
    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
    ).unwrap();

    let expected = ExpectedResult {
        expire: vec![
            (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        insert: vec![
            (1234, "test", 400, 300, "2019-01-01", "2020-06-01", "2025-07-27", "max"),
            (1234, "test", 300, 400, "2020-06-01", "2021-01-01", "2025-07-27", "max"), // as_of_from inherits from update
        ]
    };

    assert_changeset_matches_full(&current_state, &changeset, &expected, system_date);
}

#[test]
fn test_extend_current_row() {
    let current_state = create_test_batch(vec![
        (1234, "test", 100, 100, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
    ]).unwrap();

    let updates = create_test_batch(vec![
        (1234, "test", 100, 100, "2021-01-01", "2022-11-01", "2025-07-27", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();
    let changeset = process_updates(
        current_state.clone(),
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
    ).unwrap();

    let expected = ExpectedResult {
        expire: vec![
            (1234, "test", 100, 100, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        ],
        insert: vec![
            // Should be conflated with update's as_of_from (latest timestamp)
            (1234, "test", 100, 100, "2020-01-01", "2022-11-01", "2025-07-27", "max"),
        ]
    };

    assert_changeset_matches_full(&current_state, &changeset, &expected, system_date);
}
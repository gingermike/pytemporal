use bitemporal_timeseries::{process_updates, UpdateMode, ChangeSet};
use chrono::{NaiveDate, NaiveDateTime};
use arrow::array::{Date32Array, TimestampMicrosecondArray, Int32Array, Int64Array, StringArray};
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
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

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

fn extract_date(array: &Date32Array, index: usize) -> NaiveDate {
    let days = array.value(index);
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    epoch + chrono::Duration::days(days as i64)
}

fn extract_timestamp(array: &TimestampMicrosecondArray, index: usize) -> NaiveDateTime {
    let microseconds = array.value(index);
    let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
    epoch + chrono::Duration::microseconds(microseconds)
}

#[test]
fn test_head_slice_conflation() {
    // Test the head slice scenario from test_bitemporal_manual.py
    let current_state = create_test_batch(vec![
        (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
    ]).unwrap();

    let updates = create_test_batch(vec![
        (1234, "test", 400, 300, "2019-01-01", "2020-06-01", "2025-07-27", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();
    let changeset = process_updates(
        current_state,
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
    ).unwrap();

    // Should expire 1 row and insert 2 rows (with conflation)
    assert_eq!(changeset.to_expire.len(), 1);
    assert_eq!(changeset.to_insert.len(), 2);

    // Check the effective dates of the inserted records
    let inserts = &changeset.to_insert;
    
    // Sort by effective_from for verification
    let mut insert_dates = Vec::new();
    for batch in inserts {
        let eff_from_array = batch.column_by_name("effective_from").unwrap()
            .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        let eff_to_array = batch.column_by_name("effective_to").unwrap()
            .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        
        let eff_from = extract_timestamp(eff_from_array, 0).date();
        let eff_to = extract_timestamp(eff_to_array, 0).date();
        insert_dates.push((eff_from, eff_to));
    }
    insert_dates.sort_by_key(|&(from, _)| from);

    // Expected conflated segments:
    // 1. 2019-01-01 to 2020-06-01 (conflated from 2019-01-01 to 2020-01-01 + 2020-01-01 to 2020-06-01)
    // 2. 2020-06-01 to 2021-01-01
    assert_eq!(insert_dates[0].0, NaiveDate::from_ymd_opt(2019, 1, 1).unwrap());
    assert_eq!(insert_dates[0].1, NaiveDate::from_ymd_opt(2020, 6, 1).unwrap());
    assert_eq!(insert_dates[1].0, NaiveDate::from_ymd_opt(2020, 6, 1).unwrap());
    assert_eq!(insert_dates[1].1, NaiveDate::from_ymd_opt(2021, 1, 1).unwrap());
}

#[test] 
fn test_tail_slice_conflation() {
    // Test the tail slice scenario from test_bitemporal_manual.py
    let current_state = create_test_batch(vec![
        (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
    ]).unwrap();

    let updates = create_test_batch(vec![
        (1234, "test", 400, 300, "2020-06-01", "2022-01-01", "2025-07-27", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();
    let changeset = process_updates(
        current_state,
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
    ).unwrap();

    // Should expire 1 row and insert 2 rows (with conflation)
    assert_eq!(changeset.to_expire.len(), 1);
    assert_eq!(changeset.to_insert.len(), 2);
}

#[test]
fn test_unsorted_data_with_conflation() {
    // Test unsorted data scenario that should result in conflation
    let current_state = create_test_batch(vec![
        (1, "A", 300, 3000, "2024-08-01", "2024-12-31", "2024-01-01", "max"),
        (1, "A", 100, 1000, "2024-01-01", "2024-04-01", "2024-01-01", "max"),
        (1, "A", 200, 2000, "2024-04-01", "2024-08-01", "2024-01-01", "max"),
    ]).unwrap();

    let updates = create_test_batch(vec![
        (1, "A", 999, 9999, "2024-03-01", "2024-09-01", "2024-07-21", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2024, 7, 21).unwrap();
    let changeset = process_updates(
        current_state,
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
    ).unwrap();

    // Should expire 3 rows and insert 3 rows (with conflation of the middle segments)
    assert_eq!(changeset.to_expire.len(), 3);
    assert_eq!(changeset.to_insert.len(), 3);
}

#[test]
fn test_simple_overwrite() {
    // Test simple overwrite scenario
    let current_state = create_test_batch(vec![
        (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
        (1234, "fielda", 400, 500, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
    ]).unwrap();

    let updates = create_test_batch(vec![
        (1234, "test", 400, 300, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();
    let changeset = process_updates(
        current_state,
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
    ).unwrap();

    // Should expire 1 row and insert 1 row (exact replacement)
    assert_eq!(changeset.to_expire.len(), 1);
    assert_eq!(changeset.to_insert.len(), 1);
}

#[test]
fn test_no_changes_when_values_same() {
    // Test that no changes are generated when update values are the same as current
    let current_state = create_test_batch(vec![
        (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-01-01", "max"),
    ]).unwrap();

    let updates = create_test_batch(vec![
        (1234, "test", 300, 400, "2020-01-01", "2021-01-01", "2025-07-27", "max"),
    ]).unwrap();

    let system_date = NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();
    let changeset = process_updates(
        current_state,
        updates,
        vec!["id".to_string(), "field".to_string()],
        vec!["mv".to_string(), "price".to_string()],
        system_date,
        UpdateMode::Delta,
    ).unwrap();

    // Should not expire or insert anything when values are the same and dates intersect
    assert_eq!(changeset.to_expire.len(), 0);
    assert_eq!(changeset.to_insert.len(), 0);
}
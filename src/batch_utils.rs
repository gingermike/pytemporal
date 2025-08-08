use crate::types::*;
use arrow::array::{ArrayRef, RecordBatch, TimestampMicrosecondArray, Int64Array};
use std::sync::Arc;
use chrono::NaiveDateTime;
use blake3;

// Cache the epoch calculation since it's used frequently
const EPOCH: NaiveDateTime = match chrono::DateTime::from_timestamp(0, 0) {
    Some(dt) => dt.naive_utc(),
    None => panic!("Failed to create epoch datetime"),
};

pub fn extract_timestamp(array: &TimestampMicrosecondArray, idx: usize) -> NaiveDateTime {
    let microseconds_since_epoch = array.value(idx);
    EPOCH + chrono::Duration::microseconds(microseconds_since_epoch)
}

// Keep both function names for backward compatibility
pub fn extract_date_as_datetime(array: &TimestampMicrosecondArray, idx: usize) -> NaiveDateTime {
    extract_timestamp(array, idx)
}

pub fn hash_values(record_batch: &RecordBatch, row_idx: usize, value_columns: &[String]) -> u64 {
    let mut hasher_input = Vec::new();
    
    for col_name in value_columns {
        let col_idx = record_batch.schema().index_of(col_name).unwrap();
        let array = record_batch.column(col_idx);
        
        let scalar = ScalarValue::from_array(array, row_idx);
        match scalar {
            ScalarValue::String(s) => hasher_input.extend_from_slice(s.as_bytes()),
            ScalarValue::Int32(i) => hasher_input.extend_from_slice(&i.to_le_bytes()),
            ScalarValue::Int64(i) => hasher_input.extend_from_slice(&i.to_le_bytes()),
            ScalarValue::Float64(f) => hasher_input.extend_from_slice(&f.0.to_le_bytes()),
            ScalarValue::Date32(d) => hasher_input.extend_from_slice(&d.to_le_bytes()),
        }
    }
    
    let hash = blake3::hash(&hasher_input);
    let hash_bytes = hash.as_bytes();
    u64::from_be_bytes(hash_bytes[..8].try_into().unwrap())
}

// Helper function to create a timestamp array from a NaiveDateTime
fn create_timestamp_array(datetime: NaiveDateTime) -> ArrayRef {
    let mut builder = TimestampMicrosecondArray::builder(1);
    let microseconds = (datetime - EPOCH).num_microseconds().unwrap();
    builder.append_value(microseconds);
    Arc::new(builder.finish())
}

// Helper function to create a value hash array
fn create_value_hash_array(hash: u64) -> ArrayRef {
    let mut builder = Int64Array::builder(1);
    builder.append_value(hash as i64);
    Arc::new(builder.finish())
}

// Common function to build columns with temporal data from a BitemporalRecord
fn build_temporal_columns(
    record: &BitemporalRecord,
    schema: &arrow::datatypes::Schema,
    source_batch: &RecordBatch,
    source_row: usize,
) -> Result<Vec<ArrayRef>, String> {
    let mut columns: Vec<ArrayRef> = Vec::new();
    
    for field in schema.fields() {
        let column_name = field.name();
        
        match column_name.as_str() {
            "effective_from" => {
                columns.push(create_timestamp_array(record.effective_from));
            }
            "effective_to" => {
                columns.push(create_timestamp_array(record.effective_to));
            }
            "as_of_from" => {
                columns.push(create_timestamp_array(record.as_of_from));
            }
            "as_of_to" => {
                columns.push(create_timestamp_array(record.as_of_to));
            }
            "value_hash" => {
                columns.push(create_value_hash_array(record.value_hash));
            }
            _ => {
                // Copy from source batch
                let orig_array = source_batch.column_by_name(column_name).unwrap();
                let new_array = orig_array.slice(source_row, 1);
                columns.push(new_array);
            }
        }
    }
    
    Ok(columns)
}

pub fn create_record_batch_from_record(
    record: &BitemporalRecord, 
    original_batch: &RecordBatch,
    original_row: usize,
    _id_columns: &[String],
    _value_columns: &[String]
) -> Result<RecordBatch, String> {
    let schema = original_batch.schema();
    let columns = build_temporal_columns(record, &schema, original_batch, original_row)?;
    
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| e.to_string())
}

pub fn create_record_batch_from_update(
    updates: &RecordBatch, 
    row_idx: usize, 
    record: &BitemporalRecord
) -> Result<RecordBatch, String> {
    let schema = updates.schema();
    let columns = build_temporal_columns(record, &schema, updates, row_idx)?;
    
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| e.to_string())
}
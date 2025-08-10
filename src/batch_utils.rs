use crate::types::*;
use arrow::array::{Array, ArrayRef, RecordBatch, TimestampMicrosecondArray, Int64Array, StringBuilder};
use arrow::array::{Int32Builder, Float64Builder, Date32Builder};
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

/// Batch-oriented version that creates a single RecordBatch from multiple BitemporalRecords
/// This avoids the overhead of creating many single-row batches
pub fn create_record_batch_from_records(
    records: &[BitemporalRecord],
    source_batch: &RecordBatch,
    source_rows: &[usize],
) -> Result<RecordBatch, String> {
    if records.is_empty() {
        return Err("Cannot create batch from empty records".to_string());
    }
    
    let schema = source_batch.schema();
    let num_records = records.len();
    
    // Pre-allocate builders with exact capacity
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    
    for field in schema.fields() {
        let column_name = field.name();
        
        match column_name.as_str() {
            "effective_from" => {
                let mut builder = TimestampMicrosecondArray::builder(num_records);
                for record in records {
                    let microseconds = (record.effective_from - EPOCH).num_microseconds().unwrap();
                    builder.append_value(microseconds);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "effective_to" => {
                let mut builder = TimestampMicrosecondArray::builder(num_records);
                for record in records {
                    let microseconds = (record.effective_to - EPOCH).num_microseconds().unwrap();
                    builder.append_value(microseconds);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "as_of_from" => {
                let mut builder = TimestampMicrosecondArray::builder(num_records);
                for record in records {
                    let microseconds = (record.as_of_from - EPOCH).num_microseconds().unwrap();
                    builder.append_value(microseconds);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "as_of_to" => {
                let mut builder = TimestampMicrosecondArray::builder(num_records);
                for record in records {
                    let microseconds = (record.as_of_to - EPOCH).num_microseconds().unwrap();
                    builder.append_value(microseconds);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "value_hash" => {
                let mut builder = Int64Array::builder(num_records);
                for record in records {
                    builder.append_value(record.value_hash as i64);
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                // Copy data columns from source batch
                let orig_array = source_batch.column_by_name(column_name).unwrap();
                
                // Handle different column types with pre-allocated builders
                match orig_array.data_type() {
                    arrow::datatypes::DataType::Utf8 => {
                        let string_array = orig_array.as_any()
                            .downcast_ref::<arrow::array::StringArray>().unwrap();
                        let mut builder = StringBuilder::new();
                        for &source_row in source_rows {
                            if string_array.is_null(source_row) {
                                builder.append_null();
                            } else {
                                builder.append_value(string_array.value(source_row));
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Int32 => {
                        let int32_array = orig_array.as_any()
                            .downcast_ref::<arrow::array::Int32Array>().unwrap();
                        let mut builder = Int32Builder::new();
                        for &source_row in source_rows {
                            if int32_array.is_null(source_row) {
                                builder.append_null();
                            } else {
                                builder.append_value(int32_array.value(source_row));
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Int64 => {
                        let int64_array = orig_array.as_any()
                            .downcast_ref::<arrow::array::Int64Array>().unwrap();
                        let mut builder = arrow::array::Int64Builder::new();
                        for &source_row in source_rows {
                            if int64_array.is_null(source_row) {
                                builder.append_null();
                            } else {
                                builder.append_value(int64_array.value(source_row));
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Float64 => {
                        let float64_array = orig_array.as_any()
                            .downcast_ref::<arrow::array::Float64Array>().unwrap();
                        let mut builder = Float64Builder::new();
                        for &source_row in source_rows {
                            if float64_array.is_null(source_row) {
                                builder.append_null();
                            } else {
                                builder.append_value(float64_array.value(source_row));
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Date32 => {
                        let date32_array = orig_array.as_any()
                            .downcast_ref::<arrow::array::Date32Array>().unwrap();
                        let mut builder = Date32Builder::new();
                        for &source_row in source_rows {
                            if date32_array.is_null(source_row) {
                                builder.append_null();
                            } else {
                                builder.append_value(date32_array.value(source_row));
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    _ => {
                        // Fallback to slice method for unsupported types
                        let mut slices = Vec::with_capacity(num_records);
                        for &source_row in source_rows {
                            slices.push(orig_array.slice(source_row, 1));
                        }
                        // Concatenate slices - this is less efficient but handles all types
                        let arrays: Vec<&dyn arrow::array::Array> = slices.iter().map(|a| a.as_ref()).collect();
                        let result = arrow::compute::concat(&arrays)
                            .map_err(|e| format!("Failed to concatenate arrays: {}", e))?;
                        columns.push(result);
                    }
                }
            }
        }
    }
    
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| e.to_string())
}

/// Optimized batch hash computation
pub fn hash_values_batch(
    record_batch: &RecordBatch, 
    row_indices: &[usize], 
    value_columns: &[String]
) -> Vec<u64> {
    let mut hashes = Vec::with_capacity(row_indices.len());
    
    // Pre-compute column indices to avoid repeated lookups
    let col_indices: Vec<usize> = value_columns.iter()
        .map(|col_name| record_batch.schema().index_of(col_name).unwrap())
        .collect();
    
    for &row_idx in row_indices {
        let mut hasher_input = Vec::new();
        
        for &col_idx in &col_indices {
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
        hashes.push(u64::from_be_bytes(hash_bytes[..8].try_into().unwrap()));
    }
    
    hashes
}
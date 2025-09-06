use crate::types::*;
use arrow::array::{Array, ArrayRef, RecordBatch, TimestampMicrosecondArray, StringBuilder, StringArray};
use arrow::array::{Int8Builder, Int16Builder, Int32Builder, Float32Builder, Float64Builder, Date32Builder, Date64Builder, BooleanBuilder, Decimal128Builder};
use arrow::array::{Int8Array, Int16Array, Float32Array, Date32Array, Date64Array, BooleanArray, Decimal128Array};
use arrow::array::{TimestampSecondBuilder, TimestampMillisecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder};
use arrow::array::{TimestampSecondArray, TimestampMillisecondArray, TimestampNanosecondArray};
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;
use chrono::NaiveDateTime;

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


// Helper function to create a timestamp array from a NaiveDateTime with timezone support
fn create_timestamp_array(datetime: NaiveDateTime, timezone: Option<String>) -> ArrayRef {
    let microseconds = (datetime - EPOCH).num_microseconds().unwrap();
    let values = vec![Some(microseconds)];
    let array = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone);
    Arc::new(array)
}

// Helper function to create a value hash array
fn create_value_hash_array(hash: &str) -> ArrayRef {
    let mut builder = StringBuilder::new();
    builder.append_value(hash);
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
                let timezone_str = if let DataType::Timestamp(_, tz) = field.data_type() {
                    tz.as_ref().map(|t| t.to_string())
                } else { None };
                columns.push(create_timestamp_array(record.effective_from, timezone_str));
            }
            "effective_to" => {
                let timezone_str = if let DataType::Timestamp(_, tz) = field.data_type() {
                    tz.as_ref().map(|t| t.to_string())
                } else { None };
                columns.push(create_timestamp_array(record.effective_to, timezone_str));
            }
            "as_of_from" => {
                let timezone_str = if let DataType::Timestamp(_, tz) = field.data_type() {
                    tz.as_ref().map(|t| t.to_string())
                } else { None };
                columns.push(create_timestamp_array(record.as_of_from, timezone_str));
            }
            "as_of_to" => {
                let timezone_str = if let DataType::Timestamp(_, tz) = field.data_type() {
                    tz.as_ref().map(|t| t.to_string())
                } else { None };
                columns.push(create_timestamp_array(record.as_of_to, timezone_str));
            }
            "value_hash" => {
                columns.push(create_value_hash_array(&record.value_hash));
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
                let timezone_str = if let DataType::Timestamp(_, tz) = field.data_type() {
                    tz.as_ref().map(|t| t.to_string())
                } else { None };
                
                let values: Vec<Option<i64>> = records.iter()
                    .map(|record| Some((record.effective_from - EPOCH).num_microseconds().unwrap()))
                    .collect();
                
                let array = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str);
                columns.push(Arc::new(array));
            }
            "effective_to" => {
                let timezone_str = if let DataType::Timestamp(_, tz) = field.data_type() {
                    tz.as_ref().map(|t| t.to_string())
                } else { None };
                
                let values: Vec<Option<i64>> = records.iter()
                    .map(|record| Some((record.effective_to - EPOCH).num_microseconds().unwrap()))
                    .collect();
                
                let array = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str);
                columns.push(Arc::new(array));
            }
            "as_of_from" => {
                let timezone_str = if let DataType::Timestamp(_, tz) = field.data_type() {
                    tz.as_ref().map(|t| t.to_string())
                } else { None };
                
                let values: Vec<Option<i64>> = records.iter()
                    .map(|record| Some((record.as_of_from - EPOCH).num_microseconds().unwrap()))
                    .collect();
                
                let array = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str);
                columns.push(Arc::new(array));
            }
            "as_of_to" => {
                let timezone_str = if let DataType::Timestamp(_, tz) = field.data_type() {
                    tz.as_ref().map(|t| t.to_string())
                } else { None };
                
                let values: Vec<Option<i64>> = records.iter()
                    .map(|record| Some((record.as_of_to - EPOCH).num_microseconds().unwrap()))
                    .collect();
                
                let array = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str);
                columns.push(Arc::new(array));
            }
            "value_hash" => {
                let mut builder = StringBuilder::new();
                for record in records {
                    builder.append_value(&record.value_hash);
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
                    arrow::datatypes::DataType::Int8 => {
                        let int8_array = orig_array.as_any()
                            .downcast_ref::<Int8Array>().unwrap();
                        let mut builder = Int8Builder::new();
                        for &source_row in source_rows {
                            if int8_array.is_null(source_row) {
                                builder.append_null();
                            } else {
                                builder.append_value(int8_array.value(source_row));
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Int16 => {
                        let int16_array = orig_array.as_any()
                            .downcast_ref::<Int16Array>().unwrap();
                        let mut builder = Int16Builder::new();
                        for &source_row in source_rows {
                            if int16_array.is_null(source_row) {
                                builder.append_null();
                            } else {
                                builder.append_value(int16_array.value(source_row));
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Float32 => {
                        let float32_array = orig_array.as_any()
                            .downcast_ref::<Float32Array>().unwrap();
                        let mut builder = Float32Builder::new();
                        for &source_row in source_rows {
                            if float32_array.is_null(source_row) {
                                builder.append_null();
                            } else {
                                builder.append_value(float32_array.value(source_row));
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Date32 => {
                        let date32_array = orig_array.as_any()
                            .downcast_ref::<Date32Array>().unwrap();
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
                    arrow::datatypes::DataType::Date64 => {
                        let date64_array = orig_array.as_any()
                            .downcast_ref::<Date64Array>().unwrap();
                        let mut builder = Date64Builder::new();
                        for &source_row in source_rows {
                            if date64_array.is_null(source_row) {
                                builder.append_null();
                            } else {
                                builder.append_value(date64_array.value(source_row));
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Boolean => {
                        let bool_array = orig_array.as_any()
                            .downcast_ref::<BooleanArray>().unwrap();
                        let mut builder = BooleanBuilder::new();
                        for &source_row in source_rows {
                            if bool_array.is_null(source_row) {
                                builder.append_null();
                            } else {
                                builder.append_value(bool_array.value(source_row));
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Timestamp(unit, timezone) => {
                        use arrow::datatypes::TimeUnit;
                        match unit {
                            TimeUnit::Second => {
                                let ts_array = orig_array.as_any()
                                    .downcast_ref::<TimestampSecondArray>().unwrap();
                                let mut builder = TimestampSecondBuilder::new();
                                for &source_row in source_rows {
                                    if ts_array.is_null(source_row) {
                                        builder.append_null();
                                    } else {
                                        builder.append_value(ts_array.value(source_row));
                                    }
                                }
                                let array = builder.finish().with_timezone_opt(timezone.clone());
                                columns.push(Arc::new(array));
                            }
                            TimeUnit::Millisecond => {
                                let ts_array = orig_array.as_any()
                                    .downcast_ref::<TimestampMillisecondArray>().unwrap();
                                let mut builder = TimestampMillisecondBuilder::new();
                                for &source_row in source_rows {
                                    if ts_array.is_null(source_row) {
                                        builder.append_null();
                                    } else {
                                        builder.append_value(ts_array.value(source_row));
                                    }
                                }
                                let array = builder.finish().with_timezone_opt(timezone.clone());
                                columns.push(Arc::new(array));
                            }
                            TimeUnit::Microsecond => {
                                let ts_array = orig_array.as_any()
                                    .downcast_ref::<TimestampMicrosecondArray>().unwrap();
                                let mut builder = TimestampMicrosecondBuilder::new();
                                for &source_row in source_rows {
                                    if ts_array.is_null(source_row) {
                                        builder.append_null();
                                    } else {
                                        builder.append_value(ts_array.value(source_row));
                                    }
                                }
                                let array = builder.finish().with_timezone_opt(timezone.clone());
                                columns.push(Arc::new(array));
                            }
                            TimeUnit::Nanosecond => {
                                let ts_array = orig_array.as_any()
                                    .downcast_ref::<TimestampNanosecondArray>().unwrap();
                                let mut builder = TimestampNanosecondBuilder::new();
                                for &source_row in source_rows {
                                    if ts_array.is_null(source_row) {
                                        builder.append_null();
                                    } else {
                                        builder.append_value(ts_array.value(source_row));
                                    }
                                }
                                let array = builder.finish().with_timezone_opt(timezone.clone());
                                columns.push(Arc::new(array));
                            }
                        }
                    }
                    arrow::datatypes::DataType::Decimal128(precision, scale) => {
                        let decimal_array = orig_array.as_any()
                            .downcast_ref::<Decimal128Array>().unwrap();
                        let mut builder = Decimal128Builder::new();
                        for &source_row in source_rows {
                            if decimal_array.is_null(source_row) {
                                builder.append_null();
                            } else {
                                builder.append_value(decimal_array.value(source_row));
                            }
                        }
                        let array = builder.finish().with_precision_and_scale(*precision, *scale)
                            .map_err(|e| format!("Failed to create Decimal128 array: {}", e))?;
                        columns.push(Arc::new(array));
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
) -> Vec<String> {
    use sha2::{Sha256, Digest};
    
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
                ScalarValue::Int8(i) => {
                    // Normalize to 64-bit for consistency
                    let i64_val = i as i64;
                    hasher_input.extend_from_slice(&i64_val.to_le_bytes());
                },
                ScalarValue::Int16(i) => {
                    // Normalize to 64-bit for consistency
                    let i64_val = i as i64;
                    hasher_input.extend_from_slice(&i64_val.to_le_bytes());
                },
                ScalarValue::Int32(i) => {
                    // Normalize to 64-bit for consistency
                    let i64_val = i as i64;
                    hasher_input.extend_from_slice(&i64_val.to_le_bytes());
                },
                ScalarValue::Int64(i) => hasher_input.extend_from_slice(&i.to_le_bytes()),
                ScalarValue::Float32(f) => {
                    // Check if this is actually an integer value stored as float
                    if f.0.fract() == 0.0 && f.0.is_finite() && f.0 >= i64::MIN as f32 && f.0 <= i64::MAX as f32 {
                        // This is an integer value - normalize to Int64 for consistency  
                        let i64_val = f.0 as i64;
                        hasher_input.extend_from_slice(&i64_val.to_le_bytes());
                    } else {
                        // This is a true float value - promote to f64 for consistency
                        let f64_val = f.0 as f64;
                        hasher_input.extend_from_slice(&f64_val.to_le_bytes());
                    }
                },
                ScalarValue::Float64(f) => {
                    // Check if this is actually an integer value stored as float
                    if f.0.fract() == 0.0 && f.0.is_finite() && f.0 >= i64::MIN as f64 && f.0 <= i64::MAX as f64 {
                        // This is an integer value - normalize to Int64 for consistency  
                        let i64_val = f.0 as i64;
                        hasher_input.extend_from_slice(&i64_val.to_le_bytes());
                    } else {
                        // This is a true float value
                        hasher_input.extend_from_slice(&f.0.to_le_bytes());
                    }
                },
                ScalarValue::Date32(d) => hasher_input.extend_from_slice(&d.to_le_bytes()),
                ScalarValue::Date64(d) => hasher_input.extend_from_slice(&d.to_le_bytes()),
                ScalarValue::TimestampSecond(t) => hasher_input.extend_from_slice(&t.to_le_bytes()),
                ScalarValue::TimestampMillisecond(t) => hasher_input.extend_from_slice(&t.to_le_bytes()),
                ScalarValue::TimestampMicrosecond(t) => hasher_input.extend_from_slice(&t.to_le_bytes()),
                ScalarValue::TimestampNanosecond(t) => hasher_input.extend_from_slice(&t.to_le_bytes()),
                ScalarValue::Decimal128(d) => hasher_input.extend_from_slice(&d.to_le_bytes()),
                ScalarValue::Boolean(b) => hasher_input.push(if b { 1u8 } else { 0u8 }),
                ScalarValue::Null => hasher_input.extend_from_slice(b"NULL"), // Use consistent NULL representation
            }
        }
        
        let mut hasher = Sha256::new();
        hasher.update(&hasher_input);
        let hash_result = format!("{:x}", hasher.finalize());
        hashes.push(hash_result);
    }
    
    hashes
}

/// Create a RecordBatch of expired records with updated as_of_to timestamp
pub fn create_expired_records_batch(
    current_state: &RecordBatch,
    expire_indices: &[usize],
    expiry_timestamp: chrono::NaiveDateTime,
) -> Result<RecordBatch, String> {
    if expire_indices.is_empty() {
        return Err("Cannot create batch from empty expire indices".to_string());
    }
    
    let schema = current_state.schema();
    let num_records = expire_indices.len();
    
    // Pre-allocate builders with exact capacity
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    
    for field in schema.fields() {
        let column_name = field.name();
        
        if column_name == "as_of_to" {
            // Set as_of_to to the expiry timestamp for all records, matching the field's precision
            match field.data_type() {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, tz) => {
                    let timezone_str = tz.as_ref().map(|t| t.to_string());
                    let microseconds = (expiry_timestamp - EPOCH).num_microseconds().unwrap();
                    let values: Vec<Option<i64>> = expire_indices.iter()
                        .map(|_| Some(microseconds))
                        .collect();
                    let array = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str);
                    columns.push(Arc::new(array));
                }
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, tz) => {
                    let timezone_str = tz.as_ref().map(|t| t.to_string());
                    let nanoseconds = (expiry_timestamp - EPOCH).num_nanoseconds().unwrap();
                    let values: Vec<Option<i64>> = expire_indices.iter()
                        .map(|_| Some(nanoseconds))
                        .collect();
                    let array = TimestampNanosecondArray::from(values).with_timezone_opt(timezone_str);
                    columns.push(Arc::new(array));
                }
                _ => return Err(format!("Unexpected data type for as_of_to: {:?}", field.data_type()))
            }
        } else {
            // Copy data from original records at the specified indices
            let orig_array = current_state.column_by_name(column_name).unwrap();
            
            // Handle different column types with pre-allocated builders
            match orig_array.data_type() {
                arrow::datatypes::DataType::Utf8 => {
                    let string_array = orig_array.as_any()
                        .downcast_ref::<arrow::array::StringArray>().unwrap();
                    let mut builder = StringBuilder::new();
                    for &idx in expire_indices {
                        if string_array.is_null(idx) {
                            builder.append_null();
                        } else {
                            builder.append_value(string_array.value(idx));
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                arrow::datatypes::DataType::Int32 => {
                    let int32_array = orig_array.as_any()
                        .downcast_ref::<arrow::array::Int32Array>().unwrap();
                    let mut builder = Int32Builder::new();
                    for &idx in expire_indices {
                        if int32_array.is_null(idx) {
                            builder.append_null();
                        } else {
                            builder.append_value(int32_array.value(idx));
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                arrow::datatypes::DataType::Int64 => {
                    let int64_array = orig_array.as_any()
                        .downcast_ref::<arrow::array::Int64Array>().unwrap();
                    let mut builder = arrow::array::Int64Builder::new();
                    for &idx in expire_indices {
                        if int64_array.is_null(idx) {
                            builder.append_null();
                        } else {
                            builder.append_value(int64_array.value(idx));
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                arrow::datatypes::DataType::Float64 => {
                    let float64_array = orig_array.as_any()
                        .downcast_ref::<arrow::array::Float64Array>().unwrap();
                    let mut builder = Float64Builder::new();
                    for &idx in expire_indices {
                        if float64_array.is_null(idx) {
                            builder.append_null();
                        } else {
                            builder.append_value(float64_array.value(idx));
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                arrow::datatypes::DataType::Date32 => {
                    let date32_array = orig_array.as_any()
                        .downcast_ref::<arrow::array::Date32Array>().unwrap();
                    let mut builder = Date32Builder::new();
                    for &idx in expire_indices {
                        if date32_array.is_null(idx) {
                            builder.append_null();
                        } else {
                            builder.append_value(date32_array.value(idx));
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                arrow::datatypes::DataType::Timestamp(time_unit, timezone) => {
                    match time_unit {
                        arrow::datatypes::TimeUnit::Microsecond => {
                            let timestamp_array = orig_array.as_any()
                                .downcast_ref::<TimestampMicrosecondArray>().unwrap();
                            
                            // Extract values and nulls
                            let mut values = Vec::with_capacity(num_records);
                            for &idx in expire_indices {
                                if timestamp_array.is_null(idx) {
                                    values.push(None);
                                } else {
                                    values.push(Some(timestamp_array.value(idx)));
                                }
                            }
                            
                            // Create array with proper timezone information  
                            let timezone_str = timezone.as_ref().map(|tz| tz.to_string());
                            let array = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str);
                            
                            columns.push(Arc::new(array));
                        }
                        arrow::datatypes::TimeUnit::Nanosecond => {
                            let timestamp_array = orig_array.as_any()
                                .downcast_ref::<arrow::array::TimestampNanosecondArray>().unwrap();
                            
                            // Extract values and nulls
                            let mut values = Vec::with_capacity(num_records);
                            for &idx in expire_indices {
                                if timestamp_array.is_null(idx) {
                                    values.push(None);
                                } else {
                                    values.push(Some(timestamp_array.value(idx)));
                                }
                            }
                            
                            // Create array with proper timezone information  
                            let timezone_str = timezone.as_ref().map(|tz| tz.to_string());
                            let array = arrow::array::TimestampNanosecondArray::from(values).with_timezone_opt(timezone_str);
                            
                            columns.push(Arc::new(array));
                        }
                        arrow::datatypes::TimeUnit::Millisecond => {
                            let timestamp_array = orig_array.as_any()
                                .downcast_ref::<arrow::array::TimestampMillisecondArray>().unwrap();
                            
                            // Extract values and nulls
                            let mut values = Vec::with_capacity(num_records);
                            for &idx in expire_indices {
                                if timestamp_array.is_null(idx) {
                                    values.push(None);
                                } else {
                                    values.push(Some(timestamp_array.value(idx)));
                                }
                            }
                            
                            // Create array with proper timezone information  
                            let timezone_str = timezone.as_ref().map(|tz| tz.to_string());
                            let array = arrow::array::TimestampMillisecondArray::from(values).with_timezone_opt(timezone_str);
                            
                            columns.push(Arc::new(array));
                        }
                        arrow::datatypes::TimeUnit::Second => {
                            let timestamp_array = orig_array.as_any()
                                .downcast_ref::<arrow::array::TimestampSecondArray>().unwrap();
                            
                            // Extract values and nulls
                            let mut values = Vec::with_capacity(num_records);
                            for &idx in expire_indices {
                                if timestamp_array.is_null(idx) {
                                    values.push(None);
                                } else {
                                    values.push(Some(timestamp_array.value(idx)));
                                }
                            }
                            
                            // Create array with proper timezone information  
                            let timezone_str = timezone.as_ref().map(|tz| tz.to_string());
                            let array = arrow::array::TimestampSecondArray::from(values).with_timezone_opt(timezone_str);
                            
                            columns.push(Arc::new(array));
                        }
                    }
                }
                _ => {
                    // Fallback to slice method for unsupported types
                    let mut slices = Vec::with_capacity(num_records);
                    for &idx in expire_indices {
                        slices.push(orig_array.slice(idx, 1));
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
    
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| format!("Failed to create expired records batch: {}", e))
}

/// Add a hash column to a RecordBatch using the same hash function as the internal algorithm
/// This ensures complete consistency with the bitemporal processing logic
pub fn add_hash_column(
    record_batch: &RecordBatch,
    value_columns: &[String]
) -> Result<RecordBatch, String> {
    let num_rows = record_batch.num_rows();
    if num_rows == 0 {
        return Err("Cannot add hash column to empty RecordBatch".to_string());
    }
    
    // Validate that all value columns exist
    for col_name in value_columns {
        if record_batch.schema().index_of(col_name).is_err() {
            return Err(format!("Column '{}' not found in RecordBatch", col_name));
        }
    }
    
    // Use the existing hash_values_batch function to compute hashes for all rows
    let row_indices: Vec<usize> = (0..num_rows).collect();
    let hash_values_string = hash_values_batch(record_batch, &row_indices, value_columns);
    
    // Create the hash column
    let hash_array = Arc::new(StringArray::from(hash_values_string));
    
    // Check if value_hash column already exists
    let hash_column_index = record_batch.schema().index_of("value_hash");
    
    let (new_schema, new_columns) = if let Ok(hash_idx) = hash_column_index {
        // Replace existing value_hash column
        let new_fields: Vec<Arc<Field>> = record_batch.schema().fields().iter().cloned().collect();
        let new_schema = Arc::new(Schema::new(new_fields));
        
        let mut new_columns: Vec<ArrayRef> = record_batch.columns().to_vec();
        new_columns[hash_idx] = hash_array; // Replace existing column
        
        (new_schema, new_columns)
    } else {
        // Add new value_hash column
        let mut new_fields: Vec<Arc<Field>> = record_batch.schema().fields().iter().cloned().collect();
        new_fields.push(Arc::new(Field::new("value_hash", DataType::Utf8, false)));
        let new_schema = Arc::new(Schema::new(new_fields));
        
        let mut new_columns: Vec<ArrayRef> = record_batch.columns().to_vec();
        new_columns.push(hash_array);
        
        (new_schema, new_columns)
    };
    
    // Create the new RecordBatch
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| format!("Failed to create RecordBatch with hash column: {}", e))
}
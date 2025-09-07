use crate::types::*;
use arrow::array::{Array, ArrayRef, RecordBatch, TimestampMicrosecondArray, StringBuilder};
use arrow::array::{Int8Builder, Int16Builder, Int32Builder, Float32Builder, Float64Builder, Date32Builder, Date64Builder, BooleanBuilder, Decimal128Builder};
use arrow::array::{Int8Array, Int16Array, Float32Array, Date32Array, Date64Array, BooleanArray, Decimal128Array};
use arrow::array::{TimestampSecondBuilder, TimestampMillisecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder};
use arrow::array::{TimestampSecondArray, TimestampMillisecondArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType};
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


// Removed deprecated create_timestamp_array function - use create_timestamp_array_with_unit instead

// Helper function to create a timestamp array with the correct time unit
fn create_timestamp_array_with_unit(
    datetime: NaiveDateTime, 
    unit: &arrow::datatypes::TimeUnit,
    timezone: Option<String>
) -> ArrayRef {
    use arrow::datatypes::TimeUnit;
    
    match unit {
        TimeUnit::Second => {
            let seconds = (datetime - EPOCH).num_seconds();
            let values = vec![Some(seconds)];
            let array = TimestampSecondArray::from(values).with_timezone_opt(timezone);
            Arc::new(array)
        }
        TimeUnit::Millisecond => {
            let millis = (datetime - EPOCH).num_milliseconds();
            let values = vec![Some(millis)];
            let array = TimestampMillisecondArray::from(values).with_timezone_opt(timezone);
            Arc::new(array)
        }
        TimeUnit::Microsecond => {
            let micros = (datetime - EPOCH).num_microseconds().unwrap();
            let values = vec![Some(micros)];
            let array = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone);
            Arc::new(array)
        }
        TimeUnit::Nanosecond => {
            let nanos = (datetime - EPOCH).num_nanoseconds()
                .ok_or_else(|| "Timestamp overflow in nanosecond conversion")
                .unwrap_or(i64::MAX); // Fallback to max value on overflow
            let values = vec![Some(nanos)];
            let array = TimestampNanosecondArray::from(values).with_timezone_opt(timezone);
            Arc::new(array)
        }
    }
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
                match field.data_type() {
                    DataType::Timestamp(unit, tz) => {
                        let timezone_str = tz.as_ref().map(|t| t.to_string());
                        columns.push(create_timestamp_array_with_unit(record.effective_from, unit, timezone_str));
                    }
                    DataType::Date32 => {
                        let days = (record.effective_from.date() - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                        let array = Date32Array::from(vec![Some(days)]);
                        columns.push(Arc::new(array));
                    }
                    DataType::Date64 => {
                        let millis = (record.effective_from - EPOCH).num_milliseconds();
                        let array = Date64Array::from(vec![Some(millis)]);
                        columns.push(Arc::new(array));
                    }
                    _ => return Err(format!("Unsupported data type for effective_from: {:?}", field.data_type()))
                }
            }
            "effective_to" => {
                match field.data_type() {
                    DataType::Timestamp(unit, tz) => {
                        let timezone_str = tz.as_ref().map(|t| t.to_string());
                        columns.push(create_timestamp_array_with_unit(record.effective_to, unit, timezone_str));
                    }
                    DataType::Date32 => {
                        let days = (record.effective_to.date() - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                        let array = Date32Array::from(vec![Some(days)]);
                        columns.push(Arc::new(array));
                    }
                    DataType::Date64 => {
                        let millis = (record.effective_to - EPOCH).num_milliseconds();
                        let array = Date64Array::from(vec![Some(millis)]);
                        columns.push(Arc::new(array));
                    }
                    _ => return Err(format!("Unsupported data type for effective_to: {:?}", field.data_type()))
                }
            }
            "as_of_from" => {
                match field.data_type() {
                    DataType::Timestamp(unit, tz) => {
                        let timezone_str = tz.as_ref().map(|t| t.to_string());
                        columns.push(create_timestamp_array_with_unit(record.as_of_from, unit, timezone_str));
                    }
                    DataType::Date32 => {
                        let days = (record.as_of_from.date() - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                        let array = Date32Array::from(vec![Some(days)]);
                        columns.push(Arc::new(array));
                    }
                    DataType::Date64 => {
                        let millis = (record.as_of_from - EPOCH).num_milliseconds();
                        let array = Date64Array::from(vec![Some(millis)]);
                        columns.push(Arc::new(array));
                    }
                    _ => return Err(format!("Unsupported data type for as_of_from: {:?}", field.data_type()))
                }
            }
            "as_of_to" => {
                match field.data_type() {
                    DataType::Timestamp(unit, tz) => {
                        let timezone_str = tz.as_ref().map(|t| t.to_string());
                        columns.push(create_timestamp_array_with_unit(record.as_of_to, unit, timezone_str));
                    }
                    DataType::Date32 => {
                        let days = (record.as_of_to.date() - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                        let array = Date32Array::from(vec![Some(days)]);
                        columns.push(Arc::new(array));
                    }
                    DataType::Date64 => {
                        let millis = (record.as_of_to - EPOCH).num_milliseconds();
                        let array = Date64Array::from(vec![Some(millis)]);
                        columns.push(Arc::new(array));
                    }
                    _ => return Err(format!("Unsupported data type for as_of_to: {:?}", field.data_type()))
                }
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

/// Helper function to create timestamp arrays for multiple records
fn create_timestamp_array_for_records(
    records: &[BitemporalRecord],
    extract_fn: impl Fn(&BitemporalRecord) -> NaiveDateTime,
    data_type: &DataType,
) -> Result<ArrayRef, String> {
    match data_type {
        DataType::Timestamp(unit, tz) => {
            let timezone_str = tz.as_ref().map(|t| t.to_string());
            
            match unit {
                arrow::datatypes::TimeUnit::Second => {
                    let values: Vec<Option<i64>> = records.iter()
                        .map(|r| Some((extract_fn(r) - EPOCH).num_seconds()))
                        .collect();
                    let array = TimestampSecondArray::from(values).with_timezone_opt(timezone_str);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    let values: Vec<Option<i64>> = records.iter()
                        .map(|r| Some((extract_fn(r) - EPOCH).num_milliseconds()))
                        .collect();
                    let array = TimestampMillisecondArray::from(values).with_timezone_opt(timezone_str);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    let values: Vec<Option<i64>> = records.iter()
                        .map(|r| Some((extract_fn(r) - EPOCH).num_microseconds().unwrap()))
                        .collect();
                    let array = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str);
                    Ok(Arc::new(array))
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    let values: Vec<Option<i64>> = records.iter()
                        .map(|r| (extract_fn(r) - EPOCH).num_nanoseconds())
                        .collect();
                    let array = TimestampNanosecondArray::from(values).with_timezone_opt(timezone_str);
                    Ok(Arc::new(array))
                }
            }
        }
        DataType::Date32 => {
            let values: Vec<Option<i32>> = records.iter()
                .map(|r| {
                    let date = extract_fn(r).date();
                    Some((date - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
                })
                .collect();
            Ok(Arc::new(Date32Array::from(values)))
        }
        DataType::Date64 => {
            let values: Vec<Option<i64>> = records.iter()
                .map(|r| Some((extract_fn(r) - EPOCH).num_milliseconds()))
                .collect();
            Ok(Arc::new(Date64Array::from(values)))
        }
        _ => Err(format!("Unsupported date/timestamp type: {:?}", data_type))
    }
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
                columns.push(create_timestamp_array_for_records(
                    records,
                    |r| r.effective_from,
                    field.data_type()
                )?);
            }
            "effective_to" => {
                columns.push(create_timestamp_array_for_records(
                    records,
                    |r| r.effective_to,
                    field.data_type()
                )?);
            }
            "as_of_from" => {
                columns.push(create_timestamp_array_for_records(
                    records,
                    |r| r.as_of_from,
                    field.data_type()
                )?);
            }
            "as_of_to" => {
                columns.push(create_timestamp_array_for_records(
                    records,
                    |r| r.as_of_to,
                    field.data_type()
                )?);
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

// Old ScalarValue-based implementations removed - now using fast Arrow-direct hashing

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
                DataType::Timestamp(unit, tz) => {
                    let timezone_str = tz.as_ref().map(|t| t.to_string());
                    
                    match unit {
                        arrow::datatypes::TimeUnit::Second => {
                            let seconds = (expiry_timestamp - EPOCH).num_seconds();
                            let values: Vec<Option<i64>> = expire_indices.iter()
                                .map(|_| Some(seconds))
                                .collect();
                            let array = TimestampSecondArray::from(values).with_timezone_opt(timezone_str);
                            columns.push(Arc::new(array));
                        }
                        arrow::datatypes::TimeUnit::Millisecond => {
                            let millis = (expiry_timestamp - EPOCH).num_milliseconds();
                            let values: Vec<Option<i64>> = expire_indices.iter()
                                .map(|_| Some(millis))
                                .collect();
                            let array = TimestampMillisecondArray::from(values).with_timezone_opt(timezone_str);
                            columns.push(Arc::new(array));
                        }
                        arrow::datatypes::TimeUnit::Microsecond => {
                            let microseconds = (expiry_timestamp - EPOCH).num_microseconds().unwrap();
                            let values: Vec<Option<i64>> = expire_indices.iter()
                                .map(|_| Some(microseconds))
                                .collect();
                            let array = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str);
                            columns.push(Arc::new(array));
                        }
                        arrow::datatypes::TimeUnit::Nanosecond => {
                            let nanoseconds = (expiry_timestamp - EPOCH).num_nanoseconds()
                                .unwrap_or(i64::MAX); // Handle overflow
                            let values: Vec<Option<i64>> = expire_indices.iter()
                                .map(|_| Some(nanoseconds))
                                .collect();
                            let array = TimestampNanosecondArray::from(values).with_timezone_opt(timezone_str);
                            columns.push(Arc::new(array));
                        }
                    }
                }
                DataType::Date32 => {
                    let days = (expiry_timestamp.date() - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                    let values: Vec<Option<i32>> = expire_indices.iter()
                        .map(|_| Some(days))
                        .collect();
                    columns.push(Arc::new(Date32Array::from(values)));
                }
                DataType::Date64 => {
                    let millis = (expiry_timestamp - EPOCH).num_milliseconds();
                    let values: Vec<Option<i64>> = expire_indices.iter()
                        .map(|_| Some(millis))
                        .collect();
                    columns.push(Arc::new(Date64Array::from(values)));
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

// Old add_hash_column implementations removed - now using fast Arrow-direct hashing from arrow_hash.rs
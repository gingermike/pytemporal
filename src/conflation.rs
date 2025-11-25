use crate::types::*;
use crate::batch_utils::extract_date_as_datetime;
use arrow::array::{RecordBatch, TimestampMicrosecondArray, TimestampNanosecondArray, StringArray, ArrayRef, Array};
use arrow::datatypes::{DataType, Schema, Field};
use std::sync::Arc;
use std::collections::HashMap;
use chrono::NaiveDateTime;

/// Extract timestamp from any timestamp array type
fn extract_timestamp_as_datetime(array: &dyn arrow::array::Array, idx: usize) -> Result<NaiveDateTime, String> {
    if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        Ok(extract_date_as_datetime(arr, idx))
    } else if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
        let value = arr.value(idx);
        let seconds = value / 1_000_000_000;
        let nanos = (value % 1_000_000_000) as u32;
        Ok(chrono::DateTime::from_timestamp(seconds, nanos)
            .ok_or_else(|| "Failed to convert nanosecond timestamp".to_string())?
            .naive_utc())
    } else {
        Err("Unsupported timestamp array type".to_string())
    }
}

/// Check if two schemas are compatible for concatenation (ignoring metadata)
fn schemas_compatible(schema1: &Schema, schema2: &Schema) -> bool {
    if schema1.fields().len() != schema2.fields().len() {
        return false;
    }
    
    for (field1, field2) in schema1.fields().iter().zip(schema2.fields().iter()) {
        if field1.name() != field2.name() 
            || field1.data_type() != field2.data_type() 
            || field1.is_nullable() != field2.is_nullable() {
            return false;
        }
    }
    
    true
}

/// Create a clean schema without metadata for consolidation
fn create_clean_schema(original_schema: &Schema) -> Schema {
    let clean_fields: Vec<Field> = original_schema.fields()
        .iter()
        .map(|field| {
            Field::new(
                field.name(),
                field.data_type().clone(),
                field.is_nullable()
            )
        })
        .collect();
    
    Schema::new(clean_fields)
}


pub fn simple_conflate_batches(mut batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, String> {
    if batches.len() <= 1 {
        return Ok(batches);
    }

    // Sort batches by effective_from for processing
    batches.sort_by(|a, b| {
        let a_eff_from = extract_timestamp_as_datetime(
            a.column_by_name("effective_from").unwrap(), 0
        ).unwrap();
        let b_eff_from = extract_timestamp_as_datetime(
            b.column_by_name("effective_from").unwrap(), 0
        ).unwrap();
        a_eff_from.cmp(&b_eff_from)
    });

    let mut result = Vec::new();
    let mut batches_iter = batches.into_iter();
    let mut current_batch = batches_iter.next().unwrap();

    for next_batch in batches_iter {
        // Check if we can merge current_batch with next_batch
        if can_merge_batches(&current_batch, &next_batch)? {
            // Merge by extending current_batch's effective_to
            let next_eff_to = extract_timestamp_as_datetime(
                next_batch.column_by_name("effective_to").unwrap(), 0
            )?;
            current_batch = extend_batch_to_date(current_batch, next_eff_to)?;
        } else {
            // Can't merge, add current to result and make next the new current
            result.push(current_batch);
            current_batch = next_batch;
        }
    }
    
    // Add the final batch
    result.push(current_batch);
    
    Ok(result)
}

fn can_merge_batches(batch1: &RecordBatch, batch2: &RecordBatch) -> Result<bool, String> {
    if batch1.num_rows() != 1 || batch2.num_rows() != 1 {
        return Ok(false);
    }

    // Check if they have the same ID values and value hash
    let schema = batch1.schema();
    for field in schema.fields() {
        let field_name = field.name();
        if !matches!(field_name.as_str(), "effective_from" | "effective_to" | "as_of_from" | "as_of_to") {
            let array1 = batch1.column_by_name(field_name).unwrap();
            let array2 = batch2.column_by_name(field_name).unwrap();
            
            let value1 = ScalarValue::from_array(array1, 0);
            let value2 = ScalarValue::from_array(array2, 0);
            
            if value1 != value2 {
                return Ok(false);
            }
        }
    }

    // Check if they are adjacent
    let batch1_eff_to = extract_timestamp_as_datetime(
        batch1.column_by_name("effective_to").unwrap(), 0
    )?;
    let batch2_eff_from = extract_timestamp_as_datetime(
        batch2.column_by_name("effective_from").unwrap(), 0
    )?;

    Ok(batch1_eff_to == batch2_eff_from)
}

fn extend_batch_to_date(batch: RecordBatch, new_effective_to: NaiveDateTime) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::new();
    
    for field in schema.fields() {
        let column_name = field.name();
        
        if column_name == "effective_to" {
            let timezone_str = if let DataType::Timestamp(_, tz) = field.data_type() {
                tz.as_ref().map(|t| t.to_string())
            } else { None };
            
            let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
            let microseconds = (new_effective_to - epoch).num_microseconds().unwrap();
            let values = vec![Some(microseconds)];
            let array = TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str);
            columns.push(Arc::new(array));
        } else {
            // Copy original column
            columns.push(batch.column_by_name(column_name).unwrap().clone());
        }
    }
    
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| e.to_string())
}

pub fn deduplicate_record_batches(batches: Vec<RecordBatch>, id_columns: &[String]) -> Result<Vec<RecordBatch>, String> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }

    // Convert RecordBatches to a more workable format for deduplication
    // KEY FIX: Include ID columns in the deduplication key to prevent incorrectly
    // deduplicating records with same temporal bounds/hash but different IDs
    let mut records: Vec<(String, NaiveDateTime, NaiveDateTime, String, RecordBatch)> = Vec::new();

    for batch in batches {
        if batch.num_rows() == 1 {
            // Extract ID key from the batch
            let id_key = extract_id_key(&batch, 0, id_columns)?;

            // Extract timestamps handling both microsecond and nanosecond precision
            let eff_from = extract_timestamp_as_datetime(batch.column_by_name("effective_from").unwrap(), 0)?;
            let eff_to = extract_timestamp_as_datetime(batch.column_by_name("effective_to").unwrap(), 0)?;

            let hash_array = batch.column_by_name("value_hash").unwrap()
                .as_any().downcast_ref::<StringArray>().unwrap();

            let hash = hash_array.value(0).to_string();

            records.push((id_key, eff_from, eff_to, hash, batch));
        }
    }

    // Sort by id_key, then effective_from, then effective_to, then hash
    records.sort_by(|a, b| {
        match a.0.cmp(&b.0) {
            std::cmp::Ordering::Equal => {
                match a.1.cmp(&b.1) {
                    std::cmp::Ordering::Equal => {
                        match a.2.cmp(&b.2) {
                            std::cmp::Ordering::Equal => a.3.cmp(&b.3),
                            other => other,
                        }
                    }
                    other => other,
                }
            }
            other => other,
        }
    });

    // Remove exact duplicates (same ID + temporal bounds + hash)
    let mut deduped: Vec<RecordBatch> = Vec::new();
    let mut last_key: Option<(String, NaiveDateTime, NaiveDateTime, String)> = None;

    for (id_key, eff_from, eff_to, hash, batch) in records {
        let current_key = (id_key, eff_from, eff_to, hash);
        if last_key != Some(current_key.clone()) {
            deduped.push(batch);
            last_key = Some(current_key);
        }
    }

    Ok(deduped)
}

/// Extract ID column values as a string key for deduplication
fn extract_id_key(batch: &RecordBatch, row_idx: usize, id_columns: &[String]) -> Result<String, String> {
    let mut key_parts: Vec<String> = Vec::new();

    for col_name in id_columns {
        let column = batch.column_by_name(col_name)
            .ok_or_else(|| format!("Missing ID column: {}", col_name))?;

        let value_str = extract_column_value(column.as_ref(), row_idx)?;
        key_parts.push(value_str);
    }

    Ok(key_parts.join("|"))
}

/// Extract a single column value as a string
fn extract_column_value(column: &dyn arrow::array::Array, idx: usize) -> Result<String, String> {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if column.is_null(idx) {
        return Ok("NULL".to_string());
    }

    match column.data_type() {
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>()
                .ok_or("Failed to downcast to Int32Array")?;
            Ok(arr.value(idx).to_string())
        }
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>()
                .ok_or("Failed to downcast to Int64Array")?;
            Ok(arr.value(idx).to_string())
        }
        DataType::Utf8 => {
            let arr = column.as_any().downcast_ref::<StringArray>()
                .ok_or("Failed to downcast to StringArray")?;
            Ok(arr.value(idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = column.as_any().downcast_ref::<LargeStringArray>()
                .ok_or("Failed to downcast to LargeStringArray")?;
            Ok(arr.value(idx).to_string())
        }
        _ => {
            // For other types, use debug format (uncommon for ID columns)
            Ok(format!("{:?}@{}", column.data_type(), idx))
        }
    }
}

/// Conflate consecutive input update records with same ID and value hash
/// This merges rows that have:
/// - Same ID column values
/// - Same value_hash
/// - Consecutive effective dates (row[i].effective_to == row[i+1].effective_from)
pub fn conflate_input_updates(updates: RecordBatch, id_columns: &[String]) -> Result<RecordBatch, String> {
    // Handle edge cases
    if updates.num_rows() <= 1 {
        return Ok(updates);
    }

    // Extract necessary columns
    let effective_from_col = updates.column_by_name("effective_from")
        .ok_or_else(|| "Missing effective_from column".to_string())?;
    let effective_to_col = updates.column_by_name("effective_to")
        .ok_or_else(|| "Missing effective_to column".to_string())?;
    let value_hash_col = updates.column_by_name("value_hash")
        .ok_or_else(|| "Missing value_hash column".to_string())?
        .as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| "value_hash must be StringArray".to_string())?;

    // Extract ID columns
    let mut id_arrays: Vec<ArrayRef> = Vec::new();
    for id_col in id_columns {
        let array = updates.column_by_name(id_col)
            .ok_or_else(|| format!("Missing ID column: {}", id_col))?;
        id_arrays.push(array.clone());
    }

    // Build row information: (row_idx, id_key, effective_from, effective_to, value_hash)
    #[derive(Clone)]
    struct RowInfo {
        row_idx: usize,
        id_key: String,
        effective_from: NaiveDateTime,
        effective_to: NaiveDateTime,
        value_hash: String,
    }

    let mut rows: Vec<RowInfo> = Vec::new();
    let mut buffer = String::with_capacity(64);

    for row_idx in 0..updates.num_rows() {
        // Create ID key
        buffer.clear();
        for (i, array) in id_arrays.iter().enumerate() {
            if i > 0 {
                buffer.push('|');
            }
            match array.data_type() {
                DataType::Utf8 => {
                    let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                    if string_array.is_null(row_idx) {
                        buffer.push_str("NULL");
                    } else {
                        buffer.push_str(string_array.value(row_idx));
                    }
                }
                DataType::Int32 => {
                    let int_array = array.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
                    if int_array.is_null(row_idx) {
                        buffer.push_str("NULL");
                    } else {
                        buffer.push_str(&int_array.value(row_idx).to_string());
                    }
                }
                DataType::Int64 => {
                    let int_array = array.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
                    if int_array.is_null(row_idx) {
                        buffer.push_str("NULL");
                    } else {
                        buffer.push_str(&int_array.value(row_idx).to_string());
                    }
                }
                _ => {
                    // Fallback to ScalarValue for other types
                    let scalar = ScalarValue::from_array(array, row_idx);
                    buffer.push_str(&format!("{:?}", scalar));
                }
            }
        }
        let id_key = buffer.clone();

        // Extract timestamps
        let effective_from = extract_timestamp_as_datetime(effective_from_col, row_idx)?;
        let effective_to = extract_timestamp_as_datetime(effective_to_col, row_idx)?;
        let value_hash = value_hash_col.value(row_idx).to_string();

        rows.push(RowInfo {
            row_idx,
            id_key,
            effective_from,
            effective_to,
            value_hash,
        });
    }

    // Group by ID key
    let mut id_groups: HashMap<String, Vec<RowInfo>> = HashMap::new();
    for row in rows {
        id_groups.entry(row.id_key.clone()).or_insert_with(Vec::new).push(row);
    }

    // Process each ID group: sort and identify rows to keep
    let mut rows_to_keep: Vec<usize> = Vec::new();
    let mut rows_to_extend: HashMap<usize, NaiveDateTime> = HashMap::new(); // row_idx -> new effective_to

    for (_id_key, mut group) in id_groups {
        // Sort by effective_from
        group.sort_by(|a, b| a.effective_from.cmp(&b.effective_from));

        let mut i = 0;
        while i < group.len() {
            let mut segment_end = i;

            // Find consecutive rows with same value_hash
            while segment_end + 1 < group.len() {
                let current = &group[segment_end];
                let next = &group[segment_end + 1];

                // Check if consecutive (same value_hash and adjacent dates)
                if current.value_hash == next.value_hash && current.effective_to == next.effective_from {
                    segment_end += 1;
                } else {
                    break;
                }
            }

            // Keep the first row of the segment
            let first_row_idx = group[i].row_idx;
            rows_to_keep.push(first_row_idx);

            // If we merged multiple rows, extend the effective_to
            if segment_end > i {
                let last_effective_to = group[segment_end].effective_to;
                rows_to_extend.insert(first_row_idx, last_effective_to);
            }

            i = segment_end + 1;
        }
    }

    // Sort rows to keep by original index to maintain order
    rows_to_keep.sort_unstable();

    // Build new RecordBatch with selected rows and extended effective_to where needed
    let schema = updates.schema();
    let mut new_columns: Vec<ArrayRef> = Vec::new();

    for field in schema.fields() {
        let col_name = field.name();
        let original_col = updates.column_by_name(col_name).unwrap();

        if col_name == "effective_to" {
            // Build effective_to column with extensions
            let mut values: Vec<Option<i64>> = Vec::new();
            let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();

            for &row_idx in &rows_to_keep {
                let effective_to = if let Some(new_to) = rows_to_extend.get(&row_idx) {
                    *new_to
                } else {
                    extract_timestamp_as_datetime(effective_to_col, row_idx)?
                };
                let microseconds = (effective_to - epoch).num_microseconds().unwrap();
                values.push(Some(microseconds));
            }

            // Match the original field's data type and timezone
            let array: ArrayRef = match field.data_type() {
                DataType::Timestamp(unit, tz) => {
                    let timezone_str = tz.as_ref().map(|t| t.to_string());
                    use arrow::datatypes::TimeUnit;

                    match unit {
                        TimeUnit::Microsecond => {
                            Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str))
                        }
                        TimeUnit::Nanosecond => {
                            // Convert microseconds to nanoseconds
                            let nanos: Vec<Option<i64>> = values.iter()
                                .map(|&v| v.map(|us| us * 1000))
                                .collect();
                            Arc::new(TimestampNanosecondArray::from(nanos).with_timezone_opt(timezone_str))
                        }
                        _ => {
                            return Err(format!("Unsupported timestamp unit for effective_to: {:?}", unit));
                        }
                    }
                }
                _ => {
                    return Err(format!("Expected Timestamp type for effective_to, got {:?}", field.data_type()));
                }
            };
            new_columns.push(array);
        } else {
            // Copy selected rows from original column
            let indices = arrow::array::UInt32Array::from(
                rows_to_keep.iter().map(|&i| i as u32).collect::<Vec<u32>>()
            );
            let selected = arrow::compute::take(original_col, &indices, None)
                .map_err(|e| format!("Failed to select rows: {}", e))?;
            new_columns.push(selected);
        }
    }

    RecordBatch::try_new(schema, new_columns)
        .map_err(|e| format!("Failed to create conflated RecordBatch: {}", e))
}

/// Consolidate multiple RecordBatches into fewer large batches to reduce Python conversion overhead
/// This combines smaller batches from different ID groups into larger consolidated batches
pub fn consolidate_final_batches(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, String> {
    
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    
    // Log batch size statistics
    let small_batches = batches.iter().filter(|b| b.num_rows() <= 1000).count();
    let _large_batches = batches.len() - small_batches;
    
    // If we only have one batch, or all batches are already large, return as-is
    if batches.len() == 1 || batches.iter().all(|b| b.num_rows() > 1000) {
        return Ok(batches);
    }
    
    // We want to group batches by schema to ensure compatibility
    let first_schema = batches[0].schema();
    
    // Check if all batches have compatible schemas (ignore metadata differences)
    for (i, batch) in batches.iter().enumerate() {
        // Compare core schema (fields and types) without metadata
        if !schemas_compatible(&first_schema, &batch.schema()) {
            return Ok(batches); // Truly incompatible schemas, return original to be safe
        }
        if i < 5 {  // Log first few schemas for debugging
        }
    }
    
    // All batches have compatible schemas, so we can consolidate them
    // Create a clean schema without metadata to avoid conflicts
    let clean_schema = create_clean_schema(&first_schema);
    
    let table = arrow::compute::concat_batches(&Arc::new(clean_schema), &batches)
        .map_err(|e| format!("Failed to consolidate batches: {}", e))?;
    
    // Split the consolidated data into reasonably-sized batches (target ~10k rows per batch)
    let mut result_batches = Vec::new();
    let target_batch_size = 10000;
    let total_rows = table.num_rows();
    
    if total_rows <= target_batch_size {
        // Small enough to be a single batch
        result_batches.push(table);
    } else {
        // Split into multiple batches of target size
        let mut offset = 0;
        while offset < total_rows {
            let length = std::cmp::min(target_batch_size, total_rows - offset);
            let slice = table.slice(offset, length);
            result_batches.push(slice);
            offset += length;
        }
    }
    
    Ok(result_batches)
}
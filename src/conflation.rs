use crate::types::*;
use crate::batch_utils::extract_date_as_datetime;
use arrow::array::{RecordBatch, TimestampMicrosecondArray, StringArray, ArrayRef};
use std::sync::Arc;
use chrono::NaiveDateTime;


pub fn simple_conflate_batches(mut batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, String> {
    if batches.len() <= 1 {
        return Ok(batches);
    }

    // Sort batches by effective_from for processing
    batches.sort_by(|a, b| {
        let a_eff_from = extract_date_as_datetime(
            a.column_by_name("effective_from").unwrap()
                .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap(),
            0
        );
        let b_eff_from = extract_date_as_datetime(
            b.column_by_name("effective_from").unwrap()
                .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap(),
            0
        );
        a_eff_from.cmp(&b_eff_from)
    });

    let mut result = Vec::new();
    let mut batches_iter = batches.into_iter();
    let mut current_batch = batches_iter.next().unwrap();

    for next_batch in batches_iter {
        // Check if we can merge current_batch with next_batch
        if can_merge_batches(&current_batch, &next_batch)? {
            // Merge by extending current_batch's effective_to
            let next_eff_to = extract_date_as_datetime(
                next_batch.column_by_name("effective_to").unwrap()
                    .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap(),
                0
            );
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
    let batch1_eff_to = extract_date_as_datetime(
        batch1.column_by_name("effective_to").unwrap()
            .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap(),
        0
    );
    let batch2_eff_from = extract_date_as_datetime(
        batch2.column_by_name("effective_from").unwrap()
            .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap(),
        0
    );

    Ok(batch1_eff_to == batch2_eff_from)
}

fn extend_batch_to_date(batch: RecordBatch, new_effective_to: NaiveDateTime) -> Result<RecordBatch, String> {
    let schema = batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::new();
    
    for field in schema.fields() {
        let column_name = field.name();
        
        if column_name == "effective_to" {
            let mut builder = TimestampMicrosecondArray::builder(1);
            let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
            let microseconds = (new_effective_to - epoch).num_microseconds().unwrap();
            builder.append_value(microseconds);
            columns.push(Arc::new(builder.finish()));
        } else {
            // Copy original column
            columns.push(batch.column_by_name(column_name).unwrap().clone());
        }
    }
    
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| e.to_string())
}

pub fn deduplicate_record_batches(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, String> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    
    // Convert RecordBatches to a more workable format for deduplication
    let mut records: Vec<(NaiveDateTime, NaiveDateTime, String, RecordBatch)> = Vec::new();
    
    for batch in batches {
        if batch.num_rows() == 1 {
            let eff_from_array = batch.column_by_name("effective_from").unwrap()
                .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            let eff_to_array = batch.column_by_name("effective_to").unwrap()
                .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            let hash_array = batch.column_by_name("value_hash").unwrap()
                .as_any().downcast_ref::<StringArray>().unwrap();
            
            let eff_from = extract_date_as_datetime(eff_from_array, 0);
            let eff_to = extract_date_as_datetime(eff_to_array, 0);
            let hash = hash_array.value(0).to_string();
            
            records.push((eff_from, eff_to, hash, batch));
        }
    }
    
    // Sort by effective_from, then effective_to, then hash
    records.sort_by(|a, b| {
        match a.0.cmp(&b.0) {
            std::cmp::Ordering::Equal => {
                match a.1.cmp(&b.1) {
                    std::cmp::Ordering::Equal => a.2.cmp(&b.2),
                    other => other,
                }
            }
            other => other,
        }
    });
    
    // Remove exact duplicates
    let mut deduped: Vec<RecordBatch> = Vec::new();
    let mut last_key: Option<(NaiveDateTime, NaiveDateTime, String)> = None;
    
    for (eff_from, eff_to, hash, batch) in records {
        let current_key = (eff_from, eff_to, hash);
        if last_key != Some(current_key.clone()) {
            deduped.push(batch);
            last_key = Some(current_key);
        }
    }
    
    Ok(deduped)
}

/// Consolidate multiple RecordBatches into fewer large batches to reduce Python conversion overhead
/// This combines smaller batches from different ID groups into larger consolidated batches
pub fn consolidate_final_batches(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, String> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    
    // If we only have one batch, or all batches are already large, return as-is
    if batches.len() == 1 || batches.iter().all(|b| b.num_rows() > 1000) {
        return Ok(batches);
    }
    
    // We want to group batches by schema to ensure compatibility
    let first_schema = batches[0].schema();
    
    // Check if all batches have the same schema - if not, return original (safer)
    for batch in &batches {
        if batch.schema() != first_schema {
            return Ok(batches); // Mixed schemas, return original to be safe
        }
    }
    
    // All batches have the same schema, so we can consolidate them
    // Convert all batches into a single large table, then split into reasonable chunks
    let table = arrow::compute::concat_batches(&first_schema, &batches)
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
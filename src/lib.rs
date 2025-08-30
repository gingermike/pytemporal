use arrow::array::{RecordBatch, TimestampMicrosecondArray};
use chrono::NaiveDate;
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;
use std::collections::{HashMap, HashSet};
use rayon::prelude::*;

mod types;
mod overlap;
mod timeline;
mod conflation;
mod batch_utils;

pub use types::*;
use timeline::process_id_timeline;
use conflation::{deduplicate_record_batches, simple_conflate_batches, consolidate_final_batches};
use batch_utils::{hash_values, extract_date_as_datetime, extract_timestamp, add_hash_column};

pub fn process_updates(
    current_state: RecordBatch,
    updates: RecordBatch,
    id_columns: Vec<String>,
    value_columns: Vec<String>,
    system_date: NaiveDate,
    update_mode: UpdateMode,
) -> Result<ChangeSet, String> {
    // Generate a consistent as_of_from timestamp for tombstone records
    let batch_timestamp = chrono::Utc::now().naive_utc();
    
    let mut to_expire = Vec::new();
    let mut to_insert = Vec::new();
    
    // Group by ID to process each timeseries independently
    let mut id_groups: HashMap<Vec<ScalarValue>, (Vec<BitemporalRecord>, Vec<BitemporalRecord>)> = HashMap::new();
    
    // Parse current state
    let eff_from_array = current_state.column_by_name("effective_from").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let eff_to_array = current_state.column_by_name("effective_to").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let as_of_from_array = current_state.column_by_name("as_of_from").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    
    // Collect current state records
    for row_idx in 0..current_state.num_rows() {
        let mut id_values = Vec::new();
        for id_col in &id_columns {
            let col_idx = current_state.schema().index_of(id_col).unwrap();
            let array = current_state.column(col_idx);
            id_values.push(ScalarValue::from_array(array, row_idx));
        }
        
        let record = BitemporalRecord {
            id_values: id_values.clone(),
            value_hash: hash_values(&current_state, row_idx, &value_columns),
            effective_from: extract_date_as_datetime(eff_from_array, row_idx),
            effective_to: extract_date_as_datetime(eff_to_array, row_idx),
            as_of_from: extract_timestamp(as_of_from_array, row_idx),
            as_of_to: MAX_TIMESTAMP,
            original_index: Some(row_idx),
        };
        
        id_groups.entry(id_values).or_insert((Vec::new(), Vec::new())).0.push(record);
    }
    
    // Parse updates
    let upd_eff_from_array = updates.column_by_name("effective_from").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let upd_eff_to_array = updates.column_by_name("effective_to").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let upd_as_of_from_array = updates.column_by_name("as_of_from").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    
    for upd_idx in 0..updates.num_rows() {
        let mut upd_id_values = Vec::new();
        for id_col in &id_columns {
            let col_idx = updates.schema().index_of(id_col).unwrap();
            let array = updates.column(col_idx);
            upd_id_values.push(ScalarValue::from_array(array, upd_idx));
        }
        
        let record = BitemporalRecord {
            id_values: upd_id_values.clone(),
            value_hash: hash_values(&updates, upd_idx, &value_columns),
            effective_from: extract_date_as_datetime(upd_eff_from_array, upd_idx),
            effective_to: extract_date_as_datetime(upd_eff_to_array, upd_idx),
            as_of_from: extract_timestamp(upd_as_of_from_array, upd_idx),  // Preserve original input timestamp
            as_of_to: MAX_TIMESTAMP,
            original_index: Some(upd_idx), // Store update index for reference
        };
        
        id_groups.entry(upd_id_values).or_insert((Vec::new(), Vec::new())).1.push(record);
    }
    
    // Track which IDs appear in updates (for full state mode)
    let _update_ids: HashSet<Vec<ScalarValue>> = id_groups.iter()
        .filter(|(_, (_, updates_vec))| !updates_vec.is_empty())
        .map(|(id, _)| id.clone())
        .collect();
    
    // Use parallel processing for large datasets to improve CPU utilization
    let use_parallel = id_groups.len() > 50 ||
                      (current_state.num_rows() + updates.num_rows()) > 10000;
    
    if use_parallel {
        // Process ID groups in parallel for large datasets
        let results: Result<Vec<(Vec<usize>, Vec<RecordBatch>)>, String> = id_groups
            .into_par_iter()
            .map(|(_id_values, (current_records, update_records))| {
                process_id_group(
                    current_records,
                    update_records,
                    &current_state,
                    &updates,
                    &id_columns,
                    &value_columns,
                    system_date,
                    update_mode,
                    batch_timestamp,
                )
            })
            .collect();
        
        // Merge parallel results
        let results = results?;
        for (expire_indices, insert_batches) in results {
            to_expire.extend(expire_indices);
            to_insert.extend(insert_batches);
        }
    } else {
        // Process ID groups serially for small datasets to avoid overhead
        for (_id_values, (current_records, update_records)) in id_groups {
            let (expire_indices, insert_batches) = process_id_group(
                current_records,
                update_records,
                &current_state,
                &updates,
                &id_columns,
                &value_columns,
                system_date,
                update_mode,
                batch_timestamp,
            )?;
            
            to_expire.extend(expire_indices);
            to_insert.extend(insert_batches);
        }
    }
    
    // Sort and deduplicate expiry indices
    to_expire.sort_unstable();
    to_expire.dedup();
    
    // Deduplicate insert batches by combining identical time periods
    to_insert = deduplicate_record_batches(to_insert)?;
    
    // Simple post-processing conflation for adjacent segments
    to_insert = simple_conflate_batches(to_insert)?;
    
    // Final consolidation - combine all batches into fewer large batches
    to_insert = consolidate_final_batches(to_insert)?;
    
    // Create expired record batches with updated as_of_to timestamp
    let expired_records = if !to_expire.is_empty() {
        vec![crate::batch_utils::create_expired_records_batch(&current_state, &to_expire, batch_timestamp)?]
    } else {
        Vec::new()
    };

    Ok(ChangeSet { to_expire, to_insert, expired_records })
}

// Extract ID group processing logic for reuse in parallel and serial paths
fn process_id_group(
    mut current_records: Vec<BitemporalRecord>,
    mut update_records: Vec<BitemporalRecord>,
    current_state: &RecordBatch,
    updates: &RecordBatch,
    id_columns: &[String],
    value_columns: &[String],
    system_date: NaiveDate,
    update_mode: UpdateMode,
    batch_timestamp: chrono::NaiveDateTime,
) -> Result<(Vec<usize>, Vec<RecordBatch>), String> {
    // Sort records by effective_from for chronological processing
    current_records.sort_by_key(|r| r.effective_from);
    update_records.sort_by_key(|r| r.effective_from);
    
    let mut expire_indices = Vec::new();
    let mut insert_batches = Vec::new();
    
    if update_records.is_empty() {
        // No updates for this ID
        if update_mode == UpdateMode::FullState {
            // In full state mode, expire all current records for IDs not in updates
            // and create tombstone records
            let mut tombstone_records = Vec::new();
            
            for record in current_records {
                if let Some(orig_idx) = record.original_index {
                    expire_indices.push(orig_idx);
                }
                
                // Create tombstone record with effective_to = system_date
                let system_date_time = system_date.and_hms_opt(0, 0, 0).unwrap_or_else(|| {
                    panic!("Failed to convert system_date to datetime")
                });
                
                // Use the same timestamp as regular update records for consistency
                // Find any update record to get the processing timestamp
                let current_timestamp = if updates.num_rows() > 0 {
                    let upd_as_of_from_array = updates.column_by_name("as_of_from").unwrap()
                        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    extract_timestamp(upd_as_of_from_array, 0)  // Use first update's timestamp
                } else {
                    batch_timestamp  // Fallback if no updates
                };
                
                // For tombstones, we need to collect both ID and value columns from the original record
                let mut all_column_values = record.id_values.clone();
                
                // Add value columns from the current state record
                if let Some(orig_idx) = record.original_index {
                    for value_col in value_columns {
                        let col_idx = current_state.schema().index_of(value_col).unwrap();
                        let array = current_state.column(col_idx);
                        all_column_values.push(ScalarValue::from_array(array, orig_idx));
                    }
                }
                
                let tombstone = BitemporalRecord {
                    id_values: all_column_values,  // Include both ID and value columns for tombstone reconstruction
                    value_hash: record.value_hash.clone(),
                    effective_from: record.effective_from,
                    effective_to: system_date_time,  // Truncate to system date
                    as_of_from: current_timestamp,  // Use current timestamp like regular updates
                    as_of_to: chrono::NaiveDate::from_ymd_opt(2260, 12, 31)
                        .unwrap()
                        .and_hms_opt(23, 59, 59)
                        .unwrap(),
                    original_index: None,  // Tombstones don't come from source data
                };
                tombstone_records.push(tombstone);
            }
            
            // Create separate batch for tombstone records if any were created
            if !tombstone_records.is_empty() {
                let tombstone_batch = create_tombstone_batch(&tombstone_records, current_state)?;
                insert_batches.push(tombstone_batch);
            }
        }
        return Ok((expire_indices, insert_batches));
    }
    
    if update_mode == UpdateMode::FullState {
        // In full state mode, only expire/insert records if values have actually changed
        // Also create tombstone records for deleted items (exist in current but not in updates by ID)
        
        // First, collect all ID values present in updates
        let mut update_id_set = std::collections::HashSet::new();
        for update_record in &update_records {
            update_id_set.insert(update_record.id_values.clone());
        }
        
        let mut tombstone_records = Vec::new();
        
        
        // Compare current records with update records
        for current_record in &current_records {
            // Check if this current record's ID exists in the updates at all
            if update_id_set.contains(&current_record.id_values) {
                // ID exists in updates, find corresponding update record
                // In full_state mode, we need to handle temporal adjustments
                let matching_update = update_records.iter().find(|update_record| {
                    current_record.effective_from == update_record.effective_from
                });
                
                if let Some(update_record) = matching_update {
                    // Check if values or temporal range changed
                    let values_changed = current_record.value_hash != update_record.value_hash;
                    let temporal_changed = current_record.effective_to != update_record.effective_to;
                    
                    if values_changed || temporal_changed {
                        // Either values or temporal range changed
                        if let Some(orig_idx) = current_record.original_index {
                            expire_indices.push(orig_idx);
                        }
                    }
                    // If neither values nor temporal range changed, do nothing
                } else {
                    // No matching update record (different effective_from), expire this current record
                    if let Some(orig_idx) = current_record.original_index {
                        expire_indices.push(orig_idx);
                    }
                }
            } else {
                // This current record's ID doesn't exist in updates - it's being deleted
                // Expire the current record
                if let Some(orig_idx) = current_record.original_index {
                    expire_indices.push(orig_idx);
                }
                
                // Create tombstone record with effective_to = system_date
                let system_date_time = system_date.and_hms_opt(0, 0, 0).unwrap_or_else(|| {
                    panic!("Failed to convert system_date to datetime")
                });
                
                // Use the same timestamp as regular update records for consistency
                let current_timestamp = if updates.num_rows() > 0 {
                    let upd_as_of_from_array = updates.column_by_name("as_of_from").unwrap()
                        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    extract_timestamp(upd_as_of_from_array, 0)  // Use first update's timestamp
                } else {
                    batch_timestamp  // Fallback if no updates
                };
                
                // For tombstones, we need to collect both ID and value columns from the original record
                let mut all_column_values = current_record.id_values.clone();
                
                // Add value columns from the current state record
                if let Some(orig_idx) = current_record.original_index {
                    for value_col in value_columns {
                        let col_idx = current_state.schema().index_of(value_col).unwrap();
                        let array = current_state.column(col_idx);
                        all_column_values.push(ScalarValue::from_array(array, orig_idx));
                    }
                }
                
                let tombstone = BitemporalRecord {
                    id_values: all_column_values,  // Include both ID and value columns for tombstone reconstruction
                    value_hash: current_record.value_hash.clone(),
                    effective_from: current_record.effective_from,
                    effective_to: system_date_time,  // Truncate to system date
                    as_of_from: current_timestamp,  // Use consistent timestamp with regular updates
                    as_of_to: chrono::NaiveDate::from_ymd_opt(2260, 12, 31)
                        .unwrap()
                        .and_hms_opt(23, 59, 59)
                        .unwrap(),
                    original_index: None,  // Tombstones don't come from source data
                };
                tombstone_records.push(tombstone);
            }
        }
        
        // Insert only update records that either:
        // 1. Have no matching current record (new records), or  
        // 2. Have different values from their matching current record
        let mut records_to_insert = Vec::new();
        let mut source_rows_to_insert = Vec::new();
        
        for update_record in update_records {
            let matching_current = current_records.iter().find(|current_record| {
                current_record.effective_from == update_record.effective_from &&
                current_record.effective_to == update_record.effective_to
            });
            
            if let Some(current_record) = matching_current {
                // Found matching temporal range, only insert if values changed
                if current_record.value_hash != update_record.value_hash {
                    records_to_insert.push(update_record.clone());
                    source_rows_to_insert.push(update_record.original_index.unwrap());
                }
            } else {
                // No matching current record, insert this new record
                records_to_insert.push(update_record.clone());
                source_rows_to_insert.push(update_record.original_index.unwrap());
            }
        }
        
        // Create batch from regular update records
        if !records_to_insert.is_empty() {
            let batch = crate::batch_utils::create_record_batch_from_records(
                &records_to_insert,
                updates,
                &source_rows_to_insert,
            )?;
            insert_batches.push(batch);
        }
        
        // Create separate batch for tombstone records (they don't have source rows)
        if !tombstone_records.is_empty() {
            // For tombstones, we need to create a batch from the record data itself
            // Since tombstones use the same schema as updates, we can create a synthetic batch
            let tombstone_batch = create_tombstone_batch(&tombstone_records, current_state)?;
            insert_batches.push(tombstone_batch);
        }
        
    } else {
        // Delta mode - use concurrent pointer approach
        let (expire_idx, insert_batch) = process_id_timeline(
            &current_records,
            &update_records,
            current_state,
            updates,
            id_columns,
            value_columns,
            system_date,
        )?;
        
        expire_indices.extend(expire_idx);
        insert_batches.extend(insert_batch);
    }
    
    Ok((expire_indices, insert_batches))
}

#[pyfunction]
fn compute_changes(
    current_state: PyRecordBatch,
    updates: PyRecordBatch,
    id_columns: Vec<String>,
    value_columns: Vec<String>,
    system_date: String,
    update_mode: String,
) -> PyResult<(Vec<usize>, Vec<PyRecordBatch>, Vec<PyRecordBatch>)> {
    // Convert PyRecordBatch to Arrow RecordBatch
    let current_batch = current_state.as_ref().clone();
    let updates_batch = updates.as_ref().clone();
    
    // Parse system_date
    let system_date = chrono::NaiveDate::parse_from_str(&system_date, "%Y-%m-%d")
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("Invalid date format: {}", e)))?;
    
    // Parse update_mode
    let mode = match update_mode.as_str() {
        "delta" => UpdateMode::Delta,
        "full_state" => UpdateMode::FullState,
        _ => return Err(pyo3::exceptions::PyValueError::new_err("Invalid update_mode. Must be 'delta' or 'full_state'")),
    };
    
    // Call the process_updates function
    let changeset = process_updates(
        current_batch,
        updates_batch,
        id_columns,
        value_columns,
        system_date,
        mode,
    ).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;
    
    // Convert the result back to Python types
    let expire_indices = changeset.to_expire;
    let insert_batches: Vec<PyRecordBatch> = changeset.to_insert
        .into_iter()
        .map(|batch| PyRecordBatch::new(batch))
        .collect();
    let expired_batches: Vec<PyRecordBatch> = changeset.expired_records
        .into_iter()
        .map(|batch| PyRecordBatch::new(batch))
        .collect();
    
    Ok((expire_indices, insert_batches, expired_batches))
}

#[pyfunction]
fn add_hash_key(
    record_batch: PyRecordBatch,
    value_fields: Vec<String>,
) -> PyResult<PyRecordBatch> {
    // Convert PyRecordBatch to Arrow RecordBatch
    let batch = record_batch.as_ref().clone();
    
    // Call the add_hash_column function
    let batch_with_hash = add_hash_column(&batch, &value_fields)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e))?;
    
    // Convert back to PyRecordBatch
    Ok(PyRecordBatch::new(batch_with_hash))
}

/// Creates a RecordBatch from tombstone records using the same schema as the updates batch
fn create_tombstone_batch(
    tombstone_records: &[BitemporalRecord], 
    updates: &RecordBatch
) -> Result<RecordBatch, String> {
    use arrow::array::{StringBuilder, Float64Builder, Int32Builder, BooleanBuilder, TimestampMicrosecondArray};
    use std::sync::Arc;
    
    if tombstone_records.is_empty() {
        return Err("Cannot create batch from empty tombstone records".to_string());
    }
    
    let schema = updates.schema();
    let num_records = tombstone_records.len();
    let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(schema.fields().len());
    
    // Epoch for timestamp conversion
    let epoch = chrono::DateTime::from_timestamp(0, 0)
        .unwrap()
        .naive_utc();
    
    for field in schema.fields() {
        let column_name = field.name();
        
        match column_name.as_str() {
            "effective_from" => {
                let mut builder = TimestampMicrosecondArray::builder(num_records);
                for record in tombstone_records {
                    let microseconds = (record.effective_from - epoch).num_microseconds().unwrap();
                    builder.append_value(microseconds);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "effective_to" => {
                let mut builder = TimestampMicrosecondArray::builder(num_records);
                for record in tombstone_records {
                    let microseconds = (record.effective_to - epoch).num_microseconds().unwrap();
                    builder.append_value(microseconds);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "as_of_from" => {
                let mut builder = TimestampMicrosecondArray::builder(num_records);
                for record in tombstone_records {
                    let microseconds = (record.as_of_from - epoch).num_microseconds().unwrap();
                    builder.append_value(microseconds);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "as_of_to" => {
                let mut builder = TimestampMicrosecondArray::builder(num_records);
                for record in tombstone_records {
                    let microseconds = (record.as_of_to - epoch).num_microseconds().unwrap();
                    builder.append_value(microseconds);
                }
                columns.push(Arc::new(builder.finish()));
            }
            "value_hash" => {
                let mut builder = StringBuilder::new();
                for record in tombstone_records {
                    builder.append_value(&record.value_hash);
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                // For ID and value columns, extract from BitemporalRecord.id_values
                // The id_values array now contains both ID and value columns in schema order
                let field_index = schema.index_of(column_name)
                    .map_err(|_| format!("Column '{}' not found in schema", column_name))?;
                
                // Count non-temporal columns before this one to get the index in id_values
                let mut data_value_index = 0;
                for i in 0..field_index {
                    let fname = schema.field(i).name();
                    if !matches!(fname.as_str(), "effective_from" | "effective_to" | "as_of_from" | "as_of_to" | "value_hash") {
                        data_value_index += 1;
                    }
                }
                
                // Build the column based on data type
                match field.data_type() {
                    arrow::datatypes::DataType::Utf8 => {
                        let mut builder = StringBuilder::new();
                        for record in tombstone_records {
                            if let Some(crate::types::ScalarValue::String(s)) = record.id_values.get(data_value_index) {
                                builder.append_value(s);
                            } else {
                                builder.append_null();
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Int32 => {
                        let mut builder = Int32Builder::new();
                        for record in tombstone_records {
                            if let Some(crate::types::ScalarValue::Int32(i)) = record.id_values.get(data_value_index) {
                                builder.append_value(*i);
                            } else {
                                builder.append_null();
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Int64 => {
                        let mut builder = arrow::array::Int64Builder::new();
                        for record in tombstone_records {
                            if let Some(crate::types::ScalarValue::Int64(i)) = record.id_values.get(data_value_index) {
                                builder.append_value(*i);
                            } else {
                                builder.append_null();
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Float64 => {
                        let mut builder = Float64Builder::new();
                        for record in tombstone_records {
                            if let Some(crate::types::ScalarValue::Float64(f)) = record.id_values.get(data_value_index) {
                                builder.append_value(f.0);
                            } else {
                                builder.append_null();
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Boolean => {
                        let mut builder = BooleanBuilder::new();
                        for record in tombstone_records {
                            if let Some(crate::types::ScalarValue::Boolean(b)) = record.id_values.get(data_value_index) {
                                builder.append_value(*b);
                            } else {
                                builder.append_null();
                            }
                        }
                        columns.push(Arc::new(builder.finish()));
                    }
                    arrow::datatypes::DataType::Null => {
                        // Create a null array with the right length
                        use arrow::array::NullArray;
                        let null_array = NullArray::new(tombstone_records.len());
                        columns.push(Arc::new(null_array));
                    }
                    _ => {
                        return Err(format!("Unsupported data type for tombstone records: {:?}", field.data_type()));
                    }
                }
            }
        }
    }
    
    RecordBatch::try_new(schema, columns)
        .map_err(|e| format!("Failed to create tombstone RecordBatch: {}", e))
}

#[pymodule]
fn pytemporal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compute_changes, m)?)?;
    m.add_function(wrap_pyfunction!(add_hash_key, m)?)?;
    Ok(())
}
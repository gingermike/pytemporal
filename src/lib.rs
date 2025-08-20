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
use conflation::{deduplicate_record_batches, simple_conflate_batches};
use batch_utils::{hash_values, extract_date_as_datetime, extract_timestamp, add_hash_column};

pub fn process_updates(
    current_state: RecordBatch,
    updates: RecordBatch,
    id_columns: Vec<String>,
    value_columns: Vec<String>,
    system_date: NaiveDate,
    update_mode: UpdateMode,
) -> Result<ChangeSet, String> {
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
            as_of_from: extract_timestamp(upd_as_of_from_array, upd_idx),
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
    
    Ok(ChangeSet { to_expire, to_insert })
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
            for record in current_records {
                if let Some(orig_idx) = record.original_index {
                    expire_indices.push(orig_idx);
                }
            }
        }
        return Ok((expire_indices, insert_batches));
    }
    
    if update_mode == UpdateMode::FullState {
        // In full state mode, expire all current records and insert update records as-is
        for record in &current_records {
            if let Some(orig_idx) = record.original_index {
                expire_indices.push(orig_idx);
            }
        }
        
        // Insert all update records as a batch
        if !update_records.is_empty() {
            let records: Vec<BitemporalRecord> = update_records.iter().cloned().collect();
            let source_rows: Vec<usize> = update_records.iter().map(|r| r.original_index.unwrap()).collect();
            
            let batch = crate::batch_utils::create_record_batch_from_records(
                &records,
                updates,
                &source_rows,
            )?;
            insert_batches.push(batch);
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
) -> PyResult<(Vec<usize>, Vec<PyRecordBatch>)> {
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
    
    Ok((expire_indices, insert_batches))
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

#[pymodule]
fn pytemporal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compute_changes, m)?)?;
    m.add_function(wrap_pyfunction!(add_hash_key, m)?)?;
    Ok(())
}
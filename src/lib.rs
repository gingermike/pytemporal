use arrow::array::{
    Array, ArrayRef, Date32Array, RecordBatch, StringArray, 
    Int32Array, Int64Array, Float64Array
};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::{NaiveDate, Days};
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;
use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::Arc;
use xxhash_rust::xxh64::xxh64;
use ordered_float::OrderedFloat;

#[derive(Debug, Clone)]
struct BitemporalRecord {
    id_values: Vec<ScalarValue>,
    value_hash: u64,
    effective_from: NaiveDate,
    effective_to: NaiveDate,
    as_of_from: NaiveDate,
    as_of_to: NaiveDate,  // No longer optional - use MAX_DATE for infinity
    original_index: Option<usize>,
}

// Pandas-compatible max date (pandas can't handle dates beyond ~2262)
const MAX_DATE: NaiveDate = match NaiveDate::from_ymd_opt(2262, 4, 11) {
    Some(date) => date,
    None => panic!("Invalid max date"),
};

#[derive(Debug, Clone, Copy, PartialEq)]
enum UpdateMode {
    Delta,      // Only provided records are updates
    FullState,  // Provided records represent complete state
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum ScalarValue {
    String(String),
    Int32(i32),
    Int64(i64),
    Float64(ordered_float::OrderedFloat<f64>),
    Date32(i32),
}

impl ScalarValue {
    fn from_array(array: &ArrayRef, idx: usize) -> Self {
        if array.is_null(idx) {
            return ScalarValue::String("NULL".to_string());
        }
        
        match array.data_type() {
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                ScalarValue::String(arr.value(idx).to_string())
            }
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                ScalarValue::Int32(arr.value(idx))
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                ScalarValue::Int64(arr.value(idx))
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                ScalarValue::Float64(ordered_float::OrderedFloat(arr.value(idx)))
            }
            DataType::Date32 => {
                let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
                ScalarValue::Date32(arr.value(idx))
            }
            _ => panic!("Unsupported data type: {:?}", array.data_type()),
        }
    }
}

#[derive(Debug)]
struct ChangeSet {
    to_expire: Vec<usize>,  // Indices of rows to mark as expired
    to_insert: Vec<RecordBatch>,  // New rows to insert
}

fn extract_date(array: &Date32Array, idx: usize) -> NaiveDate {
    let days_since_epoch = array.value(idx);
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Days::new(days_since_epoch as u64)
}

fn hash_values(record_batch: &RecordBatch, row_idx: usize, value_columns: &[String]) -> u64 {
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
    
    xxh64(&hasher_input, 0)
}

fn process_updates(
    current_state: RecordBatch,
    updates: RecordBatch,
    id_columns: Vec<String>,
    value_columns: Vec<String>,
    system_date: NaiveDate,
    update_mode: UpdateMode,
) -> PyResult<ChangeSet> {
    // Build index of current state by ID and effective_from
    let mut state_index: HashMap<Vec<ScalarValue>, BTreeMap<NaiveDate, BitemporalRecord>> = HashMap::new();
    
    // Parse current state
    let eff_from_col = current_state.column_by_name("effective_from").unwrap();
    let eff_to_col = current_state.column_by_name("effective_to").unwrap();
    let as_of_from_col = current_state.column_by_name("as_of_from").unwrap();
    let as_of_to_col = current_state.column_by_name("as_of_to").unwrap();
    
    let eff_from_array = eff_from_col.as_any().downcast_ref::<Date32Array>().unwrap();
    let eff_to_array = eff_to_col.as_any().downcast_ref::<Date32Array>().unwrap();
    let as_of_from_array = as_of_from_col.as_any().downcast_ref::<Date32Array>().unwrap();
    let as_of_to_array = as_of_to_col.as_any().downcast_ref::<Date32Array>().unwrap();
    
    // Build sorted index of current state
    for row_idx in 0..current_state.num_rows() {
        // Skip already expired rows
        let as_of_to = extract_date(as_of_to_array, row_idx);
        if as_of_to != MAX_DATE {
            continue;
        }
        
        let mut id_values = Vec::new();
        for id_col in &id_columns {
            let col_idx = current_state.schema().index_of(id_col).unwrap();
            let array = current_state.column(col_idx);
            id_values.push(ScalarValue::from_array(array, row_idx));
        }
        
        let record = BitemporalRecord {
            id_values: id_values.clone(),
            value_hash: hash_values(&current_state, row_idx, &value_columns),
            effective_from: extract_date(eff_from_array, row_idx),
            effective_to: extract_date(eff_to_array, row_idx),
            as_of_from: extract_date(as_of_from_array, row_idx),
            as_of_to: MAX_DATE,
            original_index: Some(row_idx),
        };
        
        // BTreeMap automatically keeps records sorted by effective_from
        state_index.entry(id_values)
            .or_insert_with(BTreeMap::new)
            .insert(record.effective_from, record);
    }
    
    // Validate current state has no overlaps
    for (id_values, timeline) in &state_index {
        let mut prev_end: Option<NaiveDate> = None;
        for (_, record) in timeline.iter() {
            if let Some(prev) = prev_end {
                if prev > record.effective_from {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        format!("Overlapping records found in current state for ID {:?}", id_values)
                    ));
                }
            }
            prev_end = Some(record.effective_to);
        }
    }
    
    let mut to_expire = Vec::new();
    let mut to_insert = Vec::new();
    
    // Track which IDs we've seen in updates (for full state mode)
    let mut seen_ids: HashSet<Vec<ScalarValue>> = HashSet::new();
    
    // Sort updates by ID and effective_from for consistent processing
    let mut update_records: Vec<(usize, Vec<ScalarValue>, NaiveDate, NaiveDate, u64)> = Vec::new();
    
    let upd_eff_from_array = updates.column_by_name("effective_from")
        .unwrap().as_any().downcast_ref::<Date32Array>().unwrap();
    let upd_eff_to_array = updates.column_by_name("effective_to")
        .unwrap().as_any().downcast_ref::<Date32Array>().unwrap();
    
    for upd_idx in 0..updates.num_rows() {
        let mut upd_id_values = Vec::new();
        for id_col in &id_columns {
            let col_idx = updates.schema().index_of(id_col).unwrap();
            let array = updates.column(col_idx);
            upd_id_values.push(ScalarValue::from_array(array, upd_idx));
        }
        
        let upd_eff_from = extract_date(upd_eff_from_array, upd_idx);
        let upd_eff_to = extract_date(upd_eff_to_array, upd_idx);
        let upd_hash = hash_values(&updates, upd_idx, &value_columns);
        
        update_records.push((upd_idx, upd_id_values, upd_eff_from, upd_eff_to, upd_hash));
    }
    
    // Sort updates by ID and effective_from to ensure consistent processing
    update_records.sort_by(|a, b| {
        match a.1.cmp(&b.1) {
            std::cmp::Ordering::Equal => a.2.cmp(&b.2),
            other => other,
        }
    });
    
    // Process sorted updates
    for (upd_idx, upd_id_values, upd_eff_from, upd_eff_to, upd_hash) in update_records {
        // Track this ID for full state mode
        seen_ids.insert(upd_id_values.clone());
        
        if let Some(timeline) = state_index.get_mut(&upd_id_values) {
            // Find ALL overlapping records (not just processing them one by one)
            let overlapping: Vec<_> = timeline
                .iter()
                .filter(|(_, rec)| {
                    rec.effective_from < upd_eff_to && rec.effective_to > upd_eff_from
                })
                .map(|(k, v)| (*k, v.clone()))
                .collect();
            
            if overlapping.is_empty() {
                // No overlap - pure insert
                let new_rec = BitemporalRecord {
                    id_values: upd_id_values,
                    value_hash: upd_hash,
                    effective_from: upd_eff_from,
                    effective_to: upd_eff_to,
                    as_of_from: system_date,
                    as_of_to: MAX_DATE,
                    original_index: None,
                };
                to_insert.push(create_record_batch_from_update(&updates, upd_idx, &new_rec)?);
            } else {
                // Process all overlapping records together
                let first_start = overlapping.iter().map(|(_, r)| r.effective_from).min().unwrap();
                let last_end = overlapping.iter().map(|(_, r)| r.effective_to).max().unwrap();
                
                // Mark all overlapping records for expiry
                for (_, existing) in &overlapping {
                    if let Some(orig_idx) = existing.original_index {
                        to_expire.push(orig_idx);
                    }
                }
                
                // Build the new timeline segments
                let mut segments: Vec<(NaiveDate, NaiveDate, Option<&BitemporalRecord>, bool)> = Vec::new();
                
                // Add segment before update if needed
                if first_start < upd_eff_from {
                    let before_rec = overlapping.iter()
                        .find(|(_, r)| r.effective_from <= upd_eff_from && r.effective_to > upd_eff_from)
                        .map(|(_, r)| r);
                    segments.push((first_start, upd_eff_from, before_rec, false));
                }
                
                // Add the update segment
                segments.push((
                    upd_eff_from.max(first_start), 
                    upd_eff_to.min(last_end), 
                    None, 
                    true
                ));
                
                // Add segment after update if needed
                if last_end > upd_eff_to {
                    let after_rec = overlapping.iter()
                        .find(|(_, r)| r.effective_from < upd_eff_to && r.effective_to >= upd_eff_to)
                        .map(|(_, r)| r);
                    segments.push((upd_eff_to, last_end, after_rec, false));
                }
                
                // Fill any gaps between original records
                let mut sorted_overlapping = overlapping.clone();
                sorted_overlapping.sort_by_key(|(_, r)| r.effective_from);
                
                for window in sorted_overlapping.windows(2) {
                    let (_, rec1) = &window[0];
                    let (_, rec2) = &window[1];
                    
                    if rec1.effective_to < rec2.effective_from {
                        // There's a gap - check if it's covered by our segments
                        let gap_start = rec1.effective_to;
                        let gap_end = rec2.effective_from;
                        
                        // Only add gap segments that aren't covered by the update
                        if gap_end <= upd_eff_from || gap_start >= upd_eff_to {
                            segments.push((gap_start, gap_end, None, false));
                        }
                    }
                }
                
                // Sort segments and merge adjacent ones with same properties
                segments.sort_by_key(|s| s.0);
                
                // Create record batches for each segment
                for (seg_from, seg_to, source_rec, is_update) in segments {
                    if is_update {
                        // This is the update segment
                        let update_rec = BitemporalRecord {
                            id_values: upd_id_values.clone(),
                            value_hash: upd_hash,
                            effective_from: seg_from,
                            effective_to: seg_to,
                            as_of_from: system_date,
                            as_of_to: MAX_DATE,
                            original_index: None,
                        };
                        to_insert.push(create_record_batch_from_update(&updates, upd_idx, &update_rec)?);
                    } else if let Some(rec) = source_rec {
                        // This segment comes from an existing record
                        let segment_rec = BitemporalRecord {
                            id_values: rec.id_values.clone(),
                            value_hash: rec.value_hash,
                            effective_from: seg_from,
                            effective_to: seg_to,
                            as_of_from: system_date,
                            as_of_to: MAX_DATE,
                            original_index: None,
                        };
                        to_insert.push(create_record_batch_from_record(
                            &segment_rec,
                            &current_state,
                            rec.original_index.unwrap(),
                            &id_columns,
                            &value_columns
                        )?);
                    }
                }
                
                // Remove all overlapping records from the timeline
                for (eff_from, _) in overlapping {
                    timeline.remove(&eff_from);
                }
            }
        } else {
            // Pure insert - no existing records
            let new_rec = BitemporalRecord {
                id_values: upd_id_values,
                value_hash: upd_hash,
                effective_from: upd_eff_from,
                effective_to: upd_eff_to,
                as_of_from: system_date,
                as_of_to: MAX_DATE,
                original_index: None,
            };
            to_insert.push(create_record_batch_from_update(&updates, upd_idx, &new_rec)?);
        }
    }
    
    // Handle full state mode - expire any IDs not in the update set
    if update_mode == UpdateMode::FullState {
        for (id_values, timeline) in &state_index {
            if !seen_ids.contains(id_values) {
                // This ID is not in the updates, so expire all its records
                for (_, record) in timeline {
                    if let Some(orig_idx) = record.original_index {
                        to_expire.push(orig_idx);
                    }
                }
            }
        }
    }
    
    // Sort and deduplicate expiry indices
    to_expire.sort_unstable();
    to_expire.dedup();
    
    Ok(ChangeSet { to_expire, to_insert })
}

fn create_record_batch_from_record(
    record: &BitemporalRecord, 
    original_batch: &RecordBatch,
    original_row: usize,
    _id_columns: &[String],
    _value_columns: &[String]
) -> PyResult<RecordBatch> {
    let schema = original_batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::new();
    
    for field in schema.fields() {
        let column_name = field.name();
        
        if column_name == "effective_from" {
            let mut builder = Date32Array::builder(1);
            let days = (record.effective_from - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
            builder.append_value(days as i32);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "effective_to" {
            let mut builder = Date32Array::builder(1);
            let days = (record.effective_to - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
            builder.append_value(days as i32);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "as_of_from" {
            let mut builder = Date32Array::builder(1);
            let days = (record.as_of_from - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
            builder.append_value(days as i32);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "as_of_to" {
            let mut builder = Date32Array::builder(1);
            let days = (record.as_of_to - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
            builder.append_value(days as i32);
            columns.push(Arc::new(builder.finish()));
        } else {
            // Copy from original batch
            let orig_array = original_batch.column_by_name(column_name).unwrap();
            let new_array = orig_array.slice(original_row, 1);
            columns.push(new_array);
        }
    }
    
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

fn create_record_batch_from_update(
    updates: &RecordBatch, 
    row_idx: usize, 
    record: &BitemporalRecord
) -> PyResult<RecordBatch> {
    let schema = updates.schema();
    let mut columns: Vec<ArrayRef> = Vec::new();
    
    for field in schema.fields() {
        let column_name = field.name();
        
        if column_name == "effective_from" {
            let mut builder = Date32Array::builder(1);
            let days = (record.effective_from - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
            builder.append_value(days as i32);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "effective_to" {
            let mut builder = Date32Array::builder(1);
            let days = (record.effective_to - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
            builder.append_value(days as i32);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "as_of_from" {
            let mut builder = Date32Array::builder(1);
            let days = (record.as_of_from - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
            builder.append_value(days as i32);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "as_of_to" {
            let mut builder = Date32Array::builder(1);
            let days = (record.as_of_to - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
            builder.append_value(days as i32);
            columns.push(Arc::new(builder.finish()));
        } else {
            // Copy from updates
            let orig_array = updates.column_by_name(column_name).unwrap();
            let new_array = orig_array.slice(row_idx, 1);
            columns.push(new_array);
        }
    }
    
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
}

fn combine_record_batches(batches: Vec<RecordBatch>) -> PyResult<RecordBatch> {
    if batches.is_empty() {
        return Err(pyo3::exceptions::PyValueError::new_err("No batches to combine"));
    }
    
    let schema = batches[0].schema();
    let mut combined_columns: Vec<Vec<ArrayRef>> = vec![Vec::new(); schema.fields().len()];
    
    // Collect all arrays for each column
    for batch in &batches {
        for (i, column) in batch.columns().iter().enumerate() {
            combined_columns[i].push(column.clone());
        }
    }
    
    // Concatenate arrays for each column
    let mut final_columns: Vec<ArrayRef> = Vec::new();
    for (i, arrays) in combined_columns.iter().enumerate() {
        let field = schema.field(i);
        match field.data_type() {
            DataType::Date32 => {
                let arrays: Vec<&Date32Array> = arrays.iter()
                    .map(|a| a.as_any().downcast_ref::<Date32Array>().unwrap())
                    .collect();
                let concatenated = arrow::compute::concat(&arrays.iter().map(|a| *a as &dyn Array).collect::<Vec<_>>())
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                final_columns.push(concatenated);
            }
            DataType::Utf8 => {
                let arrays: Vec<&StringArray> = arrays.iter()
                    .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap())
                    .collect();
                let concatenated = arrow::compute::concat(&arrays.iter().map(|a| *a as &dyn Array).collect::<Vec<_>>())
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                final_columns.push(concatenated);
            }
            DataType::Int32 => {
                let arrays: Vec<&Int32Array> = arrays.iter()
                    .map(|a| a.as_any().downcast_ref::<Int32Array>().unwrap())
                    .collect();
                let concatenated = arrow::compute::concat(&arrays.iter().map(|a| *a as &dyn Array).collect::<Vec<_>>())
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                final_columns.push(concatenated);
            }
            DataType::Float64 => {
                let arrays: Vec<&Float64Array> = arrays.iter()
                    .map(|a| a.as_any().downcast_ref::<Float64Array>().unwrap())
                    .collect();
                let concatenated = arrow::compute::concat(&arrays.iter().map(|a| *a as &dyn Array).collect::<Vec<_>>())
                    .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
                final_columns.push(concatenated);
            }
            _ => return Err(pyo3::exceptions::PyValueError::new_err(
                format!("Unsupported data type for concatenation: {:?}", field.data_type())
            )),
        }
    }
    
    RecordBatch::try_new(schema.clone(), final_columns)
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
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
    )?;
    
    // Convert the result back to Python types
    let expire_indices = changeset.to_expire;
    let insert_batches: Vec<PyRecordBatch> = changeset.to_insert
        .into_iter()
        .map(|batch| PyRecordBatch::new(batch))
        .collect();
    
    Ok((expire_indices, insert_batches))
}

#[pymodule]
fn bitemporal_timeseries(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compute_changes, m)?)?;
    Ok(())
}
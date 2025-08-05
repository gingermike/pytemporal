use arrow::array::{
    Array, ArrayRef, Date32Array, TimestampMicrosecondArray, RecordBatch, StringArray, 
    Int32Array, Int64Array, Float64Array, StringBuilder
};
use arrow::datatypes::TimeUnit;
use arrow::datatypes::{DataType, Field, Schema};
use chrono::{NaiveDate, NaiveDateTime, Days};
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use xxhash_rust::xxh64::xxh64;
use ordered_float;
use rayon::prelude::*;

#[derive(Debug, Clone)]
struct BitemporalRecord {
    id_values: Vec<ScalarValue>,
    value_hash: u64,
    effective_from: NaiveDateTime,
    effective_to: NaiveDateTime,
    as_of_from: NaiveDateTime,
    as_of_to: NaiveDateTime,  // No longer optional - use MAX_TIMESTAMP for infinity
    original_index: Option<usize>,
}

// Pandas-compatible max datetime (pandas can't handle dates beyond ~2262)
const MAX_DATETIME: NaiveDateTime = match NaiveDate::from_ymd_opt(2262, 4, 11) {
    Some(date) => match date.and_hms_opt(23, 59, 59) {
        Some(datetime) => datetime,
        None => panic!("Invalid max time"),
    },
    None => panic!("Invalid max date"),
};

// Max timestamp for as_of columns (microsecond precision)
const MAX_TIMESTAMP: NaiveDateTime = match NaiveDate::from_ymd_opt(2262, 4, 11) {
    Some(date) => match date.and_hms_opt(23, 59, 59) {
        Some(datetime) => datetime,
        None => panic!("Invalid max timestamp"),
    },
    None => panic!("Invalid max date"),
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UpdateMode {
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
            DataType::Timestamp(_, _) => {
                let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                ScalarValue::Int64(arr.value(idx))
            }
            _ => panic!("Unsupported data type: {:?}", array.data_type()),
        }
    }
}

#[derive(Debug)]
pub struct ChangeSet {
    pub to_expire: Vec<usize>,  // Indices of rows to mark as expired
    pub to_insert: Vec<RecordBatch>,  // New rows to insert
}

fn extract_date_as_datetime(array: &TimestampMicrosecondArray, idx: usize) -> NaiveDateTime {
    let microseconds_since_epoch = array.value(idx);
    let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
    epoch + chrono::Duration::microseconds(microseconds_since_epoch)
}

fn extract_timestamp(array: &TimestampMicrosecondArray, idx: usize) -> NaiveDateTime {
    let microseconds_since_epoch = array.value(idx);
    let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
    epoch + chrono::Duration::microseconds(microseconds_since_epoch)
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
            .map(|(_id_values, (mut current_records, mut update_records))| {
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
        
        // Insert each update record exactly as provided
        for update_record in &update_records {
            let batch = create_record_batch_from_update(
                updates,
                update_record.original_index.unwrap(),
                update_record,
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

#[derive(Debug, Clone)]
struct TimelineEvent {
    date: NaiveDateTime,
    event_type: EventType,
    record: BitemporalRecord,
}

#[derive(Debug, Clone, PartialEq)]
enum EventType {
    CurrentStart,
    CurrentEnd,
    UpdateStart,
    UpdateEnd,
}

fn process_id_timeline(
    current_records: &[BitemporalRecord],
    update_records: &[BitemporalRecord],
    current_batch: &RecordBatch,
    updates_batch: &RecordBatch,
    id_columns: &[String],
    value_columns: &[String],
    system_date: NaiveDate,
) -> Result<(Vec<usize>, Vec<RecordBatch>), String> {
    let mut expire_indices = Vec::new();
    let mut insert_batches = Vec::new();
    
    // Separate overlapping and non-overlapping updates
    let mut overlapping_current = Vec::new();
    let mut overlapping_updates = Vec::new();
    let mut non_overlapping_updates = Vec::new();
    
    // Check each update for temporal overlap with current records
    for update_record in update_records {
        let has_overlap = current_records.iter().any(|current_record| {
            // Standard overlap check
            let standard_overlap = current_record.effective_from < update_record.effective_to &&
                                 current_record.effective_to > update_record.effective_from;
            
            // Extension/conflation check: adjacent periods with same values
            let is_extension = current_record.effective_to == update_record.effective_from &&
                             current_record.value_hash == update_record.value_hash;
            let is_reverse_extension = update_record.effective_to == current_record.effective_from &&
                                     current_record.value_hash == update_record.value_hash;
            
            standard_overlap || is_extension || is_reverse_extension
        });
        
        if has_overlap {
            overlapping_updates.push(update_record);
        } else {
            non_overlapping_updates.push(update_record);
        }
    }
    
    // Find current records that overlap with any update
    for current_record in current_records {
        let has_overlap = update_records.iter().any(|update_record| {
            // Standard overlap check
            let standard_overlap = current_record.effective_from < update_record.effective_to &&
                                 current_record.effective_to > update_record.effective_from;
            
            // Extension/conflation check: adjacent periods with same values
            let is_extension = current_record.effective_to == update_record.effective_from &&
                             current_record.value_hash == update_record.value_hash;
            let is_reverse_extension = update_record.effective_to == current_record.effective_from &&
                                     current_record.value_hash == update_record.value_hash;
            
            standard_overlap || is_extension || is_reverse_extension
        });
        
        if has_overlap {
            overlapping_current.push(current_record);
        }
    }
    
    // Process non-overlapping updates directly
    for update_record in non_overlapping_updates {
        let batch = create_record_batch_from_update(
            updates_batch,
            update_record.original_index.unwrap(),
            update_record,
        )?;
        insert_batches.push(batch);
    }
    
    // If no overlapping records, we're done
    if overlapping_current.is_empty() && overlapping_updates.is_empty() {
        return Ok((expire_indices, insert_batches));
    }
    
    // Get the update's as_of_from timestamp for re-emitted current state records
    let update_as_of_from = if !overlapping_updates.is_empty() {
        Some(overlapping_updates.first().unwrap().as_of_from)
    } else {
        None
    };
    
    // Create timeline events for overlapping current state and updates only
    let mut events = Vec::new();
    
    // Add current state events (only overlapping ones)
    for record in &overlapping_current {
        events.push(TimelineEvent {
            date: record.effective_from,
            event_type: EventType::CurrentStart,
            record: (*record).clone(),
        });
        if record.effective_to != MAX_DATETIME {
            events.push(TimelineEvent {
                date: record.effective_to,
                event_type: EventType::CurrentEnd,
                record: (*record).clone(),
            });
        }
    }
    
    // Add update events (only overlapping ones)
    for record in &overlapping_updates {
        events.push(TimelineEvent {
            date: record.effective_from,
            event_type: EventType::UpdateStart,
            record: (*record).clone(),
        });
        if record.effective_to != MAX_DATETIME {
            events.push(TimelineEvent {
                date: record.effective_to,
                event_type: EventType::UpdateEnd,
                record: (*record).clone(),
            });
        }
    }
    
    // Sort events chronologically, with specific ordering for same dates
    events.sort_by(|a, b| {
        match a.date.cmp(&b.date) {
            std::cmp::Ordering::Equal => {
                // For same date, process in order: CurrentEnd, UpdateStart, UpdateEnd, CurrentStart
                use EventType::*;
                let order = |t: &EventType| match t {
                    CurrentEnd => 0,
                    UpdateStart => 1,
                    UpdateEnd => 2,
                    CurrentStart => 3,
                };
                order(&a.event_type).cmp(&order(&b.event_type))
            }
            other => other,
        }
    });
    
    // Track active records at each point in time
    let mut active_current: Vec<&BitemporalRecord> = Vec::new();
    let mut active_updates: Vec<&BitemporalRecord> = Vec::new();
    
    let mut last_date = None;
    
    // Process events chronologically
    let mut i = 0;
    while i < events.len() {
        let current_date = events[i].date;
        
        // If we have a date gap and active state, emit a record for the gap
        if let Some(prev_date) = last_date {
            if prev_date < current_date && (!active_current.is_empty() || !active_updates.is_empty()) {
                emit_segment(
                    prev_date,
                    current_date,
                    &active_current,
                    &active_updates,
                    current_batch,
                    updates_batch,
                    id_columns,
                    value_columns,
                    system_date,
                    &mut expire_indices,
                    &mut insert_batches,
                    update_as_of_from,
                )?;
            }
        }
        
        // Process all events at this date
        while i < events.len() && events[i].date == current_date {
            let event = &events[i];
            match event.event_type {
                EventType::CurrentStart => {
                    active_current.push(&event.record);
                }
                EventType::CurrentEnd => {
                    active_current.retain(|r| r.effective_from != event.record.effective_from);
                }
                EventType::UpdateStart => {
                    active_updates.push(&event.record);
                }
                EventType::UpdateEnd => {
                    active_updates.retain(|r| r.effective_from != event.record.effective_from);
                }
            }
            i += 1;
        }
        
        last_date = Some(current_date);
        
        // Find next different date or end
        let mut next_date = MAX_DATETIME;
        if i < events.len() {
            next_date = events[i].date;
        }
        
        // Emit segment from current_date to next_date if we have active state
        if (!active_current.is_empty() || !active_updates.is_empty()) && next_date > current_date {
            emit_segment(
                current_date,
                next_date,
                &active_current,
                &active_updates,
                current_batch,
                updates_batch,
                id_columns,
                value_columns,
                system_date,
                &mut expire_indices,
                &mut insert_batches,
                update_as_of_from,
            )?;
        }
    }
    
    // Expire all current records that had overlaps (we already computed this)
    for current_record in &overlapping_current {
        if let Some(orig_idx) = current_record.original_index {
            expire_indices.push(orig_idx);
        }
    }
    
    Ok((expire_indices, insert_batches))
}

fn emit_segment(
    from_date: NaiveDateTime,
    to_date: NaiveDateTime,
    active_current: &[&BitemporalRecord],
    active_updates: &[&BitemporalRecord],
    current_batch: &RecordBatch,
    updates_batch: &RecordBatch,
    id_columns: &[String],
    value_columns: &[String],
    system_date: NaiveDate,
    _expire_indices: &mut Vec<usize>,
    insert_batches: &mut Vec<RecordBatch>,
    update_as_of_from: Option<NaiveDateTime>, // Timestamp from overlapping updates
) -> Result<(), String> {
    // Determine what record to emit
    let (record_to_emit, use_current_batch) = if let Some(update_record) = active_updates.first() {
        // Check if the update has different values than current state
        let should_emit_update = if let Some(current_record) = active_current.first() {
            // Only emit if values have actually changed
            update_record.value_hash != current_record.value_hash
        } else {
            // No current state, always emit the update
            true
        };
        
        if should_emit_update {
            (update_record, false) // Use updates batch
        } else {
            (active_current.first().unwrap(), true) // Use current batch
        }
    } else if let Some(current_record) = active_current.first() {
        (current_record, true) // Use current batch
    } else {
        return Ok(()); // Nothing to emit
    };

    // Create the segment record
    // When re-emitting current state due to overlapping updates, use the update's as_of_from
    let as_of_from = if use_current_batch && update_as_of_from.is_some() {
        // Current state being re-emitted due to overlapping update - use update's timestamp
        update_as_of_from.unwrap()
    } else {
        // Normal case - use the record's own timestamp
        record_to_emit.as_of_from
    };
    
    let segment_record = BitemporalRecord {
        id_values: record_to_emit.id_values.clone(),
        value_hash: record_to_emit.value_hash,
        effective_from: from_date,
        effective_to: to_date,
        as_of_from,
        as_of_to: MAX_TIMESTAMP,
        original_index: None,
    };

    // Check if we can conflate with the last inserted batch
    if let Some(last_batch) = insert_batches.last_mut() {
        if can_conflate_with_last_batch(last_batch, &segment_record)? {
            // Extend the last batch's effective_to instead of creating a new batch
            extend_batch_effective_to(last_batch, to_date)?;
            return Ok(());
        }
    }

    // Create new batch since we can't conflate
    let batch = if use_current_batch {
        create_record_batch_from_record(
            &segment_record,
            current_batch,
            record_to_emit.original_index.unwrap(),
            id_columns,
            value_columns,
        )?
    } else {
        create_record_batch_from_update(
            updates_batch,
            record_to_emit.original_index.unwrap(),
            &segment_record,
        )?
    };
    
    insert_batches.push(batch);
    
    Ok(())
}

fn can_conflate_with_last_batch(last_batch: &RecordBatch, new_record: &BitemporalRecord) -> Result<bool, String> {
    if last_batch.num_rows() != 1 {
        return Ok(false);
    }

    // Check if ID values match by comparing the ScalarValue arrays
    // Extract ID values from the last batch
    let mut last_id_values = Vec::new();
    let schema = last_batch.schema();
    for field in schema.fields() {
        let field_name = field.name();
        if !matches!(field_name.as_str(), "effective_from" | "effective_to" | "as_of_from" | "as_of_to" | "value_hash") {
            let array = last_batch.column_by_name(field_name).unwrap();
            let last_value = ScalarValue::from_array(array, 0);
            last_id_values.push(last_value);
        }
    }
    
    // Compare with new record's ID values
    if last_id_values != new_record.id_values {
        return Ok(false);
    }

    // Check if value hashes match
    let hash_array = last_batch.column_by_name("value_hash").unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap();
    let last_hash = hash_array.value(0) as u64;
    
    if last_hash != new_record.value_hash {
        return Ok(false);
    }

    // Check if they're adjacent (last batch's effective_to == new record's effective_from)
    let eff_to_array = last_batch.column_by_name("effective_to").unwrap()
        .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let last_effective_to = extract_date_as_datetime(eff_to_array, 0);
    
    Ok(last_effective_to == new_record.effective_from)
}

fn extend_batch_effective_to(batch: &mut RecordBatch, new_effective_to: NaiveDateTime) -> Result<(), String> {
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
    
    let new_batch = RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| e.to_string())?;
    
    *batch = new_batch;
    Ok(())
}

fn simple_conflate_batches(mut batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, String> {
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

fn create_record_batch_from_record(
    record: &BitemporalRecord, 
    original_batch: &RecordBatch,
    original_row: usize,
    _id_columns: &[String],
    _value_columns: &[String]
) -> Result<RecordBatch, String> {
    let schema = original_batch.schema();
    let mut columns: Vec<ArrayRef> = Vec::new();
    
    for field in schema.fields() {
        let column_name = field.name();
        
        if column_name == "effective_from" {
            let mut builder = TimestampMicrosecondArray::builder(1);
            let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
            let microseconds = (record.effective_from - epoch).num_microseconds().unwrap();
            builder.append_value(microseconds);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "effective_to" {
            let mut builder = TimestampMicrosecondArray::builder(1);
            let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
            let microseconds = (record.effective_to - epoch).num_microseconds().unwrap();
            builder.append_value(microseconds);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "as_of_from" {
            let mut builder = TimestampMicrosecondArray::builder(1);
            let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
            let microseconds = (record.as_of_from - epoch).num_microseconds().unwrap();
            builder.append_value(microseconds);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "as_of_to" {
            let mut builder = TimestampMicrosecondArray::builder(1);
            let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
            let microseconds = (record.as_of_to - epoch).num_microseconds().unwrap();
            builder.append_value(microseconds);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "value_hash" {
            let mut builder = Int64Array::builder(1);
            builder.append_value(record.value_hash as i64);
            columns.push(Arc::new(builder.finish()));
        } else {
            // Copy from original batch
            let orig_array = original_batch.column_by_name(column_name).unwrap();
            let new_array = orig_array.slice(original_row, 1);
            columns.push(new_array);
        }
    }
    
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| e.to_string())
}

fn create_record_batch_from_update(
    updates: &RecordBatch, 
    row_idx: usize, 
    record: &BitemporalRecord
) -> Result<RecordBatch, String> {
    let schema = updates.schema();
    let mut columns: Vec<ArrayRef> = Vec::new();
    
    for field in schema.fields() {
        let column_name = field.name();
        
        if column_name == "effective_from" {
            let mut builder = TimestampMicrosecondArray::builder(1);
            let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
            let microseconds = (record.effective_from - epoch).num_microseconds().unwrap();
            builder.append_value(microseconds);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "effective_to" {
            let mut builder = TimestampMicrosecondArray::builder(1);
            let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
            let microseconds = (record.effective_to - epoch).num_microseconds().unwrap();
            builder.append_value(microseconds);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "as_of_from" {
            let mut builder = TimestampMicrosecondArray::builder(1);
            let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
            let microseconds = (record.as_of_from - epoch).num_microseconds().unwrap();
            builder.append_value(microseconds);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "as_of_to" {
            let mut builder = TimestampMicrosecondArray::builder(1);
            let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
            let microseconds = (record.as_of_to - epoch).num_microseconds().unwrap();
            builder.append_value(microseconds);
            columns.push(Arc::new(builder.finish()));
        } else if column_name == "value_hash" {
            let mut builder = Int64Array::builder(1);
            builder.append_value(record.value_hash as i64);
            columns.push(Arc::new(builder.finish()));
        } else {
            // Copy from updates
            let orig_array = updates.column_by_name(column_name).unwrap();
            let new_array = orig_array.slice(row_idx, 1);
            columns.push(new_array);
        }
    }
    
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|e| e.to_string())
}

fn deduplicate_record_batches(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, String> {
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    
    // Convert RecordBatches to a more workable format for deduplication
    let mut records: Vec<(NaiveDateTime, NaiveDateTime, u64, RecordBatch)> = Vec::new();
    
    for batch in batches {
        if batch.num_rows() == 1 {
            let eff_from_array = batch.column_by_name("effective_from").unwrap()
                .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            let eff_to_array = batch.column_by_name("effective_to").unwrap()
                .as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            let hash_array = batch.column_by_name("value_hash").unwrap()
                .as_any().downcast_ref::<Int64Array>().unwrap();
            
            let eff_from = extract_date_as_datetime(eff_from_array, 0);
            let eff_to = extract_date_as_datetime(eff_to_array, 0);
            let hash = hash_array.value(0) as u64;
            
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
    let mut last_key: Option<(NaiveDateTime, NaiveDateTime, u64)> = None;
    
    for (eff_from, eff_to, hash, batch) in records {
        let current_key = (eff_from, eff_to, hash);
        if last_key != Some(current_key) {
            deduped.push(batch);
            last_key = Some(current_key);
        }
    }
    
    Ok(deduped)
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

#[pymodule]
fn bitemporal_timeseries(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compute_changes, m)?)?;
    Ok(())
}
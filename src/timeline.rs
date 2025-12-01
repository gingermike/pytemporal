use crate::types::*;
use crate::overlap::*;
use arrow::array::RecordBatch;
use chrono::NaiveDate;

pub fn process_id_timeline(
    current_records: &[BitemporalRecord],
    update_records: &[BitemporalRecord],
    current_batch: &RecordBatch,
    updates_batch: &RecordBatch,
    id_columns: &[String],
    value_columns: &[String],
    system_date: NaiveDate,
) -> Result<(Vec<usize>, Vec<RecordBatch>), String> {
    let mut expire_indices = Vec::new();
    
    // Categorize records based on overlap relationships
    let (overlapping_current, overlapping_updates, non_overlapping_updates) = 
        categorize_records(current_records, update_records);
    
    // Process non-overlapping updates directly
    let mut insert_batches = process_non_overlapping_updates(&non_overlapping_updates, updates_batch)?;
    
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

#[allow(clippy::too_many_arguments)]
pub fn emit_segment(
    from_date: chrono::NaiveDateTime,
    to_date: chrono::NaiveDateTime,
    active_current: &[&BitemporalRecord],
    active_updates: &[&BitemporalRecord],
    current_batch: &RecordBatch,
    updates_batch: &RecordBatch,
    id_columns: &[String],
    value_columns: &[String],
    _system_date: NaiveDate,
    _expire_indices: &mut [usize],
    insert_batches: &mut Vec<RecordBatch>,
    update_as_of_from: Option<chrono::NaiveDateTime>,
) -> Result<(), String> {
    // Skip empty ranges (from_date == to_date)
    // These represent zero-width time periods and are invalid
    if from_date >= to_date {
        return Ok(());
    }

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
    let as_of_from = if let (true, Some(timestamp)) = (use_current_batch, update_as_of_from) {
        // Current state being re-emitted due to overlapping update - use update's timestamp
        timestamp
    } else {
        // Normal case - use the record's own timestamp
        record_to_emit.as_of_from
    };
    
    let segment_record = BitemporalRecord {
        id_values: record_to_emit.id_values.clone(),
        value_hash: record_to_emit.value_hash.clone(),
        effective_from: from_date,
        effective_to: to_date,
        as_of_from,
        as_of_to: MAX_TIMESTAMP,
        original_index: None,
    };

    // Create new batch since segments require synthetic records
    let batch = if use_current_batch {
        crate::batch_utils::create_record_batch_from_record(
            &segment_record,
            current_batch,
            record_to_emit.original_index.unwrap(),
            id_columns,
            value_columns,
        )?
    } else {
        crate::batch_utils::create_record_batch_from_update(
            updates_batch,
            record_to_emit.original_index.unwrap(),
            &segment_record,
        )?
    };
    
    insert_batches.push(batch);
    
    Ok(())
}
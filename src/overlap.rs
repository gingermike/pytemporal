use crate::types::*;
use arrow::array::RecordBatch;

/// Determines if two records have any temporal intersection
pub fn has_temporal_intersection(current: &BitemporalRecord, update: &BitemporalRecord) -> bool {
    current.effective_from < update.effective_to && current.effective_to > update.effective_from
}

/// Determines if two records are adjacent in time with the same values (for conflation)
pub fn can_conflate_records(current: &BitemporalRecord, update: &BitemporalRecord) -> bool {
    let same_values = current.value_hash == update.value_hash;
    let is_extension = current.effective_to == update.effective_from;
    let is_reverse_extension = update.effective_to == current.effective_from;
    
    same_values && (is_extension || is_reverse_extension)
}

/// Determines if an update represents a no-change scenario (intersects with same values)
pub fn is_no_change_update(current_records: &[BitemporalRecord], update: &BitemporalRecord) -> bool {
    current_records.iter().any(|current| {
        has_temporal_intersection(current, update) && current.value_hash == update.value_hash
    })
}

/// Determines if an update overlaps with any current record (intersection or conflation)
pub fn has_overlap_with_current(current_records: &[BitemporalRecord], update: &BitemporalRecord) -> bool {
    current_records.iter().any(|current| {
        has_temporal_intersection(current, update) || can_conflate_records(current, update)
    })
}

/// Determines if a current record overlaps with any update (intersection or conflation) 
pub fn has_overlap_with_updates(updates: &[&BitemporalRecord], current: &BitemporalRecord) -> bool {
    updates.iter().any(|update| {
        has_temporal_intersection(current, update) || can_conflate_records(current, update)
    })
}

/// Processes non-overlapping updates by creating record batches directly
pub fn process_non_overlapping_updates(
    updates: &[&BitemporalRecord],
    updates_batch: &RecordBatch,
) -> Result<Vec<RecordBatch>, String> {
    let mut insert_batches = Vec::new();
    
    for update_record in updates {
        let batch = crate::batch_utils::create_record_batch_from_update(
            updates_batch,
            update_record.original_index.unwrap(),
            update_record,
        )?;
        insert_batches.push(batch);
    }
    
    Ok(insert_batches)
}

/// Categorizes updates and current records based on overlap relationships
pub fn categorize_records<'a>(
    current_records: &'a [BitemporalRecord],
    update_records: &'a [BitemporalRecord],
) -> (Vec<&'a BitemporalRecord>, Vec<&'a BitemporalRecord>, Vec<&'a BitemporalRecord>) {
    let mut overlapping_current = Vec::new();
    let mut overlapping_updates = Vec::new();
    let mut non_overlapping_updates = Vec::new();
    
    // Filter and categorize updates
    for update_record in update_records {
        if is_no_change_update(current_records, update_record) {
            continue; // Skip no-change updates
        }
        
        if has_overlap_with_current(current_records, update_record) {
            overlapping_updates.push(update_record);
        } else {
            non_overlapping_updates.push(update_record);
        }
    }
    
    // Find overlapping current records
    let all_remaining_updates: Vec<&BitemporalRecord> = overlapping_updates.iter()
        .chain(non_overlapping_updates.iter())
        .copied()
        .collect();
    
    for current_record in current_records {
        if has_overlap_with_updates(&all_remaining_updates, current_record) {
            overlapping_current.push(current_record);
        }
    }
    
    (overlapping_current, overlapping_updates, non_overlapping_updates)
}
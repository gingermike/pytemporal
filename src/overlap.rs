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

/// Determines if an update overlaps with any current record.
///
/// Considers both temporal intersection AND adjacency (for extension/conflation).
/// However, adjacency is only considered if there are no temporal intersections,
/// to prevent pulling in unrelated adjacent records during backfill scenarios.
pub fn has_overlap_with_current(current_records: &[BitemporalRecord], update: &BitemporalRecord) -> bool {
    // First check for any temporal intersection
    let has_any_intersection = current_records.iter().any(|current| {
        has_temporal_intersection(current, update)
    });

    if has_any_intersection {
        // Update intersects with at least one current record - that's overlap
        return true;
    }

    // No intersection - check for adjacency (extension scenario)
    // This only triggers when the update doesn't intersect with ANY current record
    current_records.iter().any(|current| {
        can_conflate_records(current, update)
    })
}

/// Determines if a current record overlaps with any update.
///
/// A current record is considered overlapping if:
/// 1. It has temporal intersection with any update, OR
/// 2. It can conflate with an update that has NO temporal intersection with ANY current record
///    (i.e., a pure extension scenario)
///
/// This prevents the backfill bug where an update that intersects with one current record
/// would incorrectly pull in an adjacent (but non-overlapping) current record just because
/// they share the same value hash.
pub fn has_overlap_with_updates_contextual(
    updates: &[&BitemporalRecord],
    current: &BitemporalRecord,
    all_current_records: &[BitemporalRecord],
) -> bool {
    updates.iter().any(|update| {
        // Always include if there's temporal intersection
        if has_temporal_intersection(current, update) {
            return true;
        }

        // For adjacency, only consider if this update has NO intersection with ANY current record
        // This is the "pure extension" case where we want merging behavior
        let update_has_any_intersection = all_current_records.iter().any(|c| {
            has_temporal_intersection(c, update)
        });

        if !update_has_any_intersection {
            // Pure extension: update is adjacent but doesn't intersect anything
            // Allow conflation in this case
            return can_conflate_records(current, update);
        }

        // Update intersects with some other current record - don't pull in adjacent records
        false
    })
}


/// Processes non-overlapping updates by creating record batches directly
pub fn process_non_overlapping_updates(
    updates: &[&BitemporalRecord],
    updates_batch: &RecordBatch,
) -> Result<Vec<RecordBatch>, String> {
    if updates.is_empty() {
        return Ok(Vec::new());
    }
    
    // Collect all update records and their source rows
    let records: Vec<BitemporalRecord> = updates.iter().map(|&r| (*r).clone()).collect();
    let source_rows: Vec<usize> = updates.iter().map(|r| r.original_index.unwrap()).collect();
    
    // Create a single batch from all non-overlapping updates
    let batch = crate::batch_utils::create_record_batch_from_records(
        &records,
        updates_batch,
        &source_rows,
    )?;
    
    Ok(vec![batch])
}

/// Categorizes updates and current records based on overlap relationships.
///
/// This function uses context-aware overlap detection to correctly handle:
/// - **Extension scenario**: Single current record + adjacent update → merge (overlapping)
/// - **Backfill scenario**: Multiple current records + update that intersects one →
///   only that one is overlapping, not adjacent ones with same values
pub fn categorize_records<'a>(
    current_records: &'a [BitemporalRecord],
    update_records: &'a [BitemporalRecord],
) -> (Vec<&'a BitemporalRecord>, Vec<&'a BitemporalRecord>, Vec<&'a BitemporalRecord>) {
    let mut overlapping_current = Vec::new();
    let mut overlapping_updates = Vec::new();
    let mut non_overlapping_updates = Vec::new();

    // Filter and categorize updates
    for update_record in update_records {
        // Skip empty ranges (effective_from >= effective_to)
        // These represent zero-width time periods and are invalid
        if update_record.effective_from >= update_record.effective_to {
            continue;
        }

        if is_no_change_update(current_records, update_record) {
            continue; // Skip no-change updates
        }

        if has_overlap_with_current(current_records, update_record) {
            overlapping_updates.push(update_record);
        } else {
            non_overlapping_updates.push(update_record);
        }
    }

    // Find overlapping current records using context-aware detection
    let all_remaining_updates: Vec<&BitemporalRecord> = overlapping_updates.iter()
        .chain(non_overlapping_updates.iter())
        .copied()
        .collect();

    for current_record in current_records {
        // Use contextual overlap detection to prevent backfill bug
        if has_overlap_with_updates_contextual(&all_remaining_updates, current_record, current_records) {
            overlapping_current.push(current_record);
        }
    }

    (overlapping_current, overlapping_updates, non_overlapping_updates)
}
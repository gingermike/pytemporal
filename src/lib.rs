use arrow::array::{RecordBatch};
use chrono::{NaiveDate, NaiveDateTime};
use pyo3::prelude::*;
use pyo3_arrow::PyRecordBatch;
use rustc_hash::FxHashMap;
use arrow::array::Array;
use rayon::prelude::*;

mod types;
mod overlap;
mod timeline;
mod conflation;
mod batch_utils;
mod arrow_hash;

/// Hash algorithm options for value hash computation
#[derive(Debug, Clone, Copy, PartialEq)]
#[derive(Default)]
pub enum HashAlgorithm {
    #[default]
    XxHash,  // Default - fast, high quality
    Sha256,  // Legacy compatibility
}

impl HashAlgorithm {
    fn from_str(s: &str) -> Result<HashAlgorithm, String> {
        match s.to_lowercase().as_str() {
            "xxhash" | "xx" => Ok(HashAlgorithm::XxHash),
            "sha256" | "sha" => Ok(HashAlgorithm::Sha256),
            _ => Err(format!("Unknown hash algorithm: {}", s)),
        }
    }
}


pub use types::*;
use timeline::process_id_timeline;
use conflation::{deduplicate_record_batches, simple_conflate_batches, consolidate_final_batches, conflate_input_updates};

/// Type alias for processing results from ID groups
type IdGroupProcessingResult = (Vec<usize>, Vec<RecordBatch>);



pub fn process_updates(
    current_state: RecordBatch,
    updates: RecordBatch,
    id_columns: Vec<String>,
    value_columns: Vec<String>,
    system_date: NaiveDate,
    update_mode: UpdateMode,
    conflate_inputs: bool,
) -> Result<ChangeSet, String> {
    process_updates_with_algorithm(current_state, updates, id_columns, value_columns, system_date, update_mode, HashAlgorithm::default(), conflate_inputs)
}

pub fn process_updates_with_algorithm(
    current_state: RecordBatch,
    updates: RecordBatch,
    id_columns: Vec<String>,
    value_columns: Vec<String>,
    system_date: NaiveDate,
    update_mode: UpdateMode,
    algorithm: HashAlgorithm,
    conflate_inputs: bool,
) -> Result<ChangeSet, String> {
    let start_time = std::time::Instant::now();

    // Phase 0: Input validation and preprocessing
    let (current_state, updates, batch_timestamp) = prepare_inputs(
        current_state, updates, &value_columns, algorithm, &id_columns, conflate_inputs
    )?;
    
    // Handle quick paths for empty inputs
    if let Some(changeset) = handle_empty_inputs(
        &current_state, &updates, &value_columns, system_date, update_mode, batch_timestamp
    )? {
        return Ok(changeset);
    }
    
    // Phase 1: ID Grouping with performance optimizations
    let phase1_start = std::time::Instant::now();
    let id_groups = build_id_groups(&current_state, &updates, &id_columns)?;
    let _phase1_total = phase1_start.elapsed();
    
    // Phase 2: Process ID groups with optimized parallel/serial strategy
    let phase2_start = std::time::Instant::now();
    let (to_expire, to_insert) = process_all_id_groups(
        id_groups, &current_state, &updates, &id_columns, &value_columns,
        system_date, update_mode, batch_timestamp
    )?;
    let _phase2_total = phase2_start.elapsed();
    
    // Phase 3: Post-processing and changeset building
    let phase3_start = std::time::Instant::now();
    let changeset = build_final_changeset(
        to_expire, to_insert, &current_state, batch_timestamp
    )?;
    let _phase3_total = phase3_start.elapsed();
    
    let _total_time = start_time.elapsed();

    Ok(changeset)
}

/// Prepare inputs by ensuring hash columns exist and generating batch timestamp
fn prepare_inputs(
    current_state: RecordBatch,
    updates: RecordBatch,
    value_columns: &[String],
    algorithm: HashAlgorithm,
    id_columns: &[String],
    conflate_inputs: bool,
) -> Result<(RecordBatch, RecordBatch, chrono::NaiveDateTime), String> {
    // Ensure value_hash columns are computed if missing or empty
    let current_state = ensure_hash_column_with_algorithm(current_state, value_columns, algorithm)?;
    let mut updates = ensure_hash_column_with_algorithm(updates, value_columns, algorithm)?;

    // Optionally conflate consecutive input updates with same ID and value hash
    if conflate_inputs && updates.num_rows() > 1 {
        updates = conflate_input_updates(updates, id_columns)?;
    }

    // Generate consistent timestamp for all operations in this batch
    let batch_timestamp = chrono::Utc::now().naive_utc();

    Ok((current_state, updates, batch_timestamp))
}

/// Handle quick paths for empty input cases
fn handle_empty_inputs(
    current_state: &RecordBatch,
    updates: &RecordBatch,
    value_columns: &[String],
    system_date: NaiveDate,
    update_mode: UpdateMode,
    batch_timestamp: chrono::NaiveDateTime,
) -> Result<Option<ChangeSet>, String> {
    // No updates - handle based on mode
    if updates.num_rows() == 0 {
        return if update_mode == UpdateMode::FullState && current_state.num_rows() > 0 {
            // Create tombstones for all current records in full state mode
            let current_indices: Vec<usize> = (0..current_state.num_rows()).collect();
            let tombstone_batch = create_tombstone_records_optimized(
                &current_indices,
                current_state,
                value_columns,
                system_date,
                batch_timestamp,
            )?;
            
            let expired_batch = crate::batch_utils::create_expired_records_batch(
                current_state, 
                &current_indices, 
                batch_timestamp
            )?;
            
            Ok(Some(ChangeSet {
                to_expire: current_indices,
                to_insert: vec![tombstone_batch],
                expired_records: vec![expired_batch],
            }))
        } else {
            Ok(Some(ChangeSet {
                to_expire: Vec::new(),
                to_insert: Vec::new(),
                expired_records: Vec::new(),
            }))
        };
    }
    
    // No current state - all updates become inserts
    if current_state.num_rows() == 0 {
        return Ok(Some(ChangeSet {
            to_expire: Vec::new(),
            to_insert: vec![updates.clone()],
            expired_records: Vec::new(),
        }));
    }
    
    // Continue with normal processing
    Ok(None)
}

/// Build ID groups using optimized direct array access for performance
/// PERFORMANCE: Inlined to allow optimizer to see through to hot loops
#[inline]
fn build_id_groups(
    current_state: &RecordBatch,
    updates: &RecordBatch,
    id_columns: &[String],
) -> Result<FxHashMap<String, (Vec<usize>, Vec<usize>)>, String> {
    // Pre-size FxHashMap with estimated capacity for better performance
    // Estimate: Most datasets have 10-50% unique ID combinations
    let estimated_unique_ids = ((current_state.num_rows() + updates.num_rows()) / 3).max(16);
    let mut id_groups: FxHashMap<String, (Vec<usize>, Vec<usize>)> = 
        FxHashMap::with_capacity_and_hasher(estimated_unique_ids, Default::default());
    
    // Extract ID column arrays once for efficiency
    let current_id_arrays: Vec<_> = id_columns.iter()
        .map(|col| current_state.column_by_name(col).unwrap().clone())
        .collect();
    let updates_id_arrays: Vec<_> = id_columns.iter()
        .map(|col| updates.column_by_name(col).unwrap().clone())
        .collect();
    
    // PERFORMANCE OPTIMIZATION: Reusable buffer to avoid 850,000+ String allocations
    let mut id_key_buffer = String::with_capacity(64);
    
    // Group current state rows by ID key
    for row_idx in 0..current_state.num_rows() {
        create_id_key_with_buffer(&current_id_arrays, row_idx, &mut id_key_buffer);
        let id_key = id_key_buffer.clone(); // TODO: Could optimize further with string interning
        id_groups.entry(id_key).or_insert((Vec::new(), Vec::new())).0.push(row_idx);
    }
    
    // Group update rows by ID key  
    for row_idx in 0..updates.num_rows() {
        create_id_key_with_buffer(&updates_id_arrays, row_idx, &mut id_key_buffer);
        let id_key = id_key_buffer.clone(); // TODO: Could optimize further with string interning
        id_groups.entry(id_key).or_insert((Vec::new(), Vec::new())).1.push(row_idx);
    }
    
    Ok(id_groups)
}

/// Process all ID groups with optimal parallel/serial strategy
#[allow(clippy::too_many_arguments)]
fn process_all_id_groups(
    id_groups: FxHashMap<String, (Vec<usize>, Vec<usize>)>,
    current_state: &RecordBatch,
    updates: &RecordBatch,
    id_columns: &[String],
    value_columns: &[String],
    system_date: NaiveDate,
    update_mode: UpdateMode,
    batch_timestamp: chrono::NaiveDateTime,
) -> Result<(Vec<usize>, Vec<RecordBatch>), String> {
    // Pre-allocate vectors with estimated capacity to reduce reallocations
    // Estimate: on average, each ID group affects 1-2 current state records and creates 1-3 insert batches
    let estimated_expire_capacity = id_groups.len() * 2;
    let estimated_insert_capacity = id_groups.len() * 3;
    
    let mut to_expire = Vec::with_capacity(estimated_expire_capacity);
    let mut to_insert = Vec::with_capacity(estimated_insert_capacity);
    
    // PERFORMANCE OPTIMIZATION: Pre-extract array to avoid 5000+ column_by_name calls
    let updates_as_of_from_array = updates.column_by_name("as_of_from")
        .ok_or_else(|| "as_of_from column not found in updates".to_string())?;
    
    // Determine optimal processing strategy based on data size
    // PERFORMANCE TUNING: More aggressive parallelization for modern multi-core systems
    let use_parallel = id_groups.len() > 25 ||
                      (current_state.num_rows() + updates.num_rows()) > 5000;
    
    if use_parallel {
        // Parallel processing for large datasets
        let results: Result<Vec<IdGroupProcessingResult>, String> = id_groups
            .into_par_iter()
            .map(|(_id_key, (current_row_indices, update_row_indices))| {
                process_id_group_optimized(
                    &current_row_indices,
                    &update_row_indices,
                    current_state,
                    updates,
                    &updates_as_of_from_array,
                    id_columns,
                    value_columns,
                    system_date,
                    update_mode,
                    batch_timestamp,
                )
            })
            .collect();
        
        let results = results?;
        for (expire_indices, insert_batches) in results {
            to_expire.extend(expire_indices);
            to_insert.extend(insert_batches);
            
            // MEMORY OPTIMIZATION: Incremental consolidation to prevent memory buildup
            // Apply deduplication + consolidation when we have too many small batches
            if to_insert.len() > 200 {
                to_insert = crate::conflation::deduplicate_record_batches(to_insert)?;
                to_insert = crate::conflation::consolidate_final_batches(to_insert)?;
            }
        }
    } else {
        // Serial processing for small datasets (avoids parallel overhead)
        for (_id_key, (current_row_indices, update_row_indices)) in id_groups {
            let (expire_indices, insert_batches) = process_id_group_optimized(
                &current_row_indices,
                &update_row_indices,
                current_state,
                updates,
                &updates_as_of_from_array,
                id_columns,
                value_columns,
                system_date,
                update_mode,
                batch_timestamp,
            )?;
            
            to_expire.extend(expire_indices);
            to_insert.extend(insert_batches);
            
            // MEMORY OPTIMIZATION: Incremental consolidation to prevent memory buildup
            // Apply deduplication + consolidation when we have too many small batches
            if to_insert.len() > 200 {
                to_insert = crate::conflation::deduplicate_record_batches(to_insert)?;
                to_insert = crate::conflation::consolidate_final_batches(to_insert)?;
            }
        }
    }
    
    Ok((to_expire, to_insert))
}

/// Build final changeset with all post-processing optimizations
fn build_final_changeset(
    mut to_expire: Vec<usize>,
    mut to_insert: Vec<RecordBatch>,
    current_state: &RecordBatch,
    batch_timestamp: chrono::NaiveDateTime,
) -> Result<ChangeSet, String> {
    // Sort and deduplicate expiry indices
    to_expire.sort_unstable();
    to_expire.dedup();
    
    // Apply all post-processing optimizations to insert batches
    to_insert = deduplicate_record_batches(to_insert)?;
    to_insert = simple_conflate_batches(to_insert)?;
    to_insert = consolidate_final_batches(to_insert)?;
    
    // Create expired record batches with updated as_of_to timestamp
    let expired_records = if !to_expire.is_empty() {
        vec![crate::batch_utils::create_expired_records_batch(current_state, &to_expire, batch_timestamp)?]
    } else {
        Vec::new()
    };
    
    Ok(ChangeSet { to_expire, to_insert, expired_records })
}

/// Ensures the value_hash column exists and is computed if missing or empty using fast Arrow-direct hashing
fn ensure_hash_column_with_algorithm(batch: RecordBatch, value_columns: &[String], algorithm: HashAlgorithm) -> Result<RecordBatch, String> {
    // Handle empty batches - no need to compute hashes
    if batch.num_rows() == 0 {
        return Ok(batch);
    }
    
    // Check if value_hash column exists and has non-empty values
    if let Some(hash_column) = batch.column_by_name("value_hash") {
        if let Some(string_array) = hash_column.as_any().downcast_ref::<arrow::array::StringArray>() {
            // Check if all values are non-empty
            let all_non_empty = (0..string_array.len())
                .all(|i| !string_array.is_null(i) && !string_array.value(i).is_empty());
            
            if all_non_empty {
                // Hash column exists and is populated, return as-is
                return Ok(batch);
            }
        }
    }
    
    // Hash column is missing or has empty values, compute it using fast Arrow-direct hashing
    crate::arrow_hash::add_hash_column_arrow_direct(&batch, value_columns, algorithm)
}

// Extract ID group processing logic for reuse in parallel and serial paths

/// Optimized ID group processing that works with row indices instead of expensive structures
/// PERFORMANCE: Inline hint for warm path (called once per ID group, ~5000 times)
#[allow(clippy::too_many_arguments)]
#[inline]
fn process_id_group_optimized(
    current_row_indices: &[usize],
    update_row_indices: &[usize],
    current_batch: &RecordBatch,
    updates_batch: &RecordBatch,
    updates_as_of_from_array: &arrow::array::ArrayRef,
    id_columns: &[String],
    value_columns: &[String],
    system_date: NaiveDate,
    update_mode: UpdateMode,
    batch_timestamp: chrono::NaiveDateTime,
) -> Result<(Vec<usize>, Vec<RecordBatch>), String> {
    let mut expire_indices = Vec::new();
    let mut insert_batches = Vec::new();
    
    // Extract consistent as_of_from timestamp from updates batch (if available)
    let consistent_timestamp = if updates_batch.num_rows() > 0 {
        // PERFORMANCE: Use pre-extracted array to avoid repeated column_by_name calls
        if let Some(ts_array) = updates_as_of_from_array.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>() {
            if !ts_array.is_null(0) {
                let micros = ts_array.value(0);
                let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
                epoch + chrono::Duration::microseconds(micros)
            } else {
                batch_timestamp
            }
        } else {
            batch_timestamp
        }
    } else {
        batch_timestamp
    };

    // Quick path: No updates for this ID group
    if update_row_indices.is_empty() {
        if update_mode == UpdateMode::FullState {
            // In full state mode, expire all current records for IDs not in updates
            expire_indices.extend(current_row_indices.iter().cloned());
            
            // Create tombstone records - but only convert to BitemporalRecord when needed
            if !current_row_indices.is_empty() {
                // Use the consistent timestamp from the updates batch for tombstones
                let tombstone_records = create_tombstone_records_optimized(
                    current_row_indices,
                    current_batch,
                    value_columns,
                    system_date,
                    consistent_timestamp,
                )?;
                insert_batches.push(tombstone_records);
            }
        }
        return Ok((expire_indices, insert_batches));
    }
    
    // Only create expensive BitemporalRecord structures when we actually need temporal processing
    if update_mode == UpdateMode::FullState {
        // For full state mode, we need to compare values - but we can do this more efficiently
        process_full_state_optimized(
            current_row_indices,
            update_row_indices,
            current_batch,
            updates_batch,
            value_columns,
            system_date,
            consistent_timestamp,
            &mut expire_indices,
            &mut insert_batches,
        )?;
    } else {
        // For delta mode, we need temporal processing - create BitemporalRecords only here
        let current_records = create_bitemporal_records_from_indices(
            current_row_indices,
            current_batch,
            id_columns,
            value_columns,
        )?;
        let update_records = create_bitemporal_records_from_indices(
            update_row_indices,
            updates_batch,
            id_columns,
            value_columns,
        )?;
        
        let (expire_idx, insert_batch) = process_id_timeline(
            &current_records,
            &update_records,
            current_batch,
            updates_batch,
            id_columns,
            value_columns,
            system_date,
        )?;
        
        expire_indices.extend(expire_idx);
        insert_batches.extend(insert_batch);
    }
    
    Ok((expire_indices, insert_batches))
}

/// Fast tombstone creation without expensive conversions
fn create_tombstone_records_optimized(
    current_row_indices: &[usize],
    current_batch: &RecordBatch,
    _value_columns: &[String],
    system_date: NaiveDate,
    batch_timestamp: chrono::NaiveDateTime,
) -> Result<RecordBatch, String> {
    // Create a slice of the current batch with only the relevant rows
    if current_row_indices.is_empty() {
        return Err("Cannot create tombstone records from empty indices".to_string());
    }
    
    // Use Arrow's take operation to efficiently extract rows
    let indices_array = arrow::array::UInt64Array::from(
        current_row_indices.iter().map(|&i| Some(i as u64)).collect::<Vec<_>>()
    );
    let sliced_batch = arrow::compute::take_record_batch(current_batch, &indices_array)
        .map_err(|e| format!("Failed to slice batch for tombstones: {}", e))?;
    
    // Modify the temporal columns for tombstone semantics
    let system_date_time = system_date.and_hms_opt(0, 0, 0).unwrap();
    
    // Clone the schema and data, but modify effective_to and as_of_from
    let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();
    let schema = sliced_batch.schema();
    
    for field in schema.fields() {
        let column_name = field.name();
        
        match column_name.as_str() {
            "effective_to" => {
                // Set effective_to to system_date for all tombstone records, preserving original time unit
                match field.data_type() {
                    arrow::datatypes::DataType::Timestamp(time_unit, tz) => {
                        let timezone_str = tz.as_ref().map(|t| t.to_string());
                        let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
                        
                        use arrow::datatypes::TimeUnit;
                        let array: arrow::array::ArrayRef = match time_unit {
                            TimeUnit::Nanosecond => {
                                let nanoseconds = (system_date_time - epoch).num_nanoseconds().unwrap();
                                let values = vec![Some(nanoseconds); current_row_indices.len()];
                                let array = arrow::array::TimestampNanosecondArray::from(values).with_timezone_opt(timezone_str);
                                std::sync::Arc::new(array)
                            }
                            TimeUnit::Microsecond => {
                                let microseconds = (system_date_time - epoch).num_microseconds().unwrap();
                                let values = vec![Some(microseconds); current_row_indices.len()];
                                let array = arrow::array::TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str);
                                std::sync::Arc::new(array)
                            }
                            TimeUnit::Millisecond => {
                                let milliseconds = (system_date_time - epoch).num_milliseconds();
                                let values = vec![Some(milliseconds); current_row_indices.len()];
                                let array = arrow::array::TimestampMillisecondArray::from(values).with_timezone_opt(timezone_str);
                                std::sync::Arc::new(array)
                            }
                            TimeUnit::Second => {
                                let seconds = (system_date_time - epoch).num_seconds();
                                let values = vec![Some(seconds); current_row_indices.len()];
                                let array = arrow::array::TimestampSecondArray::from(values).with_timezone_opt(timezone_str);
                                std::sync::Arc::new(array)
                            }
                        };
                        columns.push(array);
                    }
                    _ => return Err("effective_to column must be timestamp type".to_string())
                }
            }
            "as_of_from" => {
                // Set as_of_from to batch_timestamp for all tombstone records, preserving original time unit
                match field.data_type() {
                    arrow::datatypes::DataType::Timestamp(time_unit, tz) => {
                        let timezone_str = tz.as_ref().map(|t| t.to_string());
                        let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
                        
                        use arrow::datatypes::TimeUnit;
                        let array: arrow::array::ArrayRef = match time_unit {
                            TimeUnit::Nanosecond => {
                                let nanoseconds = (batch_timestamp - epoch).num_nanoseconds().unwrap();
                                let values = vec![Some(nanoseconds); current_row_indices.len()];
                                let array = arrow::array::TimestampNanosecondArray::from(values).with_timezone_opt(timezone_str);
                                std::sync::Arc::new(array)
                            }
                            TimeUnit::Microsecond => {
                                let microseconds = (batch_timestamp - epoch).num_microseconds().unwrap();
                                let values = vec![Some(microseconds); current_row_indices.len()];
                                let array = arrow::array::TimestampMicrosecondArray::from(values).with_timezone_opt(timezone_str);
                                std::sync::Arc::new(array)
                            }
                            TimeUnit::Millisecond => {
                                let milliseconds = (batch_timestamp - epoch).num_milliseconds();
                                let values = vec![Some(milliseconds); current_row_indices.len()];
                                let array = arrow::array::TimestampMillisecondArray::from(values).with_timezone_opt(timezone_str);
                                std::sync::Arc::new(array)
                            }
                            TimeUnit::Second => {
                                let seconds = (batch_timestamp - epoch).num_seconds();
                                let values = vec![Some(seconds); current_row_indices.len()];
                                let array = arrow::array::TimestampSecondArray::from(values).with_timezone_opt(timezone_str);
                                std::sync::Arc::new(array)
                            }
                        };
                        columns.push(array);
                    }
                    _ => return Err("as_of_from column must be timestamp type".to_string())
                }
            }
            _ => {
                // Copy original column as-is
                columns.push(sliced_batch.column_by_name(column_name).unwrap().clone());
            }
        }
    }
    
    arrow::array::RecordBatch::try_new(schema, columns)
        .map_err(|e| format!("Failed to create tombstone batch: {}", e))
}

/// Extract temporal bounds (effective_from, effective_to) for a record
/// PERFORMANCE: Inlined for hot path usage in full_state temporal comparisons
#[inline]
fn get_temporal_bounds(
    batch: &RecordBatch,
    row_idx: usize,
) -> Result<(NaiveDateTime, NaiveDateTime), String> {
    let eff_from_array = batch.column_by_name("effective_from")
        .ok_or("effective_from column not found")?;
    let eff_to_array = batch.column_by_name("effective_to")
        .ok_or("effective_to column not found")?;

    let from = extract_datetime_flexible(eff_from_array.as_ref(), row_idx)?;
    let to = extract_datetime_flexible(eff_to_array.as_ref(), row_idx)?;

    Ok((from, to))
}

/// Check if two temporal segments are adjacent (touching endpoints but not overlapping)
/// Adjacent means one segment ends exactly where the other begins
#[inline]
fn are_segments_adjacent(
    seg1_from: NaiveDateTime,
    seg1_to: NaiveDateTime,
    seg2_from: NaiveDateTime,
    seg2_to: NaiveDateTime,
) -> bool {
    seg1_to == seg2_from || seg2_to == seg1_from
}

/// Create a merged temporal segment from records across two batches
/// Used when adjacent segments have identical values and should be coalesced
fn create_merged_segment_cross_batch(
    current_batch: &RecordBatch,
    updates_batch: &RecordBatch,
    current_idx: usize,
    update_idx: usize,
    batch_timestamp: NaiveDateTime,
) -> Result<RecordBatch, String> {
    // Get temporal bounds from both records
    let (curr_from, curr_to) = get_temporal_bounds(current_batch, current_idx)?;
    let (upd_from, upd_to) = get_temporal_bounds(updates_batch, update_idx)?;

    // Calculate merged temporal range (earliest from, latest to)
    let merged_from = curr_from.min(upd_from);
    let merged_to = curr_to.max(upd_to);

    // Use update record as the base (it has newer as_of information)
    let indices = arrow::array::UInt64Array::from(vec![Some(update_idx as u64)]);
    let base_batch = arrow::compute::take_record_batch(updates_batch, &indices)
        .map_err(|e| format!("Failed to extract update record: {}", e))?;

    // Replace the temporal columns with merged values
    let schema = base_batch.schema();
    let mut new_columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let col_name = field.name();

        match col_name.as_str() {
            "effective_from" => {
                // Set to merged start time
                let array = create_timestamp_array(field.data_type(), merged_from, 1)?;
                new_columns.push(array);
            },
            "effective_to" => {
                // Set to merged end time
                let array = create_timestamp_array(field.data_type(), merged_to, 1)?;
                new_columns.push(array);
            },
            "as_of_from" => {
                // Use batch_timestamp for the merged record (newer knowledge)
                let array = create_timestamp_array(field.data_type(), batch_timestamp, 1)?;
                new_columns.push(array);
            },
            _ => {
                // Keep all other columns from the update record
                new_columns.push(base_batch.column_by_name(col_name).unwrap().clone());
            }
        }
    }

    RecordBatch::try_new(schema, new_columns)
        .map_err(|e| format!("Failed to create merged batch: {}", e))
}

/// Create a timestamp array with a single value, preserving the original data type
fn create_timestamp_array(
    data_type: &arrow::datatypes::DataType,
    datetime: NaiveDateTime,
    length: usize,
) -> Result<arrow::array::ArrayRef, String> {
    use arrow::datatypes::TimeUnit;
    use arrow::array::*;

    let epoch = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();

    match data_type {
        arrow::datatypes::DataType::Timestamp(time_unit, tz) => {
            let timezone_str = tz.as_ref().map(|t| t.to_string());

            let array: arrow::array::ArrayRef = match time_unit {
                TimeUnit::Nanosecond => {
                    let nanoseconds = (datetime - epoch).num_nanoseconds()
                        .ok_or("Timestamp overflow in nanoseconds")?;
                    let values = vec![Some(nanoseconds); length];
                    let array = TimestampNanosecondArray::from(values)
                        .with_timezone_opt(timezone_str);
                    std::sync::Arc::new(array)
                }
                TimeUnit::Microsecond => {
                    let microseconds = (datetime - epoch).num_microseconds()
                        .ok_or("Timestamp overflow in microseconds")?;
                    let values = vec![Some(microseconds); length];
                    let array = TimestampMicrosecondArray::from(values)
                        .with_timezone_opt(timezone_str);
                    std::sync::Arc::new(array)
                }
                TimeUnit::Millisecond => {
                    let milliseconds = (datetime - epoch).num_milliseconds();
                    let values = vec![Some(milliseconds); length];
                    let array = TimestampMillisecondArray::from(values)
                        .with_timezone_opt(timezone_str);
                    std::sync::Arc::new(array)
                }
                TimeUnit::Second => {
                    let seconds = (datetime - epoch).num_seconds();
                    let values = vec![Some(seconds); length];
                    let array = TimestampSecondArray::from(values)
                        .with_timezone_opt(timezone_str);
                    std::sync::Arc::new(array)
                }
            };
            Ok(array)
        }
        arrow::datatypes::DataType::Date32 => {
            let days = (datetime.date() - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
            let values = vec![Some(days); length];
            Ok(std::sync::Arc::new(Date32Array::from(values)))
        }
        arrow::datatypes::DataType::Date64 => {
            let millis = (datetime - epoch).num_milliseconds();
            let values = vec![Some(millis); length];
            Ok(std::sync::Arc::new(Date64Array::from(values)))
        }
        _ => Err(format!("Unsupported temporal data type: {:?}", data_type))
    }
}

/// Optimized full state processing without expensive conversions until needed
#[allow(clippy::too_many_arguments)]
fn process_full_state_optimized(
    current_row_indices: &[usize],
    update_row_indices: &[usize],
    current_batch: &RecordBatch,
    updates_batch: &RecordBatch,
    value_columns: &[String],
    _system_date: NaiveDate,
    _batch_timestamp: chrono::NaiveDateTime,
    expire_indices: &mut Vec<usize>,
    insert_batches: &mut Vec<RecordBatch>,
) -> Result<(), String> {
    // For full state mode, we need to compare hashes efficiently
    // Get value hash arrays if they exist
    let current_hash_array = current_batch.column_by_name("value_hash")
        .map(|col| col.as_any().downcast_ref::<arrow::array::StringArray>().unwrap());
    let updates_hash_array = updates_batch.column_by_name("value_hash")
        .map(|col| col.as_any().downcast_ref::<arrow::array::StringArray>().unwrap());
    
    if let (Some(current_hashes), Some(update_hashes)) = (current_hash_array, updates_hash_array) {
        // Enhanced full_state mode with temporal awareness:
        // - Different values (different hash) -> expire old, insert new
        // - Same values (same hash) + adjacent temporal segments -> merge into single segment
        // - Same values + non-adjacent temporal segments -> insert update as-is
        // - Same values + exact same temporal range -> do nothing (true no-change)

        // Track which updates need to be inserted (not merged)
        let mut updates_to_insert = Vec::new();

        // For each update, determine the relationship with current state
        for &update_idx in update_row_indices {
            let update_hash = update_hashes.value(update_idx);
            let update_temporal = get_temporal_bounds(updates_batch, update_idx)?;

            // Find if there's a matching current record (same hash)
            let mut matching_current_idx: Option<usize> = None;
            let mut is_adjacent = false;
            let mut is_exact_match = false;

            for &current_idx in current_row_indices {
                let current_hash = current_hashes.value(current_idx);

                if current_hash == update_hash {
                    // Found a matching value hash
                    matching_current_idx = Some(current_idx);
                    let current_temporal = get_temporal_bounds(current_batch, current_idx)?;

                    // Check temporal relationship
                    if are_segments_adjacent(
                        current_temporal.0, current_temporal.1,
                        update_temporal.0, update_temporal.1
                    ) {
                        is_adjacent = true;
                    } else if current_temporal == update_temporal {
                        // Exact same temporal range with same values = no change
                        is_exact_match = true;
                    }

                    break; // Found the matching record
                }
            }

            // Decision logic based on relationship
            match (matching_current_idx, is_adjacent, is_exact_match) {
                (Some(current_idx), true, _) => {
                    // Case 1: Adjacent segments with same values -> MERGE
                    expire_indices.push(current_idx);
                    let merged_batch = create_merged_segment_cross_batch(
                        current_batch,
                        updates_batch,
                        current_idx,
                        update_idx,
                        _batch_timestamp,
                    )?;
                    insert_batches.push(merged_batch);
                },
                (Some(_), false, true) => {
                    // Case 2: Exact same temporal range with same values -> NO CHANGE
                    // Do nothing (no expire, no insert)
                },
                (Some(_current_idx), false, false) => {
                    // Case 3: Same values but different non-adjacent temporal ranges
                    // Insert the update as a separate temporal segment
                    updates_to_insert.push(update_idx);
                },
                (None, _, _) => {
                    // Case 4: Different values -> expire all current, insert update
                    if updates_to_insert.is_empty() {
                        // Only expire once for this ID group
                        expire_indices.extend(current_row_indices.iter().cloned());
                    }
                    updates_to_insert.push(update_idx);
                },
            }
        }

        // Insert updates that weren't merged
        if !updates_to_insert.is_empty() {
            let indices_array = arrow::array::UInt64Array::from(
                updates_to_insert.iter().map(|&i| Some(i as u64)).collect::<Vec<_>>()
            );
            let updates_slice = arrow::compute::take_record_batch(updates_batch, &indices_array)
                .map_err(|e| format!("Failed to slice updates batch: {}", e))?;
            insert_batches.push(updates_slice);
        }
        
    } else {
        // Fallback to expensive comparison if no hash columns
        // Convert to BitemporalRecords only when needed
        let _current_records = create_bitemporal_records_from_indices(
            current_row_indices,
            current_batch,
            &[], // Don't need ID columns for comparison
            value_columns,
        )?;
        let _update_records = create_bitemporal_records_from_indices(
            update_row_indices,
            updates_batch,
            &[],
            value_columns,
        )?;
        
        // Do full state comparison logic (implementation would go here)
        // For now, expire all current and insert all updates
        expire_indices.extend(current_row_indices.iter().cloned());
        
        let indices_array = arrow::array::UInt64Array::from(
            update_row_indices.iter().map(|&i| Some(i as u64)).collect::<Vec<_>>()
        );
        let updates_slice = arrow::compute::take_record_batch(updates_batch, &indices_array)
            .map_err(|e| format!("Failed to slice updates batch: {}", e))?;
        insert_batches.push(updates_slice);
    }
    
    Ok(())
}

/// Helper function to extract datetime from any date/timestamp array type
/// PERFORMANCE: Inlined for hot path - called for every temporal field access
#[inline(always)]
fn extract_datetime_flexible(array: &dyn arrow::array::Array, idx: usize) -> Result<chrono::NaiveDateTime, String> {
    use arrow::array::*;
    use arrow::datatypes::TimeUnit;
    
    match array.data_type() {
        // Date32 - days since epoch
        arrow::datatypes::DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>()
                .ok_or("Failed to downcast to Date32Array")?;
            let days = arr.value(idx);
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .ok_or_else(|| format!("Invalid Date32 value: {}", days))?;
            Ok(date.and_hms_opt(0, 0, 0).unwrap())
        }
        // Date64 - milliseconds since epoch
        arrow::datatypes::DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>()
                .ok_or("Failed to downcast to Date64Array")?;
            let millis = arr.value(idx);
            let seconds = millis / 1000;
            let nanos = ((millis % 1000) * 1_000_000) as u32;
            Ok(chrono::DateTime::from_timestamp(seconds, nanos)
                .ok_or_else(|| format!("Invalid Date64 value: {}", millis))?
                .naive_utc())
        }
        // Timestamp with different time units
        arrow::datatypes::DataType::Timestamp(unit, _) => {
            match unit {
                TimeUnit::Second => {
                    let arr = array.as_any().downcast_ref::<TimestampSecondArray>()
                        .ok_or("Failed to downcast to TimestampSecondArray")?;
                    let seconds = arr.value(idx);
                    Ok(chrono::DateTime::from_timestamp(seconds, 0)
                        .ok_or_else(|| format!("Invalid timestamp seconds: {}", seconds))?
                        .naive_utc())
                }
                TimeUnit::Millisecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMillisecondArray>()
                        .ok_or("Failed to downcast to TimestampMillisecondArray")?;
                    let millis = arr.value(idx);
                    let seconds = millis / 1000;
                    let nanos = ((millis % 1000) * 1_000_000) as u32;
                    Ok(chrono::DateTime::from_timestamp(seconds, nanos)
                        .ok_or_else(|| format!("Invalid timestamp milliseconds: {}", millis))?
                        .naive_utc())
                }
                TimeUnit::Microsecond => {
                    let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or("Failed to downcast to TimestampMicrosecondArray")?;
                    let micros = arr.value(idx);
                    let seconds = micros / 1_000_000;
                    let nanos = ((micros % 1_000_000) * 1000) as u32;
                    Ok(chrono::DateTime::from_timestamp(seconds, nanos)
                        .ok_or_else(|| format!("Invalid timestamp microseconds: {}", micros))?
                        .naive_utc())
                }
                TimeUnit::Nanosecond => {
                    let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>()
                        .ok_or("Failed to downcast to TimestampNanosecondArray")?;
                    let nanos = arr.value(idx);
                    let seconds = nanos / 1_000_000_000;
                    let nano_part = (nanos % 1_000_000_000) as u32;
                    Ok(chrono::DateTime::from_timestamp(seconds, nano_part)
                        .ok_or_else(|| format!("Invalid timestamp nanoseconds: {}", nanos))?
                        .naive_utc())
                }
            }
        }
        dt => Err(format!("Unsupported date/timestamp type for temporal columns: {:?}. Supported types: Date32, Date64, Timestamp(Second/Millisecond/Microsecond/Nanosecond)", dt))
    }
}

/// Create BitemporalRecords only when needed for temporal processing
fn create_bitemporal_records_from_indices(
    row_indices: &[usize],
    batch: &RecordBatch,
    id_columns: &[String],
    _value_columns: &[String],
) -> Result<Vec<BitemporalRecord>, String> {
    if row_indices.is_empty() {
        return Ok(Vec::new());
    }
    
    let mut records = Vec::with_capacity(row_indices.len());
    
    // Extract arrays once - now flexible with types
    let eff_from_array = batch.column_by_name("effective_from")
        .ok_or("effective_from column not found")?;
    let eff_to_array = batch.column_by_name("effective_to")
        .ok_or("effective_to column not found")?;
    let as_of_from_array = batch.column_by_name("as_of_from")
        .ok_or("as_of_from column not found")?;
    
    // Get the pre-computed hash column - it should always exist due to ensure_hash_column
    let hash_array = batch.column_by_name("value_hash")
        .ok_or_else(|| "value_hash column not found - this should not happen".to_string())?
        .as_any().downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| "value_hash column is not a StringArray".to_string())?;
    
    for &row_idx in row_indices {
        let mut id_values = Vec::new();
        for id_col in id_columns {
            let col_idx = batch.schema().index_of(id_col)
                .map_err(|_| format!("ID column {} not found", id_col))?;
            let array = batch.column(col_idx);
            id_values.push(ScalarValue::from_array(array, row_idx));
        }
        
        let record = BitemporalRecord {
            id_values,
            value_hash: hash_array.value(row_idx).to_string(),
            effective_from: extract_datetime_flexible(eff_from_array.as_ref(), row_idx)?,
            effective_to: extract_datetime_flexible(eff_to_array.as_ref(), row_idx)?,
            as_of_from: extract_datetime_flexible(as_of_from_array.as_ref(), row_idx)?,
            as_of_to: MAX_TIMESTAMP,
            original_index: Some(row_idx),
        };
        
        records.push(record);
    }
    
    Ok(records)
}

/// Fast ID key creation using string concatenation instead of expensive ScalarValue conversions
/// PERFORMANCE: Inlined because this is called 850,000+ times (once per row)
#[inline(always)]
fn create_id_key_with_buffer(id_arrays: &[arrow::array::ArrayRef], row_idx: usize, buffer: &mut String) {
    buffer.clear(); // Reuse existing allocation
    
    for (i, array) in id_arrays.iter().enumerate() {
        if i > 0 {
            buffer.push('|'); // Separator
        }
        
        // Fast string extraction without ScalarValue conversion
        match array.data_type() {
            arrow::datatypes::DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                if string_array.is_null(row_idx) {
                    buffer.push_str("NULL");
                } else {
                    buffer.push_str(string_array.value(row_idx));
                }
            }
            arrow::datatypes::DataType::Int32 => {
                let int_array = array.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
                if int_array.is_null(row_idx) {
                    buffer.push_str("NULL");
                } else {
                    buffer.push_str(&int_array.value(row_idx).to_string());
                }
            }
            arrow::datatypes::DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
                if int_array.is_null(row_idx) {
                    buffer.push_str("NULL");
                } else {
                    buffer.push_str(&int_array.value(row_idx).to_string());
                }
            }
            arrow::datatypes::DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
                if float_array.is_null(row_idx) {
                    buffer.push_str("NULL");
                } else {
                    buffer.push_str(&float_array.value(row_idx).to_string());
                }
            }
            _ => {
                // Fallback to ScalarValue for other types (but most ID columns are strings/ints)
                let scalar = ScalarValue::from_array(array, row_idx);
                buffer.push_str(&format!("{:?}", scalar));
            }
        }
    }
}

#[pyfunction]
fn compute_changes(
    current_state: PyRecordBatch,
    updates: PyRecordBatch,
    id_columns: Vec<String>,
    value_columns: Vec<String>,
    system_date: String,
    update_mode: String,
    conflate_inputs: Option<bool>,
) -> PyResult<(Vec<usize>, Vec<PyRecordBatch>, Vec<PyRecordBatch>)> {
    compute_changes_with_hash_algorithm(current_state, updates, id_columns, value_columns, system_date, update_mode, None, conflate_inputs)
}

#[pyfunction]
fn compute_changes_with_hash_algorithm(
    current_state: PyRecordBatch,
    updates: PyRecordBatch,
    id_columns: Vec<String>,
    value_columns: Vec<String>,
    system_date: String,
    update_mode: String,
    hash_algorithm: Option<String>,
    conflate_inputs: Option<bool>,
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

    // Parse hash algorithm
    let algorithm = match hash_algorithm {
        Some(algo_str) => HashAlgorithm::from_str(&algo_str)
            .map_err(pyo3::exceptions::PyValueError::new_err)?,
        None => HashAlgorithm::default(),
    };

    // Parse conflate_inputs parameter (default to false for backward compatibility)
    let conflate = conflate_inputs.unwrap_or(false);

    // Call the process_updates function
    let changeset = process_updates_with_algorithm(
        current_batch,
        updates_batch,
        id_columns,
        value_columns,
        system_date,
        mode,
        algorithm,
        conflate,
    ).map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    
    // Convert the result back to Python types
    let expire_indices = changeset.to_expire;
    let insert_batches: Vec<PyRecordBatch> = changeset.to_insert
        .into_iter()
        .map(PyRecordBatch::new)
        .collect();
    let expired_batches: Vec<PyRecordBatch> = changeset.expired_records
        .into_iter()
        .map(PyRecordBatch::new)
        .collect();
    
    Ok((expire_indices, insert_batches, expired_batches))
}

#[pyfunction]
fn add_hash_key(
    record_batch: PyRecordBatch,
    value_fields: Vec<String>,
) -> PyResult<PyRecordBatch> {
    add_hash_key_with_algorithm(record_batch, value_fields, None)
}

#[pyfunction]
fn add_hash_key_with_algorithm(
    record_batch: PyRecordBatch,
    value_fields: Vec<String>,
    hash_algorithm: Option<String>,
) -> PyResult<PyRecordBatch> {
    // Convert PyRecordBatch to Arrow RecordBatch
    let batch = record_batch.as_ref().clone();
    
    // Parse hash algorithm
    let algorithm = match hash_algorithm {
        Some(algo_str) => HashAlgorithm::from_str(&algo_str)
            .map_err(pyo3::exceptions::PyValueError::new_err)?,
        None => HashAlgorithm::default(),
    };
    
    // Call the fast Arrow-direct hash function
    let batch_with_hash = crate::arrow_hash::add_hash_column_arrow_direct(&batch, &value_fields, algorithm)
        .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
    
    // Convert back to PyRecordBatch
    Ok(PyRecordBatch::new(batch_with_hash))
}

#[pymodule]
fn pytemporal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compute_changes, m)?)?;
    m.add_function(wrap_pyfunction!(compute_changes_with_hash_algorithm, m)?)?;
    m.add_function(wrap_pyfunction!(add_hash_key, m)?)?;
    m.add_function(wrap_pyfunction!(add_hash_key_with_algorithm, m)?)?;
    Ok(())
}
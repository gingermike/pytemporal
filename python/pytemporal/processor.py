"""
Bitemporal timeseries processor.

This module provides a high-level interface for processing bitemporal timeseries
data using the underlying Rust implementation.
"""
import pyarrow as pa
import pandas as pd
from typing import List, Tuple, Optional, Literal
from datetime import datetime, date

# Import the Rust functions
from .pytemporal import (
    compute_changes as _compute_changes,
    add_hash_key as _add_hash_key,
    add_hash_key_with_algorithm as _add_hash_key_with_algorithm
)

# Infinity date representation - use a safe date that doesn't overflow pandas
INFINITY_TIMESTAMP = pd.Timestamp('2260-12-31 23:59:59')

# Safe maximum timestamp that won't overflow when timezone-localized
# Use a timestamp well before pandas max to avoid overflow during tz operations
SAFE_MAX_TIMESTAMP = pd.Timestamp('2260-12-31 23:59:59')

# Pandas maximum timestamp (approximately 2262-04-11) - use cautiously
PANDAS_MAX_TIMESTAMP = pd.Timestamp.max

class BitemporalTimeseriesProcessor:
    """
    A processor for bitemporal timeseries data that efficiently computes
    changes between current state and incoming updates.
    
    Supports both delta updates (only changes) and full state updates
    (complete replacement of state for given IDs).
    """
    
    def __init__(self, id_columns: List[str], value_columns: List[str], conflate_inputs: bool = False):
        """
        Initialize the processor with column definitions.

        Args:
            id_columns: List of column names that identify a unique timeseries
            value_columns: List of column names containing the values to track
            conflate_inputs: Whether to conflate consecutive input updates with same ID and values (default: False)
        """
        self.id_columns = id_columns
        self.value_columns = value_columns
        self.conflate_inputs = conflate_inputs
    
    def compute_changes(
        self,
        current_state: pd.DataFrame,
        updates: pd.DataFrame,
        system_date: Optional[str] = None,
        update_mode: Literal["delta", "full_state"] = "delta",
        conflate_inputs: Optional[bool] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Compute the changes needed to update the bitemporal timeseries.

        Args:
            current_state: DataFrame with current database state
            updates: DataFrame with incoming updates
            system_date: Optional system date (YYYY-MM-DD format)
            update_mode: "delta" for incremental updates, "full_state" for complete state replacement (only expires/inserts when values change)
            conflate_inputs: Whether to conflate consecutive input updates with same ID and values (default: use class-level setting)

        Returns:
            Tuple of (rows_to_expire, rows_to_insert)
            - rows_to_expire: DataFrame with rows that need as_of_to set
            - rows_to_insert: DataFrame with new rows to insert
        """
        # Prepare DataFrames for processing
        current_state = self._prepare_dataframe(current_state)
        updates = self._prepare_dataframe(updates)

        # Align schemas: reorder columns and validate compatibility
        current_state, updates = self._align_schemas(current_state, updates)

        # Normalize schemas to ensure timezone consistency between DataFrames
        current_state, updates = self._normalize_schemas(current_state, updates)

        # Convert pandas DataFrames to Arrow RecordBatches
        # CRITICAL: preserve_index=False prevents pandas index from leaking into Arrow schema
        # Without this, some batches get __index_level_0__ column which breaks Rust consolidation
        current_batch = pa.RecordBatch.from_pandas(current_state, preserve_index=False)
        updates_batch = pa.RecordBatch.from_pandas(updates, preserve_index=False)
        
        # Convert timestamp columns from nanoseconds to microseconds for Rust compatibility
        current_batch = self._convert_timestamps_to_microseconds(current_batch)
        updates_batch = self._convert_timestamps_to_microseconds(updates_batch)
        
        # Determine conflate_inputs value (use method parameter if provided, otherwise use class default)
        actual_conflate_inputs = conflate_inputs if conflate_inputs is not None else self.conflate_inputs

        # Call Rust function
        actual_system_date = system_date or datetime.now().strftime('%Y-%m-%d')
        expire_indices, insert_batch, expired_batch = _compute_changes(
            current_batch,
            updates_batch,
            self.id_columns,
            self.value_columns,
            actual_system_date,
            update_mode,
            actual_conflate_inputs
        )
        
        # Use expired records from Rust (with updated as_of_to timestamps)
        # Convert using zero-copy Arrow PyCapsule interface for optimal performance
        if expired_batch:
            # Convert arro3 batches to PyArrow via PyCapsule interface (zero-copy)
            pa_batches = [pa.record_batch(batch) for batch in expired_batch]
            if pa_batches:
                table = pa.Table.from_batches(pa_batches)
                rows_to_expire = table.to_pandas(self_destruct=True)
            else:
                rows_to_expire = pd.DataFrame(columns=current_state.columns)
        else:
            rows_to_expire = pd.DataFrame(columns=current_state.columns)
        
        # In full_state mode, adjust effective_to for records that have temporal changes
        if update_mode == 'full_state' and not rows_to_expire.empty and not updates.empty:
            # Create a lookup for updates by ID values and effective_from using vectorized operations
            id_cols = self.id_columns

            # Build lookup using vectorized zip (much faster than iterrows)
            update_id_keys = list(zip(*[updates[col].values for col in id_cols]))
            update_eff_from = updates['effective_from'].values
            update_eff_to = updates['effective_to'].values
            updates_lookup = {(id_key, eff_from): eff_to
                             for id_key, eff_from, eff_to in zip(update_id_keys, update_eff_from, update_eff_to)}

            # Build expire keys using vectorized operations
            expire_id_keys = list(zip(*[rows_to_expire[col].values for col in id_cols]))
            expire_eff_from = rows_to_expire['effective_from'].values

            # Find matching updates and adjust effective_to
            eff_to_col_idx = rows_to_expire.columns.get_loc('effective_to')
            for idx, (id_key, eff_from) in enumerate(zip(expire_id_keys, expire_eff_from)):
                key = (id_key, eff_from)
                if key in updates_lookup:
                    rows_to_expire.iloc[idx, eff_to_col_idx] = updates_lookup[key]
        
        # as_of_to is now set by Rust layer
        
        # Convert insert batches back to pandas using zero-copy Arrow PyCapsule interface
        if insert_batch:
            # Convert arro3 batches to PyArrow via PyCapsule interface (zero-copy)
            pa_batches = [pa.record_batch(batch) for batch in insert_batch]
            if pa_batches:
                table = pa.Table.from_batches(pa_batches)
                rows_to_insert = table.to_pandas(self_destruct=True)
            else:
                rows_to_insert = pd.DataFrame(columns=current_state.columns)
        else:
            rows_to_insert = pd.DataFrame(columns=current_state.columns)
        
        if not rows_to_insert.empty:
            rows_to_insert = self._convert_from_internal_format(rows_to_insert)
            # Sort by effective_from for consistent ordering
            rows_to_insert = rows_to_insert.sort_values(by=['effective_from']).reset_index(drop=True)
        
        return rows_to_expire, rows_to_insert
    
    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for processing by converting infinity dates.
        """
        df = df.copy()
        
        # Convert infinity to pandas max timestamp for internal processing
        effective_date_columns = ['effective_from', 'effective_to']
        as_of_timestamp_columns = ['as_of_from', 'as_of_to']
        
        # Handle effective date columns (convert to dates)
        for col in effective_date_columns:
            if col in df.columns:
                # Only convert to datetime if not already a datetime type (preserve timezone info)
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col])
                
                # Handle null values and infinity replacement with timezone awareness
                fill_value = SAFE_MAX_TIMESTAMP
                infinity_threshold = pd.Timestamp('9999-01-01')
                replacement_value = SAFE_MAX_TIMESTAMP
                
                if (hasattr(df[col].dtype, 'tz') and df[col].dtype.tz is not None) or \
                   (hasattr(df[col], 'dt') and df[col].dt.tz is not None):
                    # Column is timezone-aware, make all values timezone-aware too
                    col_tz = getattr(df[col].dtype, 'tz', None) or df[col].dt.tz
                    fill_value = fill_value.tz_localize(col_tz)
                    infinity_threshold = infinity_threshold.tz_localize(col_tz)
                    replacement_value = replacement_value.tz_localize(col_tz)
                
                # Replace null with pandas max timestamp
                df[col] = df[col].fillna(fill_value)
                df.loc[df[col] >= infinity_threshold, col] = replacement_value
                
                # Keep as timestamp for processing (timestamp precision)
        
        # Handle as_of timestamp columns (preserve timestamp precision)
        for col in as_of_timestamp_columns:
            if col in df.columns:
                # Only convert to datetime if not already a datetime type (preserve timezone info)
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col])
                
                # Handle null values and infinity replacement with timezone awareness
                fill_value = SAFE_MAX_TIMESTAMP
                infinity_threshold = pd.Timestamp('9999-01-01')
                replacement_value = SAFE_MAX_TIMESTAMP
                
                if (hasattr(df[col].dtype, 'tz') and df[col].dtype.tz is not None) or \
                   (hasattr(df[col], 'dt') and df[col].dt.tz is not None):
                    # Column is timezone-aware, make all values timezone-aware too
                    col_tz = getattr(df[col].dtype, 'tz', None) or df[col].dt.tz
                    fill_value = fill_value.tz_localize(col_tz)
                    infinity_threshold = infinity_threshold.tz_localize(col_tz)
                    replacement_value = replacement_value.tz_localize(col_tz)
                
                # Replace null with pandas max timestamp
                df[col] = df[col].fillna(fill_value)
                df.loc[df[col] >= infinity_threshold, col] = replacement_value
                
                # Keep as timestamp for microsecond precision
                # Note: pandas uses nanosecond precision, which is compatible with Arrow timestamp[ns]
        
        # Add value_hash column if it doesn't exist (it will be computed by Rust)
        if 'value_hash' not in df.columns:
            df['value_hash'] = ""  # Placeholder, will be computed by Rust
        
        return df
    
    def _align_schemas(self, current_state: pd.DataFrame, updates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Align schemas between current_state and updates by reordering columns.

        This ensures Arrow schema compatibility by making both DataFrames have
        the same column order for their common columns.
        """
        current_cols = set(current_state.columns)
        updates_cols = set(updates.columns)

        # Find common columns
        common_cols = current_cols & updates_cols

        # If no common columns or empty DataFrames, return as-is
        if not common_cols or current_state.empty:
            return current_state, updates

        # Build canonical column order from current_state (for common columns only)
        canonical_order = [c for c in current_state.columns if c in common_cols]

        # Filter and reorder both DataFrames to have same columns in same order
        current_state = current_state[canonical_order]
        updates = updates[canonical_order]

        return current_state, updates

    def _normalize_schemas(self, current_state: pd.DataFrame, updates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Normalize schemas between current_state and updates to ensure timezone consistency.
        This prevents Arrow schema mismatches when one DataFrame has timezone-aware columns
        and the other has timezone-naive columns.
        """
        current_state = current_state.copy()
        updates = updates.copy()
        
        timestamp_columns = ['effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        
        for col in timestamp_columns:
            if col in current_state.columns and col in updates.columns:
                current_tz = None
                updates_tz = None
                
                # Get timezone info from current_state
                if hasattr(current_state[col].dtype, 'tz') and current_state[col].dtype.tz is not None:
                    current_tz = current_state[col].dtype.tz
                elif hasattr(current_state[col], 'dt') and current_state[col].dt.tz is not None:
                    current_tz = current_state[col].dt.tz
                
                # Get timezone info from updates
                if hasattr(updates[col].dtype, 'tz') and updates[col].dtype.tz is not None:
                    updates_tz = updates[col].dtype.tz
                elif hasattr(updates[col], 'dt') and updates[col].dt.tz is not None:
                    updates_tz = updates[col].dt.tz
                
                # Normalize to a common timezone representation
                if current_tz is not None and updates_tz is None:
                    # Current has timezone, updates doesn't - add timezone to updates
                    # Assume updates are in the same timezone as current (common case)
                    updates[col] = updates[col].dt.tz_localize(current_tz)
                elif current_tz is None and updates_tz is not None:
                    # Updates has timezone, current doesn't - add timezone to current
                    # Assume current is in the same timezone as updates
                    current_state[col] = current_state[col].dt.tz_localize(updates_tz)
                elif current_tz is not None and updates_tz is not None and current_tz != updates_tz:
                    # Both have different timezones - convert updates to current's timezone
                    updates[col] = updates[col].dt.tz_convert(current_tz)
                
                # If both are None (timezone-naive), leave them as-is
        
        return current_state, updates
    
    def _convert_timestamps_to_microseconds(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """
        Convert timestamp columns to microseconds for Rust compatibility.
        Also convert effective_from/effective_to from Date32 to Timestamp.
        """
        schema = batch.schema
        columns = []
        
        for i, field in enumerate(schema):
            column = batch.column(i)
            
            # Convert as_of timestamp columns from ns to us
            if field.name in ['as_of_from', 'as_of_to'] and pa.types.is_timestamp(field.type):
                if field.type.unit == 'ns':
                    # Preserve timezone information during conversion
                    target_type = pa.timestamp('us', tz=field.type.tz)
                    # Handle pandas max timestamp which is too large for microseconds
                    # Cast with safe conversion that truncates nanoseconds
                    try:
                        column = column.cast(target_type)
                    except pa.ArrowInvalid:
                        # If casting fails due to overflow, manually convert
                        # This happens with pd.Timestamp.max
                        np_array = column.to_pandas().values
                        # Convert to microseconds by dividing nanoseconds by 1000
                        us_values = np_array.astype('datetime64[us]')
                        column = pa.array(us_values, type=target_type)
            
            # Convert effective timestamp columns from ns to us
            elif field.name in ['effective_from', 'effective_to'] and pa.types.is_timestamp(field.type):
                if field.type.unit == 'ns':
                    # Preserve timezone information during conversion
                    target_type = pa.timestamp('us', tz=field.type.tz)
                    # Convert nanosecond timestamps to microsecond timestamps
                    try:
                        column = column.cast(target_type)
                    except pa.ArrowInvalid:
                        # If casting fails due to overflow, manually convert
                        np_array = column.to_pandas().values
                        us_values = np_array.astype('datetime64[us]')
                        column = pa.array(us_values, type=target_type)
            
            # Convert effective date columns from Date32 to Timestamp  
            elif field.name in ['effective_from', 'effective_to'] and pa.types.is_date32(field.type):
                # Convert Date32 to Timestamp (midnight for date-only values)
                pandas_series = column.to_pandas()
                timestamp_series = pd.to_datetime(pandas_series)
                column = pa.array(timestamp_series, type=pa.timestamp('us'))
            
            columns.append(column)
        
        # Create new schema with updated timestamp types (preserving timezone info)
        new_fields = []
        for field in schema:
            if field.name in ['as_of_from', 'as_of_to', 'effective_from', 'effective_to'] and pa.types.is_timestamp(field.type):
                # Preserve timezone information when updating to microseconds
                new_fields.append(pa.field(field.name, pa.timestamp('us', tz=field.type.tz), field.nullable))
            elif field.name in ['effective_from', 'effective_to'] and pa.types.is_date32(field.type):
                # Date32 columns don't have timezone, so use None
                new_fields.append(pa.field(field.name, pa.timestamp('us'), field.nullable))
            else:
                new_fields.append(field)
        
        new_schema = pa.schema(new_fields)
        return pa.RecordBatch.from_arrays(columns, schema=new_schema)
    
    def _convert_from_internal_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert from internal format back to external format.
        """
        df = df.copy()
        
        # Convert dates back to timestamps - handle effective and as_of columns differently
        effective_date_columns = ['effective_from', 'effective_to']
        as_of_timestamp_columns = ['as_of_from', 'as_of_to']

        # Convert effective date columns - force datetime.date objects to datetime
        # Use vectorized pd.to_datetime() instead of slow .apply() per element
        for col in effective_date_columns:
            if col in df.columns:
                # pd.to_datetime handles both date objects and timestamps correctly
                df[col] = pd.to_datetime(df[col])
        
        # Convert as_of timestamp columns more carefully to preserve precision
        for col in as_of_timestamp_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col])
                except (pd.errors.OutOfBoundsDatetime, OverflowError):
                    # If conversion fails, it's likely the max timestamp, handle below
                    pass
        
        # Convert pandas max timestamp back to PostgreSQL infinity for unbounded dates
        unbounded_columns = ['effective_to', 'as_of_to']
        for col in unbounded_columns:
            if col in df.columns:
                # Handle dates that are beyond pandas range or at the max value
                try:
                    # First, check if we already have datetime values
                    if not pd.api.types.is_datetime64_any_dtype(df[col]):
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    
                    # Create mask for infinity detection:
                    # 1. NaT values (result of overflow during conversion)
                    # 2. Dates beyond 2262 (near pandas max)
                    # 3. Dates exactly equal to pandas max
                    is_nat_mask = pd.isna(df[col])
                    is_large_date_mask = df[col].dt.year >= 2262
                    
                    # Create timezone-aware comparison timestamp if needed
                    max_timestamp_threshold = pd.Timestamp('2262-04-01')
                    if (hasattr(df[col].dtype, 'tz') and df[col].dtype.tz is not None) or \
                       (hasattr(df[col], 'dt') and df[col].dt.tz is not None):
                        col_tz = getattr(df[col].dtype, 'tz', None) or df[col].dt.tz
                        max_timestamp_threshold = max_timestamp_threshold.tz_localize(col_tz)
                    
                    is_max_timestamp_mask = df[col] >= max_timestamp_threshold
                    
                    infinity_mask = is_nat_mask | is_large_date_mask | is_max_timestamp_mask
                    
                    if infinity_mask.any():
                        # Replace infinity values with infinity date
                        infinity_replacement = INFINITY_TIMESTAMP
                        if (hasattr(df[col].dtype, 'tz') and df[col].dtype.tz is not None) or \
                           (hasattr(df[col], 'dt') and df[col].dt.tz is not None):
                            col_tz = getattr(df[col].dtype, 'tz', None) or df[col].dt.tz
                            infinity_replacement = infinity_replacement.tz_localize(col_tz)
                        df.loc[infinity_mask, col] = infinity_replacement
                        
                except (pd.errors.OutOfBoundsDatetime, OverflowError, AttributeError):
                    # If any conversion fails due to overflow, assume entire column needs infinity
                    infinity_replacement = INFINITY_TIMESTAMP
                    if (hasattr(df[col].dtype, 'tz') and df[col].dtype.tz is not None) or \
                       (hasattr(df[col], 'dt') and df[col].dt.tz is not None):
                        col_tz = getattr(df[col].dtype, 'tz', None) or df[col].dt.tz
                        infinity_replacement = infinity_replacement.tz_localize(col_tz)
                    df[col] = infinity_replacement
        
        return df
    
    def validate_schema(self, df: pd.DataFrame) -> bool:
        """
        Validate that a DataFrame has the required schema.
        """
        required_cols = set(self.id_columns + self.value_columns + 
                           ['effective_from', 'effective_to', 'as_of_from', 'as_of_to'])
        return required_cols.issubset(set(df.columns))


def add_hash_key(df: pd.DataFrame, value_fields: List[str], hash_algorithm: str = 'xxhash') -> pd.DataFrame:
    """
    Add a hash key column to a pandas DataFrame based on specified value fields.

    This function uses the same hash algorithm as the internal bitemporal processing
    to ensure complete consistency. The hash is computed using XxHash (a fast,
    high-quality non-cryptographic hash) by default, providing an efficient way
    to detect changes in value columns.

    Args:
        df: Input DataFrame
        value_fields: List of column names to include in the hash calculation
        hash_algorithm: Hash algorithm to use. Options:
            - 'xxhash' (default): Fast, high-quality non-cryptographic hash
            - 'sha256': Cryptographic hash for legacy compatibility

    Returns:
        DataFrame with an additional 'value_hash' column containing hash hex strings

    Raises:
        ValueError: If any value_fields are not found in the DataFrame, or if
                   an invalid hash_algorithm is specified
        RuntimeError: If the hash computation fails

    Example:
        >>> import pandas as pd
        >>> from pytemporal import add_hash_key
        >>> df = pd.DataFrame({'id': [1, 2], 'price': [100, 200], 'volume': [10, 20]})
        >>>
        >>> # Use default XxHash algorithm
        >>> result = add_hash_key(df, ['price', 'volume'])
        >>> print(result.columns.tolist())
        ['id', 'price', 'volume', 'value_hash']
        >>>
        >>> # Use SHA256 for legacy compatibility
        >>> result_sha = add_hash_key(df, ['price', 'volume'], hash_algorithm='sha256')
    """
    if df.empty:
        raise ValueError("Cannot add hash key to empty DataFrame")

    # Validate that all value fields exist
    missing_cols = [col for col in value_fields if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Value fields not found in DataFrame: {missing_cols}")

    # Convert to Arrow RecordBatch (preserve_index=False prevents schema issues)
    record_batch = pa.RecordBatch.from_pandas(df, preserve_index=False)

    # Call the Rust function with the specified algorithm
    result_batch = _add_hash_key_with_algorithm(record_batch, value_fields, hash_algorithm)

    # Convert back to pandas using zero-copy Arrow PyCapsule interface
    pa_batch = pa.record_batch(result_batch)
    result_df = pa_batch.to_pandas()

    return result_df


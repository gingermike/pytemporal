"""
Bitemporal timeseries processor with PostgreSQL infinity date support.

This module handles the conversion between PostgreSQL's infinity timestamps
and pandas-compatible dates. Internally, we use pandas' maximum timestamp
(approximately 2262-04-11) to represent unbounded dates, then convert to
PostgreSQL's infinity representation (9999-12-31) when outputting data.
"""
import pyarrow as pa
import pandas as pd
from typing import List, Tuple, Optional, Literal
from datetime import datetime, date

# Import the Rust compute_changes function
from .bitemporal_timeseries import compute_changes as _compute_changes

# PostgreSQL infinity date representation - use a safe date that doesn't overflow pandas
POSTGRES_INFINITY = pd.Timestamp('2260-12-31 23:59:59')

# Pandas maximum timestamp (approximately 2262-04-11)
PANDAS_MAX_TIMESTAMP = pd.Timestamp.max

class BitemporalTimeseriesProcessor:
    """
    A processor for bitemporal timeseries data that efficiently computes
    changes between current state and incoming updates.
    
    Supports both delta updates (only changes) and full state updates
    (complete replacement of state for given IDs).
    """
    
    def __init__(self, id_columns: List[str], value_columns: List[str]):
        """
        Initialize the processor with column definitions.
        
        Args:
            id_columns: List of column names that identify a unique timeseries
            value_columns: List of column names containing the values to track
        """
        self.id_columns = id_columns
        self.value_columns = value_columns
    
    def compute_changes(
        self, 
        current_state: pd.DataFrame, 
        updates: pd.DataFrame,
        system_date: Optional[str] = None,
        update_mode: Literal["delta", "full_state"] = "delta"
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Compute the changes needed to update the bitemporal timeseries.
        
        Args:
            current_state: DataFrame with current database state
            updates: DataFrame with incoming updates
            system_date: Optional system date (YYYY-MM-DD format)
            update_mode: "delta" for incremental updates, "full_state" for complete state
            
        Returns:
            Tuple of (rows_to_expire, rows_to_insert)
            - rows_to_expire: DataFrame with rows that need as_of_to set
            - rows_to_insert: DataFrame with new rows to insert
        """
        # Prepare DataFrames for processing
        current_state = self._prepare_dataframe(current_state)
        updates = self._prepare_dataframe(updates)
        
        # Convert pandas DataFrames to Arrow RecordBatches
        current_batch = pa.RecordBatch.from_pandas(current_state)
        updates_batch = pa.RecordBatch.from_pandas(updates)
        
        # Convert timestamp columns from nanoseconds to microseconds for Rust compatibility
        current_batch = self._convert_timestamps_to_microseconds(current_batch)
        updates_batch = self._convert_timestamps_to_microseconds(updates_batch)
        
        # Call Rust function
        actual_system_date = system_date or datetime.now().strftime('%Y-%m-%d')
        expire_indices, insert_batch = _compute_changes(
            current_batch,
            updates_batch,
            self.id_columns,
            self.value_columns,
            actual_system_date,
            update_mode
        )
        
        # Extract rows to expire from original DataFrame
        rows_to_expire = current_state.iloc[expire_indices].copy()
        # Set as_of_to to current timestamp (when expiring the row)
        rows_to_expire['as_of_to'] = pd.Timestamp.now()
        
        # Convert insert batches back to pandas and combine them
        if insert_batch:
            insert_dfs = []
            for batch in insert_batch:
                # Convert arro3 RecordBatch to pandas DataFrame
                # Now that we have arro3-core installed, we can access its methods
                data = {}
                col_names = batch.column_names
                
                for i in range(batch.num_columns):
                    col_name = col_names[i]
                    column = batch.column(i)
                    
                    # Convert column to Python list
                    # arro3 columns have to_pylist method
                    col_data = column.to_pylist()
                    
                    data[col_name] = col_data
                
                insert_dfs.append(pd.DataFrame(data))
            
            # Combine all DataFrames
            rows_to_insert = pd.concat(insert_dfs, ignore_index=True) if insert_dfs else pd.DataFrame(columns=current_state.columns)
        else:
            rows_to_insert = pd.DataFrame(columns=current_state.columns)
        
        if not rows_to_insert.empty:
            rows_to_insert = self._convert_from_internal_format(rows_to_insert)
            # Sort by effective_from for consistent ordering
            rows_to_insert = rows_to_insert.sort_values(by=['effective_from']).reset_index(drop=True)
        
        return rows_to_expire, rows_to_insert
    
    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for processing by converting PostgreSQL infinity dates.
        """
        df = df.copy()
        
        # Convert PostgreSQL infinity to pandas max timestamp for internal processing
        effective_date_columns = ['effective_from', 'effective_to']
        as_of_timestamp_columns = ['as_of_from', 'as_of_to']
        
        # Handle effective date columns (convert to dates)
        for col in effective_date_columns:
            if col in df.columns:
                # Replace null with pandas max timestamp
                df[col] = df[col].fillna(PANDAS_MAX_TIMESTAMP)
                # Replace PostgreSQL infinity with pandas max timestamp
                df[col] = pd.to_datetime(df[col])
                df.loc[df[col] >= pd.Timestamp('9999-01-01'), col] = PANDAS_MAX_TIMESTAMP
                
                # Keep as timestamp for processing (timestamp precision)
        
        # Handle as_of timestamp columns (preserve timestamp precision)
        for col in as_of_timestamp_columns:
            if col in df.columns:
                # Replace null with pandas max timestamp
                df[col] = df[col].fillna(PANDAS_MAX_TIMESTAMP)
                # Replace PostgreSQL infinity with pandas max timestamp
                df[col] = pd.to_datetime(df[col])
                df.loc[df[col] >= pd.Timestamp('9999-01-01'), col] = PANDAS_MAX_TIMESTAMP
                
                # Keep as timestamp for microsecond precision
                # Note: pandas uses nanosecond precision, which is compatible with Arrow timestamp[ns]
        
        # Add value_hash column if it doesn't exist (it will be computed by Rust)
        if 'value_hash' not in df.columns:
            df['value_hash'] = 0  # Placeholder, will be computed by Rust
        
        return df
    
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
                    # Handle pandas max timestamp which is too large for microseconds
                    # Cast with safe conversion that truncates nanoseconds
                    try:
                        column = column.cast(pa.timestamp('us'))
                    except pa.ArrowInvalid:
                        # If casting fails due to overflow, manually convert
                        # This happens with pd.Timestamp.max
                        np_array = column.to_pandas().values
                        # Convert to microseconds by dividing nanoseconds by 1000
                        us_values = np_array.astype('datetime64[us]')
                        column = pa.array(us_values, type=pa.timestamp('us'))
            
            # Convert effective timestamp columns from ns to us
            elif field.name in ['effective_from', 'effective_to'] and pa.types.is_timestamp(field.type):
                if field.type.unit == 'ns':
                    # Convert nanosecond timestamps to microsecond timestamps
                    try:
                        column = column.cast(pa.timestamp('us'))
                    except pa.ArrowInvalid:
                        # If casting fails due to overflow, manually convert
                        np_array = column.to_pandas().values
                        us_values = np_array.astype('datetime64[us]')
                        column = pa.array(us_values, type=pa.timestamp('us'))
            
            # Convert effective date columns from Date32 to Timestamp  
            elif field.name in ['effective_from', 'effective_to'] and pa.types.is_date32(field.type):
                # Convert Date32 to Timestamp (midnight for date-only values)
                pandas_series = column.to_pandas()
                timestamp_series = pd.to_datetime(pandas_series)
                column = pa.array(timestamp_series, type=pa.timestamp('us'))
            
            columns.append(column)
        
        # Create new schema with updated timestamp types
        new_fields = []
        for field in schema:
            if field.name in ['as_of_from', 'as_of_to', 'effective_from', 'effective_to'] and pa.types.is_timestamp(field.type):
                new_fields.append(pa.field(field.name, pa.timestamp('us'), field.nullable))
            elif field.name in ['effective_from', 'effective_to'] and pa.types.is_date32(field.type):
                new_fields.append(pa.field(field.name, pa.timestamp('us'), field.nullable))
            else:
                new_fields.append(field)
        
        new_schema = pa.schema(new_fields)
        return pa.RecordBatch.from_arrays(columns, schema=new_schema)
    
    def _convert_from_internal_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert from internal format back to PostgreSQL format.
        """
        df = df.copy()
        
        # Convert dates back to timestamps - handle effective and as_of columns differently
        effective_date_columns = ['effective_from', 'effective_to']
        as_of_timestamp_columns = ['as_of_from', 'as_of_to']
        
        # Convert effective date columns - force datetime.date objects to datetime
        for col in effective_date_columns:
            if col in df.columns:
                # Handle the case where Arrow returns date objects instead of timestamps
                def convert_to_datetime(val):
                    if isinstance(val, date) and not isinstance(val, datetime):
                        # Convert date to datetime at midnight
                        return datetime.combine(val, datetime.min.time())
                    return val
                
                df[col] = df[col].apply(convert_to_datetime)
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
                    is_max_timestamp_mask = df[col] >= pd.Timestamp('2262-04-01')
                    
                    infinity_mask = is_nat_mask | is_large_date_mask | is_max_timestamp_mask
                    
                    if infinity_mask.any():
                        # Replace infinity values with PostgreSQL infinity date
                        df.loc[infinity_mask, col] = POSTGRES_INFINITY
                        
                except (pd.errors.OutOfBoundsDatetime, OverflowError, AttributeError):
                    # If any conversion fails due to overflow, assume entire column needs infinity
                    df[col] = POSTGRES_INFINITY
        
        return df
    
    def validate_schema(self, df: pd.DataFrame) -> bool:
        """
        Validate that a DataFrame has the required schema.
        """
        required_cols = set(self.id_columns + self.value_columns + 
                           ['effective_from', 'effective_to', 'as_of_from', 'as_of_to'])
        return required_cols.issubset(set(df.columns))


# Helper function for PostgreSQL integration
def apply_changes_to_postgres(
    connection,
    rows_to_expire: pd.DataFrame,
    rows_to_insert: pd.DataFrame,
    table_name: str = 'timeseries_data',
    id_column: str = 'id'
):
    """
    Apply bitemporal changes to PostgreSQL database.
    
    This function handles the conversion between pandas timestamps and PostgreSQL
    infinity values. The rows_to_insert DataFrame should already have POSTGRES_INFINITY
    values for unbounded dates (as returned by compute_changes).
    
    Args:
        connection: psycopg2 connection or SQLAlchemy engine
        rows_to_expire: DataFrame with rows to expire
        rows_to_insert: DataFrame with rows to insert
        table_name: Name of the target table
        id_column: Primary key column name
    """
    from psycopg2.extras import execute_batch
    
    with connection.begin() if hasattr(connection, 'begin') else connection:
        cur = connection.cursor() if hasattr(connection, 'cursor') else connection
        
        # Expire rows
        if len(rows_to_expire) > 0:
            expire_data = [
                {
                    'as_of_to': row['as_of_to'],
                    'id': row[id_column]
                }
                for _, row in rows_to_expire.iterrows()
            ]
            
            execute_batch(cur, f"""
                UPDATE {table_name} 
                SET as_of_to = %(as_of_to)s 
                WHERE {id_column} = %(id)s
            """, expire_data)
        
        # Insert new rows
        if len(rows_to_insert) > 0:
            # Convert DataFrame to list of dicts
            insert_data = rows_to_insert.to_dict('records')
            
            # Build insert query dynamically based on columns
            columns = list(rows_to_insert.columns)
            placeholders = ', '.join([f'%({col})s' for col in columns])
            column_names = ', '.join(columns)
            
            execute_batch(cur, f"""
                INSERT INTO {table_name} ({column_names})
                VALUES ({placeholders})
            """, insert_data)
        
        if hasattr(connection, 'commit'):
            connection.commit()


# Performance optimization function for large datasets
def process_large_dataset_with_batching(
    processor: BitemporalTimeseriesProcessor,
    current_state_query: str,
    updates_query: str,
    connection,
    batch_size: int = 50000
):
    """
    Process very large datasets by batching.
    
    Args:
        processor: BitemporalTimeseriesProcessor instance
        current_state_query: SQL query for current state
        updates_query: SQL query for updates
        connection: Database connection
        batch_size: Number of rows to process at once
    """
    import math
    
    # Get total count
    total_updates = pd.read_sql_query(
        f"SELECT COUNT(*) as cnt FROM ({updates_query}) t", 
        connection
    ).iloc[0]['cnt']
    
    num_batches = math.ceil(total_updates / batch_size)
    
    for batch_num in range(num_batches):
        offset = batch_num * batch_size
        
        # Read current state for relevant IDs only
        batch_updates = pd.read_sql_query(
            f"{updates_query} LIMIT {batch_size} OFFSET {offset}",
            connection
        )
        
        # Build ID filter for current state
        id_conditions = []
        for _, row in batch_updates.iterrows():
            cond = " AND ".join([
                f"{col} = '{row[col]}'" 
                for col in processor.id_columns
            ])
            id_conditions.append(f"({cond})")
        
        id_filter = " OR ".join(id_conditions)
        
        # Read only relevant current state
        current_batch = pd.read_sql_query(
            f"{current_state_query} AND ({id_filter})",
            connection
        )
        
        # Process batch
        to_expire, to_insert = processor.compute_changes(
            current_batch,
            batch_updates
        )
        
        # Apply changes
        apply_changes_to_postgres(
            connection,
            to_expire,
            to_insert
        )
        
        print(f"Processed batch {batch_num + 1}/{num_batches}")
    
    print("Batch processing complete")


# Helper function to read data from PostgreSQL with infinity handling
def read_from_postgres_with_infinity(query: str, connection) -> pd.DataFrame:
    """
    Read data from PostgreSQL and handle infinity timestamps.
    
    PostgreSQL's 'infinity' timestamps are automatically converted to NaT by pandas.
    This function reads the data and replaces NaT with POSTGRES_INFINITY for
    consistency with the library's expectations.
    
    Args:
        query: SQL query to execute
        connection: Database connection
        
    Returns:
        DataFrame with infinity values properly represented
    """
    df = pd.read_sql_query(query, connection)
    
    # Replace NaT (from PostgreSQL infinity) with our POSTGRES_INFINITY constant
    date_columns = ['effective_to', 'as_of_to']
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].fillna(POSTGRES_INFINITY)
    
    return df
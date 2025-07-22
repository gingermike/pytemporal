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
import bitemporal_timeseries
from datetime import datetime

# PostgreSQL infinity date representation
POSTGRES_INFINITY = pd.Timestamp('9999-12-31 23:59:59')

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
        
        # Call Rust function
        expire_indices, insert_batch = bitemporal_timeseries.compute_changes(
            current_batch,
            updates_batch,
            self.id_columns,
            self.value_columns,
            system_date,
            update_mode
        )
        
        # Extract rows to expire from original DataFrame
        rows_to_expire = current_state.iloc[expire_indices].copy()
        rows_to_expire['as_of_to'] = system_date or pd.Timestamp.now().strftime('%Y-%m-%d')
        
        # Convert insert batch back to pandas
        rows_to_insert = insert_batch.to_pandas()
        rows_to_insert = self._convert_from_internal_format(rows_to_insert)
        
        return rows_to_expire, rows_to_insert
    
    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for processing by converting PostgreSQL infinity dates.
        """
        df = df.copy()
        
        # Convert PostgreSQL infinity to pandas max timestamp for internal processing
        date_columns = ['effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        for col in date_columns:
            if col in df.columns:
                # Replace null with pandas max timestamp
                df[col] = df[col].fillna(PANDAS_MAX_TIMESTAMP)
                # Replace PostgreSQL infinity with pandas max timestamp
                df[col] = pd.to_datetime(df[col])
                df.loc[df[col] >= pd.Timestamp('9999-01-01'), col] = PANDAS_MAX_TIMESTAMP
                
                # Convert to date for processing
                df[col] = df[col].dt.date
        
        return df
    
    def _convert_from_internal_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert from internal format back to PostgreSQL format.
        """
        df = df.copy()
        
        # Convert dates back to timestamps
        date_columns = ['effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        # Convert pandas max timestamp back to PostgreSQL infinity for unbounded dates
        unbounded_columns = ['effective_to', 'as_of_to']
        for col in unbounded_columns:
            if col in df.columns:
                # Any date beyond 2262 is treated as infinity
                df.loc[df[col] >= pd.Timestamp('2262-01-01'), col] = POSTGRES_INFINITY
        
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
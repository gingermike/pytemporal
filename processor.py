import pyarrow as pa
import pandas as pd
from typing import List, Tuple, Optional, Literal
import bitemporal_timeseries
from datetime import datetime

# PostgreSQL infinity date representation
POSTGRES_INFINITY = pd.Timestamp('9999-12-31 23:59:59')

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
        
        # Convert PostgreSQL infinity to internal representation
        date_columns = ['effective_from', 'effective_to', 'as_of_from', 'as_of_to']
        for col in date_columns:
            if col in df.columns:
                # Replace infinity/null with max date
                df[col] = df[col].fillna(POSTGRES_INFINITY)
                df[col] = df[col].replace(POSTGRES_INFINITY, pd.Timestamp('9999-12-31'))
                
                # Ensure date format
                df[col] = pd.to_datetime(df[col]).dt.date
        
        return df
    
    def _convert_from_internal_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert from internal format back to PostgreSQL format.
        """
        df = df.copy()
        
        # Convert max date back to PostgreSQL infinity
        date_columns = ['effective_to', 'as_of_to']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                # Replace max date with PostgreSQL infinity
                df.loc[df[col] >= pd.Timestamp('9999-12-30'), col] = POSTGRES_INFINITY
        
        # Ensure other date columns are also timestamps
        for col in ['effective_from', 'as_of_from']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
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


# Example usage
if __name__ == "__main__":
    # Example 1: Delta updates (default mode)
    processor = BitemporalTimeseriesProcessor(
        id_columns=['instrument_id', 'market'],
        value_columns=['price', 'volume', 'bid', 'ask']
    )
    
    # Current state from database
    current_state = pd.DataFrame({
        'instrument_id': ['AAPL', 'AAPL', 'GOOGL'],
        'market': ['NYSE', 'NYSE', 'NASDAQ'],
        'price': [150.0, 155.0, 2800.0],
        'volume': [1000000, 1200000, 500000],
        'bid': [149.95, 154.95, 2799.50],
        'ask': [150.05, 155.05, 2800.50],
        'effective_from': pd.to_datetime(['2024-01-01', '2024-07-01', '2024-01-01']),
        'effective_to': [POSTGRES_INFINITY, POSTGRES_INFINITY, POSTGRES_INFINITY],
        'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
        'as_of_to': [POSTGRES_INFINITY, POSTGRES_INFINITY, POSTGRES_INFINITY]
    })
    
    # Incoming updates (delta mode - only changes)
    updates = pd.DataFrame({
        'instrument_id': ['AAPL', 'GOOGL'],
        'market': ['NYSE', 'NASDAQ'],
        'price': [160.0, 2850.0],
        'volume': [1500000, 600000],
        'bid': [159.95, 2849.50],
        'ask': [160.05, 2850.50],
        'effective_from': pd.to_datetime(['2024-06-01', '2024-03-01']),
        'effective_to': pd.to_datetime(['2024-09-01', '2024-06-01']),
        'as_of_from': pd.to_datetime(['2024-07-21', '2024-07-21']),
        'as_of_to': [POSTGRES_INFINITY, POSTGRES_INFINITY]
    })
    
    # Compute changes
    to_expire, to_insert = processor.compute_changes(
        current_state, 
        updates,
        system_date='2024-07-21',
        update_mode='delta'
    )
    
    print("Delta Update Results:")
    print(f"Rows to expire: {len(to_expire)}")
    print(f"Rows to insert: {len(to_insert)}")
    
    # Example 2: Full state update
    # In this mode, any ID not in the updates will be expired
    full_state_updates = pd.DataFrame({
        'instrument_id': ['AAPL'],  # GOOGL is missing, so it will be expired
        'market': ['NYSE'],
        'price': [165.0],
        'volume': [2000000],
        'bid': [164.95],
        'ask': [165.05],
        'effective_from': pd.to_datetime(['2024-01-01']),
        'effective_to': [POSTGRES_INFINITY],
        'as_of_from': pd.to_datetime(['2024-07-22']),
        'as_of_to': [POSTGRES_INFINITY]
    })
    
    to_expire_full, to_insert_full = processor.compute_changes(
        current_state,
        full_state_updates,
        system_date='2024-07-22',
        update_mode='full_state'
    )
    
    print("\nFull State Update Results:")
    print(f"Rows to expire: {len(to_expire_full)}")
    print(f"Rows to insert: {len(to_insert_full)}")
    print("Note: GOOGL records should be expired as they're not in the full state")


# Advanced usage with PostgreSQL
def process_with_postgres_example():
    """
    Example of processing with PostgreSQL database.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        database="timeseries_db",
        user="user",
        password="password"
    )
    
    try:
        # Create processor
        processor = BitemporalTimeseriesProcessor(
            id_columns=['instrument_id', 'exchange'],
            value_columns=['price', 'volume']
        )
        
        # Read current state
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM market_data 
                WHERE as_of_to = 'infinity'::timestamp
                ORDER BY instrument_id, exchange, effective_from
            """)
            current_state = pd.DataFrame(cur.fetchall())
        
        # Read updates from staging table
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM market_data_staging")
            updates = pd.DataFrame(cur.fetchall())
        
        # Determine update mode based on staging metadata
        with conn.cursor() as cur:
            cur.execute("SELECT update_mode FROM staging_metadata LIMIT 1")
            update_mode = cur.fetchone()[0]  # 'delta' or 'full_state'
        
        # Process changes
        rows_to_expire, rows_to_insert = processor.compute_changes(
            current_state,
            updates,
            update_mode=update_mode
        )
        
        # Apply changes
        apply_changes_to_postgres(
            conn,
            rows_to_expire,
            rows_to_insert,
            table_name='market_data'
        )
        
        print(f"Successfully processed {len(updates)} updates")
        print(f"Expired {len(rows_to_expire)} rows")
        print(f"Inserted {len(rows_to_insert)} rows")
        
    finally:
        conn.close()


# Performance optimization for large datasets
def process_large_dataset_with_batching(
    processor: BitemporalTimeseriesProcessor,
    current_state_query: str,
    updates_query: str,
    connection,
    batch_size: int = 50000
):
    """
    Process very large datasets by batching.
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
    
    print("Batch processing complete")import pyarrow as pa
import pandas as pd
from typing import List, Tuple, Optional
import bitemporal_timeseries

class BitemporalTimeseriesProcessor:
    """
    A processor for bitemporal timeseries data that efficiently computes
    changes between current state and incoming updates.
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
        system_date: Optional[str] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Compute the changes needed to update the bitemporal timeseries.
        
        Args:
            current_state: DataFrame with current database state
            updates: DataFrame with incoming updates
            system_date: Optional system date (YYYY-MM-DD format)
            
        Returns:
            Tuple of (rows_to_expire, rows_to_insert)
            - rows_to_expire: DataFrame with rows that need as_of_to set
            - rows_to_insert: DataFrame with new rows to insert
        """
        # Convert pandas DataFrames to Arrow RecordBatches
        current_batch = pa.RecordBatch.from_pandas(current_state)
        updates_batch = pa.RecordBatch.from_pandas(updates)
        
        # Call Rust function
        expire_indices, insert_batch = bitemporal_timeseries.compute_changes(
            current_batch,
            updates_batch,
            self.id_columns,
            self.value_columns,
            system_date
        )
        
        # Extract rows to expire from original DataFrame
        rows_to_expire = current_state.iloc[expire_indices].copy()
        rows_to_expire['as_of_to'] = system_date or pd.Timestamp.now().strftime('%Y-%m-%d')
        
        # Convert insert batch back to pandas
        rows_to_insert = insert_batch.to_pandas()
        
        return rows_to_expire, rows_to_insert
    
    def validate_schema(self, df: pd.DataFrame) -> bool:
        """
        Validate that a DataFrame has the required schema.
        """
        required_cols = set(self.id_columns + self.value_columns + 
                           ['effective_from', 'effective_to', 'as_of_from', 'as_of_to'])
        return required_cols.issubset(set(df.columns))


# Example usage
if __name__ == "__main__":
    # Example with financial timeseries data
    processor = BitemporalTimeseriesProcessor(
        id_columns=['instrument_id', 'market'],
        value_columns=['price', 'volume', 'bid', 'ask']
    )
    
    # Current state from database
    current_state = pd.DataFrame({
        'instrument_id': ['AAPL', 'AAPL', 'GOOGL'],
        'market': ['NYSE', 'NYSE', 'NASDAQ'],
        'price': [150.0, 155.0, 2800.0],
        'volume': [1000000, 1200000, 500000],
        'bid': [149.95, 154.95, 2799.50],
        'ask': [150.05, 155.05, 2800.50],
        'effective_from': pd.to_datetime(['2024-01-01', '2024-07-01', '2024-01-01']),
        'effective_to': pd.to_datetime(['2024-07-01', '2024-12-31', '2024-12-31']),
        'as_of_from': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-01']),
        'as_of_to': [None, None, None]
    })
    
    # Incoming updates
    updates = pd.DataFrame({
        'instrument_id': ['AAPL', 'GOOGL'],
        'market': ['NYSE', 'NASDAQ'],
        'price': [160.0, 2850.0],
        'volume': [1500000, 600000],
        'bid': [159.95, 2849.50],
        'ask': [160.05, 2850.50],
        'effective_from': pd.to_datetime(['2024-06-01', '2024-03-01']),
        'effective_to': pd.to_datetime(['2024-09-01', '2024-06-01']),
        'as_of_from': pd.to_datetime(['2024-07-21', '2024-07-21']),
        'as_of_to': [None, None]
    })
    
    # Compute changes
    to_expire, to_insert = processor.compute_changes(
        current_state, 
        updates,
        system_date='2024-07-21'
    )
    
    print("Rows to expire (set as_of_to):")
    print(to_expire)
    print("\nRows to insert:")
    print(to_insert)


# Advanced usage with Arrow arrays directly for better performance
def process_large_dataset_with_arrow(
    current_state_path: str,
    updates_path: str,
    id_columns: List[str],
    value_columns: List[str]
) -> Tuple[pa.Table, pa.Table]:
    """
    Process large datasets using Arrow directly for maximum performance.
    """
    # Read parquet files directly into Arrow tables
    current_table = pa.parquet.read_table(current_state_path)
    updates_table = pa.parquet.read_table(updates_path)
    
    # Process in batches for memory efficiency
    batch_size = 100000
    all_expire_indices = []
    all_insert_batches = []
    
    for i in range(0, current_table.num_rows, batch_size):
        current_batch = current_table.slice(i, min(batch_size, current_table.num_rows - i)).to_batches()[0]
        
        # Process updates in chunks too
        for j in range(0, updates_table.num_rows, batch_size):
            updates_batch = updates_table.slice(j, min(batch_size, updates_table.num_rows - j)).to_batches()[0]
            
            expire_indices, insert_batch = bitemporal_timeseries.compute_changes(
                current_batch,
                updates_batch,
                id_columns,
                value_columns
            )
            
            # Adjust indices for the current batch offset
            adjusted_indices = [idx + i for idx in expire_indices]
            all_expire_indices.extend(adjusted_indices)
            all_insert_batches.append(insert_batch)
    
    # Combine results
    rows_to_expire = current_table.take(all_expire_indices)
    rows_to_insert = pa.Table.from_batches(all_insert_batches)
    
    return rows_to_expire, rows_to_insert
use crate::HashAlgorithm;
use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::array::{Int8Array, Int16Array, Int32Array, Int64Array};
use arrow::array::{Float32Array, Float64Array, BooleanArray};
use arrow::array::{Date32Array, Date64Array, Decimal128Array};
use arrow::array::{TimestampSecondArray, TimestampMillisecondArray, TimestampMicrosecondArray, TimestampNanosecondArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

/// Fast hash computation directly on Arrow arrays without deserialization
pub fn hash_values_batch_arrow_direct(
    record_batch: &RecordBatch, 
    row_indices: &[usize], 
    value_columns: &[String],
    algorithm: HashAlgorithm,
) -> Vec<String> {
    let mut hashes = Vec::with_capacity(row_indices.len());
    
    // Pre-compute column indices and arrays to avoid repeated lookups
    let col_data: Vec<(&str, &ArrayRef)> = value_columns.iter()
        .map(|col_name| {
            let col_idx = record_batch.schema().index_of(col_name).unwrap();
            (col_name.as_str(), record_batch.column(col_idx))
        })
        .collect();
    
    for &row_idx in row_indices {
        let mut hasher_input = Vec::with_capacity(1024); // Pre-allocate reasonable buffer
        
        // Hash each column's raw bytes directly without conversion to ScalarValue
        for (_col_name, array) in &col_data {
            hash_array_value_direct(array, row_idx, &mut hasher_input);
        }
        
        let hash_result = match algorithm {
            HashAlgorithm::XxHash => {
                use xxhash_rust::xxh64::xxh64;
                format!("{:016x}", xxh64(&hasher_input, 0))
            },
            HashAlgorithm::Sha256 => {
                use sha2::{Sha256, Digest};
                let mut hasher = Sha256::new();
                hasher.update(&hasher_input);
                format!("{:x}", hasher.finalize())
            },
        };
        hashes.push(hash_result);
    }
    
    hashes
}

/// Hash a single array value directly without Arrow→Rust conversion
fn hash_array_value_direct(array: &ArrayRef, row_idx: usize, hasher_input: &mut Vec<u8>) {
    // Handle null values consistently
    if array.is_null(row_idx) {
        hasher_input.extend_from_slice(b"NULL");
        return;
    }
    
    match array.data_type() {
        DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            let value = string_array.value(row_idx);
            hasher_input.extend_from_slice(value.as_bytes());
        },
        
        DataType::Int8 => {
            let int_array = array.as_any().downcast_ref::<Int8Array>().unwrap();
            let value = int_array.value(row_idx);
            // Normalize to 64-bit for consistency
            hasher_input.extend_from_slice(&(value as i64).to_le_bytes());
        },
        
        DataType::Int16 => {
            let int_array = array.as_any().downcast_ref::<Int16Array>().unwrap();
            let value = int_array.value(row_idx);
            hasher_input.extend_from_slice(&(value as i64).to_le_bytes());
        },
        
        DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            let value = int_array.value(row_idx);
            hasher_input.extend_from_slice(&(value as i64).to_le_bytes());
        },
        
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let value = int_array.value(row_idx);
            hasher_input.extend_from_slice(&value.to_le_bytes());
        },
        
        DataType::Float32 => {
            let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            let value = float_array.value(row_idx);
            
            // Check if this is actually an integer value stored as float
            if value.fract() == 0.0 && value.is_finite() && value >= i64::MIN as f32 && value <= i64::MAX as f32 {
                // This is an integer value - normalize to Int64 for consistency  
                let i64_val = value as i64;
                hasher_input.extend_from_slice(&i64_val.to_le_bytes());
            } else {
                // This is a true float value - promote to f64 for consistency
                let f64_val = value as f64;
                hasher_input.extend_from_slice(&f64_val.to_le_bytes());
            }
        },
        
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let value = float_array.value(row_idx);
            
            // Check if this is actually an integer value stored as float
            if value.fract() == 0.0 && value.is_finite() && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
                // This is an integer value - normalize to Int64 for consistency  
                let i64_val = value as i64;
                hasher_input.extend_from_slice(&i64_val.to_le_bytes());
            } else {
                // This is a true float value
                hasher_input.extend_from_slice(&value.to_le_bytes());
            }
        },
        
        DataType::Boolean => {
            let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let value = bool_array.value(row_idx);
            hasher_input.push(if value { 1u8 } else { 0u8 });
        },
        
        DataType::Date32 => {
            let date_array = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let value = date_array.value(row_idx);
            hasher_input.extend_from_slice(&value.to_le_bytes());
        },
        
        DataType::Date64 => {
            let date_array = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let value = date_array.value(row_idx);
            hasher_input.extend_from_slice(&value.to_le_bytes());
        },
        
        DataType::Timestamp(_, _) => {
            // Handle all timestamp precisions
            match array.data_type() {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => {
                    let ts_array = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                    let value = ts_array.value(row_idx);
                    hasher_input.extend_from_slice(&value.to_le_bytes());
                },
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                    let ts_array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                    let value = ts_array.value(row_idx);
                    hasher_input.extend_from_slice(&value.to_le_bytes());
                },
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => {
                    let ts_array = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                    let value = ts_array.value(row_idx);
                    hasher_input.extend_from_slice(&value.to_le_bytes());
                },
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                    let ts_array = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                    let value = ts_array.value(row_idx);
                    hasher_input.extend_from_slice(&value.to_le_bytes());
                },
                _ => unreachable!()
            }
        },
        
        DataType::Decimal128(_, _) => {
            let decimal_array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            let value = decimal_array.value(row_idx);
            hasher_input.extend_from_slice(&value.to_le_bytes());
        },
        
        _ => {
            // Fallback to string representation for unsupported types
            // This shouldn't happen with our supported types but provides safety
            let debug_str = format!("{:?}", array.slice(row_idx, 1));
            hasher_input.extend_from_slice(debug_str.as_bytes());
        }
    }
}

/// Fast add hash column using direct Arrow hashing
pub fn add_hash_column_arrow_direct(
    record_batch: &RecordBatch,
    value_columns: &[String],
    algorithm: HashAlgorithm,
) -> Result<RecordBatch, String> {
    let num_rows = record_batch.num_rows();
    if num_rows == 0 {
        return Err("Cannot add hash column to empty RecordBatch".to_string());
    }
    
    // Validate that all value columns exist
    for col_name in value_columns {
        if record_batch.schema().index_of(col_name).is_err() {
            return Err(format!("Column '{}' not found in RecordBatch", col_name));
        }
    }
    
    // Use the fast Arrow-direct hash computation
    let row_indices: Vec<usize> = (0..num_rows).collect();
    let hash_values_string = hash_values_batch_arrow_direct(record_batch, &row_indices, value_columns, algorithm);
    
    // Create the hash column
    let hash_array = Arc::new(StringArray::from(hash_values_string));
    
    // Check if value_hash column already exists
    let hash_column_index = record_batch.schema().index_of("value_hash");
    
    let (new_schema, new_columns) = if let Ok(hash_idx) = hash_column_index {
        // Replace existing value_hash column
        let new_fields: Vec<Arc<arrow::datatypes::Field>> = record_batch.schema().fields().iter().cloned().collect();
        let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
        
        let mut new_columns: Vec<ArrayRef> = record_batch.columns().to_vec();
        new_columns[hash_idx] = hash_array;
        (new_schema, new_columns)
    } else {
        // Add new value_hash column
        let mut new_fields: Vec<Arc<arrow::datatypes::Field>> = record_batch.schema().fields().iter().cloned().collect();
        new_fields.push(Arc::new(arrow::datatypes::Field::new("value_hash", arrow::datatypes::DataType::Utf8, false)));
        let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
        
        let mut new_columns: Vec<ArrayRef> = record_batch.columns().to_vec();
        new_columns.push(hash_array);
        (new_schema, new_columns)
    };
    
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| e.to_string())
}
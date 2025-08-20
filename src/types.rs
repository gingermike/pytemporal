use arrow::array::{Array, ArrayRef, Date32Array, TimestampMicrosecondArray, TimestampNanosecondArray, TimestampSecondArray, TimestampMillisecondArray, RecordBatch, StringArray, Int32Array, Int64Array, Float64Array};
use arrow::datatypes::DataType;
use chrono::{NaiveDate, NaiveDateTime};
use ordered_float;

#[derive(Debug, Clone)]
pub struct BitemporalRecord {
    pub id_values: Vec<ScalarValue>,
    pub value_hash: u64,
    pub effective_from: NaiveDateTime,
    pub effective_to: NaiveDateTime,
    pub as_of_from: NaiveDateTime,
    pub as_of_to: NaiveDateTime,
    pub original_index: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UpdateMode {
    Delta,
    FullState,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ScalarValue {
    String(String),
    Int32(i32),
    Int64(i64),
    Float64(ordered_float::OrderedFloat<f64>),
    Date32(i32),
}

impl ScalarValue {
    pub fn from_array(array: &ArrayRef, idx: usize) -> Self {
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
            DataType::Timestamp(unit, _) => {
                use arrow::datatypes::TimeUnit;
                match unit {
                    TimeUnit::Second => {
                        let arr = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                        ScalarValue::Int64(arr.value(idx))
                    }
                    TimeUnit::Millisecond => {
                        let arr = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                        ScalarValue::Int64(arr.value(idx))
                    }
                    TimeUnit::Microsecond => {
                        let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                        ScalarValue::Int64(arr.value(idx))
                    }
                    TimeUnit::Nanosecond => {
                        let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                        ScalarValue::Int64(arr.value(idx))
                    }
                }
            }
            _ => panic!("Unsupported data type: {:?}", array.data_type()),
        }
    }
}

#[derive(Debug)]
pub struct ChangeSet {
    pub to_expire: Vec<usize>,
    pub to_insert: Vec<RecordBatch>,
}

#[derive(Debug, Clone)]
pub struct TimelineEvent {
    pub date: NaiveDateTime,
    pub event_type: EventType,
    pub record: BitemporalRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventType {
    CurrentStart,
    CurrentEnd,
    UpdateStart,
    UpdateEnd,
}

// Pandas-compatible max datetime (pandas can't handle dates beyond ~2262)
pub const MAX_DATETIME: NaiveDateTime = match NaiveDate::from_ymd_opt(2262, 4, 11) {
    Some(date) => match date.and_hms_opt(23, 59, 59) {
        Some(datetime) => datetime,
        None => panic!("Invalid max time"),
    },
    None => panic!("Invalid max date"),
};

// Max timestamp for as_of columns (microsecond precision)
pub const MAX_TIMESTAMP: NaiveDateTime = match NaiveDate::from_ymd_opt(2262, 4, 11) {
    Some(date) => match date.and_hms_opt(23, 59, 59) {
        Some(datetime) => datetime,
        None => panic!("Invalid max timestamp"),
    },
    None => panic!("Invalid max date"),
};

/// Batch collector that accumulates records to process them in batches instead of individually
#[derive(Debug)]
pub struct BatchCollector {
    /// Records to be processed from current state
    pub current_records: Vec<BitemporalRecord>,
    /// Source row indices for current_records
    pub current_source_rows: Vec<usize>,
    /// Records to be processed from updates
    pub update_records: Vec<BitemporalRecord>,  
    /// Source row indices for update_records
    pub update_source_rows: Vec<usize>,
}

impl BatchCollector {
    pub fn new() -> Self {
        Self {
            current_records: Vec::new(),
            current_source_rows: Vec::new(),
            update_records: Vec::new(),
            update_source_rows: Vec::new(),
        }
    }
    
    pub fn add_current_record(&mut self, record: BitemporalRecord, source_row: usize) {
        self.current_records.push(record);
        self.current_source_rows.push(source_row);
    }
    
    pub fn add_update_record(&mut self, record: BitemporalRecord, source_row: usize) {
        self.update_records.push(record);
        self.update_source_rows.push(source_row);
    }
    
    /// For temporary compatibility - directly add a RecordBatch
    pub fn add_batch(&mut self, _batch: RecordBatch) {
        // For now, this is a no-op since we're using it just for segments
        // In a full implementation, we'd collect these batches too
    }
    
    pub fn is_empty(&self) -> bool {
        self.current_records.is_empty() && self.update_records.is_empty()
    }
    
    pub fn len(&self) -> usize {
        self.current_records.len() + self.update_records.len()
    }
    
    /// Flush accumulated records into RecordBatches and clear the collector
    pub fn flush(
        &mut self, 
        current_batch: &RecordBatch, 
        updates_batch: &RecordBatch
    ) -> Result<Vec<RecordBatch>, String> {
        let mut batches = Vec::new();
        
        // Create batch from current records
        if !self.current_records.is_empty() {
            let batch = crate::batch_utils::create_record_batch_from_records(
                &self.current_records,
                current_batch,
                &self.current_source_rows,
            )?;
            batches.push(batch);
        }
        
        // Create batch from update records  
        if !self.update_records.is_empty() {
            let batch = crate::batch_utils::create_record_batch_from_records(
                &self.update_records,
                updates_batch,
                &self.update_source_rows,
            )?;
            batches.push(batch);
        }
        
        // Clear accumulated records
        self.current_records.clear();
        self.current_source_rows.clear();
        self.update_records.clear();
        self.update_source_rows.clear();
        
        Ok(batches)
    }
}
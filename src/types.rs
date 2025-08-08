use arrow::array::{Array, ArrayRef, Date32Array, TimestampMicrosecondArray, RecordBatch, StringArray, Int32Array, Int64Array, Float64Array};
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
            DataType::Timestamp(_, _) => {
                let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                ScalarValue::Int64(arr.value(idx))
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
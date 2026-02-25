use serde::{Serialize, Deserialize};
// use datafusion::common::ScalarValue; // Avoiding direct dependency on datafusion here if possible to keep executor light? 
// No, executor will need it if we want full compat. But `executor` is my custom one.
// Let's keep `SimpleValue` pure Rust for now, and have `table_provider` convert to `ScalarValue`.

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SimpleValue {
    Null,
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
}

impl std::fmt::Display for SimpleValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SimpleValue::Null => write!(f, "NULL"),
            SimpleValue::Int(v) => write!(f, "{}", v),
            SimpleValue::Float(v) => write!(f, "{}", v),
            SimpleValue::String(v) => write!(f, "{}", v),
            SimpleValue::Bool(v) => write!(f, "{}", v),
        }
    }
}

// src/protocol/type_map.rs
use arrow::datatypes::DataType;

/// Map an Arrow DataType to the corresponding PostgreSQL OID (text format).
pub fn arrow_to_pg_oid(dt: &DataType) -> i32 {
    match dt {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16 => 23,  // INT4
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => 20, // INT8
        DataType::Float32 => 700,  // FLOAT4
        DataType::Float64 => 701,  // FLOAT8
        DataType::Utf8 | DataType::LargeUtf8 => 25,  // TEXT
        DataType::Boolean => 16,   // BOOL
        DataType::Date32 => 1082,  // DATE
        DataType::Timestamp(_, _) => 1114, // TIMESTAMP
        DataType::Binary | DataType::LargeBinary => 17, // BYTEA
        _ => 25, // TEXT fallback (safe for everything else)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn test_int32_maps_to_int4() {
        assert_eq!(arrow_to_pg_oid(&DataType::Int32), 23);
    }

    #[test]
    fn test_int8_maps_to_int4() {
        assert_eq!(arrow_to_pg_oid(&DataType::Int8), 23);
    }

    #[test]
    fn test_int64_maps_to_int8() {
        assert_eq!(arrow_to_pg_oid(&DataType::Int64), 20);
    }

    #[test]
    fn test_float32_maps_to_float4() {
        assert_eq!(arrow_to_pg_oid(&DataType::Float32), 700);
    }

    #[test]
    fn test_float64_maps_to_float8() {
        assert_eq!(arrow_to_pg_oid(&DataType::Float64), 701);
    }

    #[test]
    fn test_utf8_maps_to_text() {
        assert_eq!(arrow_to_pg_oid(&DataType::Utf8), 25);
    }

    #[test]
    fn test_bool_maps_to_bool() {
        assert_eq!(arrow_to_pg_oid(&DataType::Boolean), 16);
    }

    #[test]
    fn test_unknown_maps_to_text_fallback() {
        // List is not in the mapping — should fall back to TEXT
        assert_eq!(arrow_to_pg_oid(&DataType::List(
            std::sync::Arc::new(arrow::datatypes::Field::new("item", DataType::Int32, true))
        )), 25);
    }
}

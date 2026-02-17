# ONNX Predict UDF

MiracleDB provides two UDFs for running ML inference with ONNX models:

## 1. `predict()` - Variadic Arguments

Takes individual feature columns as separate arguments:

```sql
-- Load model
CREATE MODEL my_model FROM '/path/to/model.onnx';

-- Predict with individual columns
SELECT
    id,
    predict('my_model', feature1, feature2, feature3) as prediction
FROM my_table;
```

**Signature**: `predict(model_name: String, feature1: Float, feature2: Float, ...) -> Float32`

## 2. `onnx_predict()` - Array Features

Takes features as a single array:

```sql
-- Load model
CREATE MODEL my_model FROM '/path/to/model.onnx';

-- Predict with array syntax
SELECT
    id,
    onnx_predict('my_model', ARRAY[feature1, feature2, feature3]) as prediction
FROM my_table;

-- Or with literal arrays
SELECT onnx_predict('my_model', ARRAY[1.0, 2.0, 3.0]) as prediction;
```

**Signature**: `onnx_predict(model_name: String, features: List<Float32>) -> Float32`

## NULL Handling

Both UDFs handle NULL values gracefully:
- NULL model name → NULL result
- NULL features → NULL result
- NULL in feature array → NULL result

## Error Handling

- **Model not found**: Returns error if model hasn't been loaded via `CREATE MODEL`
- **Invalid input type**: Returns error if features are not numeric
- **ONNX runtime error**: Returns error if model inference fails

## When to Use Each

- **Use `predict()`** when working with columnar data where each feature is a separate column
- **Use `onnx_predict()`** when:
  - Features are already in array format
  - You want to pass dynamic feature arrays
  - You need to construct feature vectors programmatically

## Example: Image Classification

```sql
-- Load MobileNet model
CREATE MODEL mobilenet FROM 's3://models/mobilenet-v2.onnx';

-- Predict on preprocessed image features
SELECT
    image_id,
    onnx_predict('mobilenet', pixel_array) as class_score
FROM images;
```

## Example: Credit Scoring

```sql
-- Load credit model
CREATE MODEL credit_model FROM '/models/credit_score.onnx';

-- Predict with individual features
SELECT
    customer_id,
    predict('credit_model', income, age, debt_ratio, credit_history) as credit_score
FROM customers;
```

## Implementation Details

- Both UDFs use the **ort 2.0 API** for ONNX Runtime
- Models are cached in memory for fast inference
- Thread-safe: Multiple queries can use the same model concurrently
- Batch processing: Processes all rows in query efficiently

## Compilation

Enable the `ml` feature to use ONNX UDFs:

```bash
cargo build --features ml
```

Or add to `Cargo.toml`:

```toml
[dependencies]
miracledb = { version = "0.1", features = ["ml"] }
```

//! WASM User-Defined Functions (UDFs)
//!
//! Provides a secure runtime for executing WebAssembly-based user-defined functions
//! in SQL queries using Wasmer.

use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use datafusion::logical_expr::{ScalarUDF, Volatility, create_udf};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::arrow::array::{Array, ArrayRef, StringArray, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DataFusionResult;
use wasmer::{Store, Module, Instance, Value, Imports};
use serde::{Deserialize, Serialize};

/// WASM UDF Manager - manages loaded WASM modules and function instances
pub struct WasmUdfManager {
    modules: Arc<RwLock<HashMap<String, WasmModule>>>,
    store: Store,
}

/// Metadata for a loaded WASM module
struct WasmModule {
    module: Module,
    instance: Instance,
    store: Store,
    exports: Vec<String>,
}

/// Function signature information
#[derive(Clone, Debug)]
pub struct FunctionSignature {
    pub params: Vec<wasmer::Type>,
    pub results: Vec<wasmer::Type>,
}

impl WasmModule {
    /// Get the signature of an exported function
    pub fn get_function_signature(&self, name: &str) -> DataFusionResult<FunctionSignature> {
        // Create a temporary instance to introspect the function
        let mut store = Store::default();
        let import_object = Imports::new();
        let instance = Instance::new(&mut store, &self.module, &import_object)
            .map_err(|e| datafusion::error::DataFusionError::Execution(
                format!("Failed to create instance: {}", e)
            ))?;

        let func = instance.exports.get_function(name)
            .map_err(|e| datafusion::error::DataFusionError::Execution(
                format!("Function '{}' not found: {}", name, e)
            ))?;

        let ty = func.ty(&store);

        Ok(FunctionSignature {
            params: ty.params().to_vec(),
            results: ty.results().to_vec(),
        })
    }

    /// Get the number of parameters a function accepts
    pub fn get_function_arity(&self, name: &str) -> DataFusionResult<usize> {
        let sig = self.get_function_signature(name)?;
        Ok(sig.params.len())
    }

    /// List all exported functions with their signatures
    pub fn list_exports(&self) -> Vec<(String, FunctionSignature)> {
        let mut exports = Vec::new();

        // Create a temporary instance to introspect functions
        let mut store = Store::default();
        let import_object = Imports::new();
        let Ok(instance) = Instance::new(&mut store, &self.module, &import_object) else {
            return exports;
        };

        for (name, _) in instance.exports.iter() {
            if let Ok(func) = instance.exports.get_function(&name) {
                let ty = func.ty(&store);
                exports.push((
                    name.to_string(),
                    FunctionSignature {
                        params: ty.params().to_vec(),
                        results: ty.results().to_vec(),
                    },
                ));
            }
        }

        exports
    }
}

/// Configuration for a WASM UDF
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WasmUdfConfig {
    pub name: String,
    pub wasm_path: String,
    pub function_name: String,
    pub return_type: WasmDataType,
    pub arg_types: Vec<WasmDataType>,
}

/// Supported data types for WASM UDFs
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WasmDataType {
    Int64,
    Float64,
    String,
}

impl WasmUdfManager {
    /// Create a new WASM UDF manager
    pub fn new() -> Self {
        Self {
            modules: Arc::new(RwLock::new(HashMap::new())),
            store: Store::default(),
        }
    }

    /// Load a WASM module from a file
    pub fn load_module(
        &mut self,
        name: &str,
        wasm_bytes: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Loading WASM module: {}", name);

        // Create a new store for this module
        let mut store = Store::default();

        // Compile the WASM module
        let module = Module::new(&store, wasm_bytes)?;

        // Get exported functions
        let exports = module
            .exports()
            .functions()
            .map(|f| f.name().to_string())
            .collect();

        // Create an instance
        let import_object = Imports::new();
        let instance = Instance::new(&mut store, &module, &import_object)?;

        let wasm_module = WasmModule {
            module,
            instance,
            store,
            exports
        };

        let mut modules = self.modules.write().unwrap();
        modules.insert(name.to_string(), wasm_module);

        tracing::info!("WASM module '{}' loaded successfully", name);

        Ok(())
    }

    /// Load a WASM module from a file path
    pub fn load_module_from_file(
        &mut self,
        name: &str,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let wasm_bytes = std::fs::read(path)?;
        self.load_module(name, &wasm_bytes)
    }

    /// Check if a module is loaded
    pub fn has_module(&self, name: &str) -> bool {
        let modules = self.modules.read().unwrap();
        modules.contains_key(name)
    }

    /// Get list of exported functions from a module
    pub fn get_exports(&self, module_name: &str) -> Option<Vec<String>> {
        let modules = self.modules.read().unwrap();
        modules.get(module_name).map(|m| m.exports.clone())
    }

    /// Execute a WASM function with i64 inputs and i64 output
    pub fn call_i64(
        &mut self,
        module_name: &str,
        function_name: &str,
        args: &[i64],
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let modules = self.modules.read().unwrap();
        let wasm_module = modules
            .get(module_name)
            .ok_or_else(|| format!("WASM module '{}' not found", module_name))?;

        // Create a new instance for this execution
        let import_object = Imports::new();
        let instance = Instance::new(&mut self.store, &wasm_module.module, &import_object)?;

        // Get the function
        let func = instance
            .exports
            .get_function(function_name)?;

        // Convert args to WASM values
        let wasm_args: Vec<Value> = args.iter().map(|&v| Value::I64(v)).collect();

        // Call the function
        let result = func.call(&mut self.store, &wasm_args)?;

        // Extract result
        if let Some(Value::I64(val)) = result.first() {
            Ok(*val)
        } else {
            Err("Function did not return i64".into())
        }
    }

    /// Execute a WASM function with f64 inputs and f64 output
    pub fn call_f64(
        &mut self,
        module_name: &str,
        function_name: &str,
        args: &[f64],
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let modules = self.modules.read().unwrap();
        let wasm_module = modules
            .get(module_name)
            .ok_or_else(|| format!("WASM module '{}' not found", module_name))?;

        let import_object = Imports::new();
        let instance = Instance::new(&mut self.store, &wasm_module.module, &import_object)?;

        let func = instance.exports.get_function(function_name)?;

        let wasm_args: Vec<Value> = args.iter().map(|&v| Value::F64(v)).collect();

        let result = func.call(&mut self.store, &wasm_args)?;

        if let Some(Value::F64(val)) = result.first() {
            Ok(*val)
        } else {
            Err("Function did not return f64".into())
        }
    }

    /// Register a WASM function as a DataFusion UDF (supports multi-argument)
    pub fn register_function(
        &self,
        ctx: &datafusion::execution::context::SessionContext,
        module_name: &str,
        wasm_func_name: &str,
        udf_name: &str,
    ) -> DataFusionResult<()> {
        let modules = self.modules.read().unwrap();
        let module = modules.get(module_name).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!("Module '{}' not found", module_name))
        })?;

        // Get function signature
        let sig = module.get_function_signature(wasm_func_name)?;

        // Validate: all params must be same type, single return value
        if sig.results.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                format!("Function must return exactly 1 value, got {}", sig.results.len())
            ));
        }

        if sig.params.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "Function must have at least 1 parameter".to_string()
            ));
        }

        // Check if all params are the same type
        let first_param_type = sig.params[0];
        if !sig.params.iter().all(|t| *t == first_param_type) {
            return Err(datafusion::error::DataFusionError::Execution(
                "All parameters must be the same type (homogeneous)".to_string()
            ));
        }

        // Register based on type
        match (first_param_type, sig.results[0]) {
            (wasmer::Type::I64, wasmer::Type::I64) => {
                self.register_i64_function_multi(
                    ctx,
                    module_name,
                    wasm_func_name,
                    udf_name,
                    sig.params.len(),
                )
            }
            (wasmer::Type::F64, wasmer::Type::F64) => {
                self.register_f64_function_multi(
                    ctx,
                    module_name,
                    wasm_func_name,
                    udf_name,
                    sig.params.len(),
                )
            }
            _ => Err(datafusion::error::DataFusionError::Execution(
                "Only i64->i64 and f64->f64 functions are supported".to_string()
            )),
        }
    }

    /// Register a WASM function as a DataFusion UDF
    pub fn register_udf(
        &self,
        ctx: &datafusion::execution::context::SessionContext,
        config: WasmUdfConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Registering WASM UDF: {}", config.name);

        // Use the new register_function method which auto-detects arity
        self.register_function(
            ctx,
            &config.wasm_path,
            &config.function_name,
            &config.name,
        ).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        Ok(())
    }

    /// Register multi-argument i64 function
    fn register_i64_function_multi(
        &self,
        ctx: &datafusion::execution::context::SessionContext,
        module_name: &str,
        wasm_func_name: &str,
        udf_name: &str,
        arity: usize,
    ) -> DataFusionResult<()> {
        let modules = self.modules.clone();
        let module_name = module_name.to_string();
        let wasm_func_name = wasm_func_name.to_string();

        let func = move |args: &[ArrayRef]| -> DataFusionResult<ArrayRef> {
            if args.len() != arity {
                return Err(datafusion::error::DataFusionError::Execution(
                    format!("Expected {} arguments, got {}", arity, args.len())
                ));
            }

            // Convert all arguments to Int64Array
            let mut arg_arrays = Vec::new();
            for arg in args {
                let arr = arg.as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                        "Argument must be Int64".to_string()
                    ))?;
                arg_arrays.push(arr);
            }

            let num_rows = arg_arrays[0].len();

            // Process row by row
            let mut results = Vec::with_capacity(num_rows);

            for row_idx in 0..num_rows {
                // Check for NULLs in any argument
                let has_null = arg_arrays.iter()
                    .any(|arr| !arr.is_valid(row_idx));

                if has_null {
                    results.push(None);
                    continue;
                }

                // Collect values for this row
                let values: Vec<i64> = arg_arrays.iter()
                    .map(|arr| arr.value(row_idx))
                    .collect();

                // Call WASM function - need to create new instance each time
                let modules_guard = modules.read().unwrap();
                let wasm_module = modules_guard.get(&module_name)
                    .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                        format!("Module '{}' not found", module_name)
                    ))?;

                // Create a new store and instance for this call
                let mut store = Store::default();
                let import_object = Imports::new();
                let instance = Instance::new(&mut store, &wasm_module.module, &import_object)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(
                        format!("Failed to create WASM instance: {}", e)
                    ))?;

                let func = instance.exports.get_function(&wasm_func_name)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(
                        format!("Function '{}' not found: {}", wasm_func_name, e)
                    ))?;

                // Call with dynamic arity
                let wasm_values: Vec<wasmer::Value> = values.iter()
                    .map(|v| wasmer::Value::I64(*v))
                    .collect();

                let result = func.call(&mut store, &wasm_values)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(
                        format!("WASM function call failed: {}", e)
                    ))?;

                let output = result[0].unwrap_i64();
                results.push(Some(output));
            }

            Ok(Arc::new(Int64Array::from(results)))
        };

        // Create DataFusion signature with variable arity
        let arg_types = vec![DataType::Int64; arity];

        let udf = create_udf(
            udf_name,
            arg_types,
            Arc::new(DataType::Int64),
            Volatility::Immutable,
            make_scalar_function(func),
        );

        ctx.register_udf(udf);
        tracing::info!("Registered {}-argument i64 WASM UDF: {}", arity, udf_name);

        Ok(())
    }

    /// Register multi-argument f64 function
    fn register_f64_function_multi(
        &self,
        ctx: &datafusion::execution::context::SessionContext,
        module_name: &str,
        wasm_func_name: &str,
        udf_name: &str,
        arity: usize,
    ) -> DataFusionResult<()> {
        let modules = self.modules.clone();
        let module_name = module_name.to_string();
        let wasm_func_name = wasm_func_name.to_string();

        let func = move |args: &[ArrayRef]| -> DataFusionResult<ArrayRef> {
            if args.len() != arity {
                return Err(datafusion::error::DataFusionError::Execution(
                    format!("Expected {} arguments, got {}", arity, args.len())
                ));
            }

            let mut arg_arrays = Vec::new();
            for arg in args {
                let arr = arg.as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                        "Argument must be Float64".to_string()
                    ))?;
                arg_arrays.push(arr);
            }

            let num_rows = arg_arrays[0].len();
            let mut results = Vec::with_capacity(num_rows);

            for row_idx in 0..num_rows {
                let has_null = arg_arrays.iter()
                    .any(|arr| !arr.is_valid(row_idx));

                if has_null {
                    results.push(None);
                    continue;
                }

                let values: Vec<f64> = arg_arrays.iter()
                    .map(|arr| arr.value(row_idx))
                    .collect();

                // Call WASM function - need to create new instance each time
                let modules_guard = modules.read().unwrap();
                let wasm_module = modules_guard.get(&module_name)
                    .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                        format!("Module '{}' not found", module_name)
                    ))?;

                // Create a new store and instance for this call
                let mut store = Store::default();
                let import_object = Imports::new();
                let instance = Instance::new(&mut store, &wasm_module.module, &import_object)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(
                        format!("Failed to create WASM instance: {}", e)
                    ))?;

                let func = instance.exports.get_function(&wasm_func_name)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(
                        format!("Function '{}' not found: {}", wasm_func_name, e)
                    ))?;

                let wasm_values: Vec<wasmer::Value> = values.iter()
                    .map(|v| wasmer::Value::F64(*v))
                    .collect();

                let result = func.call(&mut store, &wasm_values)
                    .map_err(|e| datafusion::error::DataFusionError::Execution(
                        format!("WASM function call failed: {}", e)
                    ))?;

                let output = result[0].unwrap_f64();
                results.push(Some(output));
            }

            Ok(Arc::new(Float64Array::from(results)))
        };

        let arg_types = vec![DataType::Float64; arity];

        let udf = create_udf(
            udf_name,
            arg_types,
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            make_scalar_function(func),
        );

        ctx.register_udf(udf);
        tracing::info!("Registered {}-argument f64 WASM UDF: {}", arity, udf_name);

        Ok(())
    }

}

/// Global WASM UDF manager instance
lazy_static::lazy_static! {
    static ref WASM_MANAGER: Arc<RwLock<Option<WasmUdfManager>>> = Arc::new(RwLock::new(None));
}

/// Initialize the global WASM UDF manager
pub fn init_wasm_manager() {
    let mut manager = WASM_MANAGER.write().unwrap();
    if manager.is_none() {
        *manager = Some(WasmUdfManager::new());
        tracing::info!("WASM UDF Manager initialized");
    }
}

/// Get a reference to the global WASM UDF manager
pub fn get_wasm_manager() -> Arc<RwLock<Option<WasmUdfManager>>> {
    WASM_MANAGER.clone()
}

/// Register built-in WASM functions (currently empty)
pub fn register_wasm_functions(ctx: &datafusion::execution::context::SessionContext) {
    // Initialize the WASM manager if not already done
    init_wasm_manager();

    // Register any pre-loaded WASM UDFs
    // In the future, this will load UDFs from a configuration file or database

    tracing::debug!("WASM function registration complete");
}

/// Example: Load and register a WASM UDF from a file
pub fn load_and_register_wasm_udf(
    ctx: &datafusion::execution::context::SessionContext,
    config: WasmUdfConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let manager_lock = get_wasm_manager();
    let mut manager_opt = manager_lock.write().unwrap();

    let manager = manager_opt
        .as_mut()
        .ok_or("WASM manager not initialized")?;

    // Load module if not already loaded
    if !manager.has_module(&config.wasm_path) {
        manager.load_module_from_file(&config.wasm_path, &config.wasm_path)?;
    }

    // Register the UDF
    manager.register_udf(ctx, config)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[test]
    fn test_wasm_manager_creation() {
        let manager = WasmUdfManager::new();
        assert!(!manager.has_module("test"));
    }

    #[test]
    fn test_wasm_udf_config() {
        let config = WasmUdfConfig {
            name: "my_function".to_string(),
            wasm_path: "./udfs/my_function.wasm".to_string(),
            function_name: "process".to_string(),
            return_type: WasmDataType::Float64,
            arg_types: vec![WasmDataType::Float64],
        };

        assert_eq!(config.name, "my_function");
        assert_eq!(config.function_name, "process");
    }

    #[test]
    fn test_detect_function_arity() {
        let mut manager = WasmUdfManager::new();

        // Load test module with multi-arg functions
        let wasm_bytes = include_bytes!("../../test_data/add_two_i64.wasm");
        manager.load_module("test", wasm_bytes).unwrap();

        // Detect arity of 'add' function (should be 2)
        let modules = manager.modules.read().unwrap();
        let module = modules.get("test").unwrap();

        let arity = module.get_function_arity("add").unwrap();
        assert_eq!(arity, 2);
    }

    #[test]
    fn test_detect_return_type() {
        let mut manager = WasmUdfManager::new();
        let wasm_bytes = include_bytes!("../../test_data/add_two_i64.wasm");
        manager.load_module("test", wasm_bytes).unwrap();

        let modules = manager.modules.read().unwrap();
        let module = modules.get("test").unwrap();

        let sig = module.get_function_signature("add").unwrap();
        assert_eq!(sig.params.len(), 2);
        assert_eq!(sig.params[0], wasmer::Type::I64);
        assert_eq!(sig.params[1], wasmer::Type::I64);
        assert_eq!(sig.results[0], wasmer::Type::I64);
    }

    #[tokio::test]
    async fn test_register_multi_arg_i64() {
        let mut manager = WasmUdfManager::new();

        let wasm_bytes = include_bytes!("../../test_data/add_two_i64.wasm");
        manager.load_module("math", wasm_bytes).unwrap();

        let ctx = SessionContext::new();

        // This should auto-detect 2 arguments and register appropriately
        manager.register_function(&ctx, "math", "add", "add_numbers")
            .expect("Failed to register");

        // Verify the UDF is registered
        let df = ctx.sql("SELECT add_numbers(5, 10) AS result").await.unwrap();
        let batches = df.collect().await.unwrap();

        let result = batches[0].column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();

        assert_eq!(result.value(0), 15);
    }
}

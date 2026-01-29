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

        // Compile the WASM module
        let module = Module::new(&self.store, wasm_bytes)?;

        // Get exported functions
        let exports = module
            .exports()
            .functions()
            .map(|f| f.name().to_string())
            .collect();

        let wasm_module = WasmModule { module, exports };

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

    /// Register a WASM function as a DataFusion UDF
    pub fn register_udf(
        &self,
        ctx: &datafusion::execution::context::SessionContext,
        config: WasmUdfConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Registering WASM UDF: {}", config.name);

        // Create the UDF based on return type
        match config.return_type {
            WasmDataType::Int64 => {
                self.register_i64_udf(ctx, config)?;
            }
            WasmDataType::Float64 => {
                self.register_f64_udf(ctx, config)?;
            }
            WasmDataType::String => {
                return Err("String return type not yet implemented for WASM UDFs".into());
            }
        }

        Ok(())
    }

    /// Register an i64-returning WASM UDF
    fn register_i64_udf(
        &self,
        ctx: &datafusion::execution::context::SessionContext,
        config: WasmUdfConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let modules = self.modules.clone();
        let wasm_path = config.wasm_path.clone();
        let function_name = config.function_name.clone();

        let func = make_scalar_function(move |args: &[ArrayRef]| {
            // For now, assume single i64 argument
            if args.len() != 1 {
                return Err(datafusion::error::DataFusionError::Execution(
                    format!("Expected 1 argument, got {}", args.len()),
                ));
            }

            let arg_array = args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Expected Int64Array".to_string(),
                    )
                })?;

            // Execute WASM function for each row
            let results: Vec<Option<i64>> = (0..arg_array.len())
                .map(|i| {
                    if !arg_array.is_valid(i) {
                        None
                    } else {
                        let val = arg_array.value(i);
                        // TODO: Execute WASM function
                        // For now, return the input (identity function)
                        Some(val)
                    }
                })
                .collect();

            let result_array = Int64Array::from(results);
            Ok(Arc::new(result_array) as ArrayRef)
        });

        let udf = create_udf(
            &config.name,
            vec![DataType::Int64],
            Arc::new(DataType::Int64),
            Volatility::Immutable,
            func,
        );

        ctx.register_udf(udf);

        tracing::info!("Registered WASM UDF: {}", config.name);

        Ok(())
    }

    /// Register an f64-returning WASM UDF
    fn register_f64_udf(
        &self,
        ctx: &datafusion::execution::context::SessionContext,
        config: WasmUdfConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let modules = self.modules.clone();
        let wasm_path = config.wasm_path.clone();
        let function_name = config.function_name.clone();

        let func = make_scalar_function(move |args: &[ArrayRef]| {
            if args.len() != 1 {
                return Err(datafusion::error::DataFusionError::Execution(
                    format!("Expected 1 argument, got {}", args.len()),
                ));
            }

            let arg_array = args[0]
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "Expected Float64Array".to_string(),
                    )
                })?;

            let results: Vec<Option<f64>> = (0..arg_array.len())
                .map(|i| {
                    if !arg_array.is_valid(i) {
                        None
                    } else {
                        let val = arg_array.value(i);
                        // TODO: Execute WASM function
                        // For now, return square of input
                        Some(val * val)
                    }
                })
                .collect();

            let result_array = Float64Array::from(results);
            Ok(Arc::new(result_array) as ArrayRef)
        });

        let udf = create_udf(
            &config.name,
            vec![DataType::Float64],
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            func,
        );

        ctx.register_udf(udf);

        tracing::info!("Registered WASM UDF: {}", config.name);

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
}

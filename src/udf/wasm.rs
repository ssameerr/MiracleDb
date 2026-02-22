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

/// Enhanced parameter type classification
#[derive(Clone, Debug, PartialEq)]
pub enum WasmParamType {
    I64,
    F64,
    String,
}

/// Enhanced function signature with high-level types
#[derive(Clone, Debug)]
pub struct EnhancedFunctionSignature {
    pub params: Vec<WasmParamType>,
    pub return_type: WasmParamType,
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

    /// Detect high-level parameter types from WASM signature
    ///
    /// Strings are represented as consecutive (i32, i32) pairs (pointer, length).
    /// This method identifies these patterns and classifies parameters as I64, F64, or String.
    pub fn detect_param_types(&self, name: &str) -> DataFusionResult<EnhancedFunctionSignature> {
        let sig = self.get_function_signature(name)?;

        let mut params = Vec::new();
        let mut i = 0;

        while i < sig.params.len() {
            match sig.params[i] {
                wasmer::Type::I64 => {
                    params.push(WasmParamType::I64);
                    i += 1;
                }
                wasmer::Type::F64 => {
                    params.push(WasmParamType::F64);
                    i += 1;
                }
                wasmer::Type::I32 => {
                    // Check if this is a string parameter (i32, i32) pair
                    if i + 1 < sig.params.len() && matches!(sig.params[i + 1], wasmer::Type::I32) {
                        params.push(WasmParamType::String);
                        i += 2; // Skip both i32 values
                    } else {
                        return Err(datafusion::error::DataFusionError::Execution(
                            format!("Unsupported parameter type: lone i32 at position {}", i)
                        ));
                    }
                }
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        format!("Unsupported parameter type: {:?}", sig.params[i])
                    ));
                }
            }
        }

        // Detect return type
        let return_type = if sig.results.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "Function must have a return value".to_string()
            ));
        } else if sig.results.len() == 1 {
            match sig.results[0] {
                wasmer::Type::I64 => WasmParamType::I64,
                wasmer::Type::F64 => WasmParamType::F64,
                _ => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        format!("Unsupported return type: {:?}", sig.results[0])
                    ));
                }
            }
        } else if sig.results.len() == 2
            && matches!(sig.results[0], wasmer::Type::I32)
            && matches!(sig.results[1], wasmer::Type::I32) {
            WasmParamType::String
        } else {
            return Err(datafusion::error::DataFusionError::Execution(
                format!("Unsupported return signature: {:?}", sig.results)
            ));
        };

        Ok(EnhancedFunctionSignature {
            params,
            return_type,
        })
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

    /// Get function signature from a module
    pub fn get_function_signature(
        &self,
        module_name: &str,
        func_name: &str,
    ) -> DataFusionResult<FunctionSignature> {
        let modules = self.modules.read().unwrap();
        let module = modules.get(module_name).ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(
                format!("Module '{}' not found", module_name)
            )
        })?;
        module.get_function_signature(func_name)
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

    /// Allocate memory in WASM linear memory for a string
    fn allocate_wasm_string(
        store: &mut Store,
        instance: &Instance,
        data: &str,
    ) -> Result<(i32, i32), Box<dyn std::error::Error>> {
        // Get memory export
        let memory = instance.exports.get_memory("memory")?;

        // Try to find allocator function (try both common names)
        let alloc_fn = instance.exports.get_function("allocate")
            .or_else(|_| instance.exports.get_function("__wasm_allocate"))?;

        // Allocate memory
        let bytes = data.as_bytes();
        let len = bytes.len() as i32;

        let result = alloc_fn.call(store, &[Value::I32(len)])?;
        let ptr = result[0].unwrap_i32();

        // Write string bytes to memory
        let memory_view = memory.view(store);
        memory_view.write(ptr as u64, bytes)?;

        Ok((ptr, len))
    }

    /// Read a string from WASM linear memory
    fn read_wasm_string(
        store: &Store,
        instance: &Instance,
        ptr: i32,
        len: i32,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let memory = instance.exports.get_memory("memory")?;
        let memory_view = memory.view(store);

        let mut buffer = vec![0u8; len as usize];
        memory_view.read(ptr as u64, &mut buffer)?;

        // Validate UTF-8
        String::from_utf8(buffer)
            .map_err(|e| format!("Invalid UTF-8 from WASM: {}", e).into())
    }

    /// Deallocate WASM memory
    fn deallocate_wasm_string(
        store: &mut Store,
        instance: &Instance,
        ptr: i32,
        len: i32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Try to find deallocator function (try both common names)
        if let Ok(dealloc_fn) = instance.exports.get_function("deallocate")
            .or_else(|_| instance.exports.get_function("__wasm_deallocate"))
        {
            dealloc_fn.call(store, &[Value::I32(ptr), Value::I32(len)])?;
        }
        // If no deallocator exists, memory will be freed when instance is dropped
        Ok(())
    }

    /// Execute a WASM function with string inputs and string output
    pub fn call_string(
        &mut self,
        module_name: &str,
        function_name: &str,
        args: &[&str],
    ) -> Result<String, Box<dyn std::error::Error>> {
        let modules = self.modules.read().unwrap();
        let wasm_module = modules
            .get(module_name)
            .ok_or_else(|| format!("WASM module '{}' not found", module_name))?;

        let import_object = Imports::new();
        let instance = Instance::new(&mut self.store, &wasm_module.module, &import_object)?;

        // Allocate all input strings
        let mut allocated_ptrs = Vec::new();
        let mut wasm_args = Vec::new();

        for arg_str in args {
            let (ptr, len) = Self::allocate_wasm_string(&mut self.store, &instance, arg_str)?;
            allocated_ptrs.push((ptr, len));
            wasm_args.push(Value::I32(ptr));
            wasm_args.push(Value::I32(len));
        }

        // Call function
        let func = instance.exports.get_function(function_name)?;
        let result = func.call(&mut self.store, &wasm_args)?;

        // Extract result pointer and length
        let result_ptr = result[0].unwrap_i32();
        let result_len = result[1].unwrap_i32();

        // Read result string
        let result_string = Self::read_wasm_string(&self.store, &instance, result_ptr, result_len)?;

        // Deallocate input strings
        for (ptr, len) in allocated_ptrs {
            let _ = Self::deallocate_wasm_string(&mut self.store, &instance, ptr, len);
        }

        // Deallocate result string
        let _ = Self::deallocate_wasm_string(&mut self.store, &instance, result_ptr, result_len);

        Ok(result_string)
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

        // NEW: Try enhanced signature detection first (supports strings and mixed types)
        if let Ok(enhanced_sig) = module.detect_param_types(wasm_func_name) {
            // Check if parameters are homogeneous (all same type)
            let is_homogeneous = enhanced_sig.params.windows(2).all(|w| w[0] == w[1]);
            let return_matches_params = !enhanced_sig.params.is_empty()
                && enhanced_sig.params[0] == enhanced_sig.return_type;

            // If homogeneous and return type matches, use specialized registration
            if is_homogeneous && return_matches_params && !enhanced_sig.params.is_empty() {
                let arity = enhanced_sig.params.len();
                match enhanced_sig.params[0] {
                    WasmParamType::I64 => {
                        drop(modules); // Release lock before recursive call
                        return self.register_i64_function_multi(
                            ctx,
                            module_name,
                            wasm_func_name,
                            udf_name,
                            arity,
                        );
                    }
                    WasmParamType::F64 => {
                        drop(modules); // Release lock before recursive call
                        return self.register_f64_function_multi(
                            ctx,
                            module_name,
                            wasm_func_name,
                            udf_name,
                            arity,
                        );
                    }
                    WasmParamType::String => {
                        drop(modules); // Release lock before recursive call
                        return self.register_string_function_multi(
                            ctx,
                            module_name,
                            wasm_func_name,
                            udf_name,
                            arity,
                        );
                    }
                }
            } else {
                // Heterogeneous parameters or different return type - use mixed-type registration
                drop(modules); // Release lock before recursive call
                return self.register_mixed_function(
                    ctx,
                    module_name,
                    wasm_func_name,
                    udf_name,
                    enhanced_sig,
                );
            }
        }

        // FALLBACK: Legacy signature detection (backward compatibility)
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
                drop(modules); // Release lock
                self.register_i64_function_multi(
                    ctx,
                    module_name,
                    wasm_func_name,
                    udf_name,
                    sig.params.len(),
                )
            }
            (wasmer::Type::F64, wasmer::Type::F64) => {
                drop(modules); // Release lock
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

    /// Register multi-argument string function (homogeneous string parameters)
    fn register_string_function_multi(
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

            // Convert all arguments to StringArray
            let mut arg_arrays = Vec::new();
            for arg in args {
                let arr = arg.as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                        "Argument must be String/VARCHAR".to_string()
                    ))?;
                arg_arrays.push(arr);
            }

            let num_rows = arg_arrays[0].len();

            // Process row by row
            let mut results: Vec<Option<String>> = Vec::with_capacity(num_rows);

            for row_idx in 0..num_rows {
                // Check for NULLs in any argument
                let has_null = arg_arrays.iter()
                    .any(|arr| !arr.is_valid(row_idx));

                if has_null {
                    results.push(None);
                    continue;
                }

                // Collect string values for this row
                let values: Vec<&str> = arg_arrays.iter()
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

                // Allocate and call string function
                let result_string = match Self::call_string_internal(
                    &mut store,
                    &instance,
                    &wasm_func_name,
                    &values,
                ) {
                    Ok(s) => s,
                    Err(e) => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            format!("WASM string call failed: {}", e)
                        ));
                    }
                };

                results.push(Some(result_string));
            }

            Ok(Arc::new(StringArray::from(results)))
        };

        // Create DataFusion signature with variable arity
        let arg_types = vec![DataType::Utf8; arity];

        let udf = create_udf(
            udf_name,
            arg_types,
            Arc::new(DataType::Utf8),
            Volatility::Immutable,
            make_scalar_function(func),
        );

        ctx.register_udf(udf);
        tracing::info!("Registered {}-argument string WASM UDF: {}", arity, udf_name);

        Ok(())
    }

    /// Internal helper for calling string WASM functions
    fn call_string_internal(
        store: &mut Store,
        instance: &Instance,
        function_name: &str,
        args: &[&str],
    ) -> Result<String, Box<dyn std::error::Error>> {
        let func = instance.exports.get_function(function_name)?;

        let mut allocated_ptrs = Vec::new();
        let mut wasm_args = Vec::new();

        for arg_str in args {
            let (ptr, len) = Self::allocate_wasm_string(store, instance, arg_str)?;
            allocated_ptrs.push((ptr, len));
            wasm_args.push(Value::I32(ptr));
            wasm_args.push(Value::I32(len));
        }

        let result = func.call(store, &wasm_args)?;

        let result_ptr = result[0].unwrap_i32();
        let result_len = result[1].unwrap_i32();

        let result_string = Self::read_wasm_string(store, instance, result_ptr, result_len)?;

        // Cleanup
        for (ptr, len) in allocated_ptrs {
            let _ = Self::deallocate_wasm_string(store, instance, ptr, len);
        }
        let _ = Self::deallocate_wasm_string(store, instance, result_ptr, result_len);

        Ok(result_string)
    }

    /// Register a mixed-type function (heterogeneous parameter types)
    ///
    /// Supports functions like fn(String, i64, i64) -> String
    fn register_mixed_function(
        &self,
        ctx: &datafusion::execution::context::SessionContext,
        module_name: &str,
        wasm_func_name: &str,
        udf_name: &str,
        sig: EnhancedFunctionSignature,
    ) -> DataFusionResult<()> {
        let module_name = module_name.to_string();
        let wasm_func_name = wasm_func_name.to_string();
        let module_name_for_log = module_name.clone();
        let wasm_func_name_for_log = wasm_func_name.clone();
        let modules = self.modules.clone();

        // Determine DataFusion return type
        let return_type = match sig.return_type {
            WasmParamType::I64 => DataType::Int64,
            WasmParamType::F64 => DataType::Float64,
            WasmParamType::String => DataType::Utf8,
        };

        // Determine DataFusion input types
        let input_types: Vec<DataType> = sig.params.iter().map(|pt| match pt {
            WasmParamType::I64 => DataType::Int64,
            WasmParamType::F64 => DataType::Float64,
            WasmParamType::String => DataType::Utf8,
        }).collect();

        let num_params = sig.params.len();

        let func = make_scalar_function(move |args: &[ArrayRef]| {
            if args.len() != num_params {
                return Err(datafusion::error::DataFusionError::Execution(
                    format!("Expected {} arguments, got {}", num_params, args.len())
                ));
            }

            let num_rows = args[0].len();
            let modules_guard = modules.read().unwrap();
            let wasm_module = modules_guard.get(&module_name).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    format!("Module '{}' not found", module_name)
                )
            })?;

            // Create fresh store and instance for this execution
            let mut store = Store::default();
            let import_object = Imports::new();
            let instance = Instance::new(&mut store, &wasm_module.module, &import_object)
                .map_err(|e| datafusion::error::DataFusionError::Execution(
                    format!("Failed to create instance: {}", e)
                ))?;

            // Process based on return type
            match sig.return_type {
                WasmParamType::String => {
                    let mut results: Vec<Option<String>> = Vec::with_capacity(num_rows);

                    for row_idx in 0..num_rows {
                        // Check for NULL in any input
                        let mut has_null = false;
                        for (arg_idx, arg) in args.iter().enumerate() {
                            if !arg.is_valid(row_idx) {
                                has_null = true;
                                break;
                            }
                        }

                        if has_null {
                            results.push(None);
                            continue;
                        }

                        // Extract arguments based on their types
                        let mut wasm_args = Vec::new();
                        let mut allocated_ptrs: Vec<(i32, i32)> = Vec::new();

                        for (arg_idx, param_type) in sig.params.iter().enumerate() {
                            match param_type {
                                WasmParamType::I64 => {
                                    let arr = args[arg_idx].as_any().downcast_ref::<Int64Array>()
                                        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                                            format!("Expected Int64Array at position {}", arg_idx)
                                        ))?;
                                    let val = arr.value(row_idx);
                                    wasm_args.push(Value::I64(val));
                                }
                                WasmParamType::F64 => {
                                    let arr = args[arg_idx].as_any().downcast_ref::<Float64Array>()
                                        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                                            format!("Expected Float64Array at position {}", arg_idx)
                                        ))?;
                                    let val = arr.value(row_idx);
                                    wasm_args.push(Value::F64(val));
                                }
                                WasmParamType::String => {
                                    let arr = args[arg_idx].as_any().downcast_ref::<StringArray>()
                                        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                                            format!("Expected StringArray at position {}", arg_idx)
                                        ))?;
                                    let s = arr.value(row_idx);

                                    // Allocate string in WASM memory
                                    let (ptr, len) = Self::allocate_wasm_string(&mut store, &instance, s)
                                        .map_err(|e| datafusion::error::DataFusionError::Execution(
                                            format!("Failed to allocate string: {}", e)
                                        ))?;
                                    allocated_ptrs.push((ptr, len));
                                    wasm_args.push(Value::I32(ptr));
                                    wasm_args.push(Value::I32(len));
                                }
                            }
                        }

                        // Call WASM function
                        let func = instance.exports.get_function(&wasm_func_name)
                            .map_err(|e| datafusion::error::DataFusionError::Execution(
                                format!("Function not found: {}", e)
                            ))?;

                        let result = func.call(&mut store, &wasm_args)
                            .map_err(|e| datafusion::error::DataFusionError::Execution(
                                format!("WASM execution failed: {}", e)
                            ))?;

                        // Extract result (should be (i32, i32) for string)
                        let result_ptr = result[0].i32().ok_or_else(|| {
                            datafusion::error::DataFusionError::Execution(
                                "Expected i32 result pointer".to_string()
                            )
                        })?;
                        let result_len = result[1].i32().ok_or_else(|| {
                            datafusion::error::DataFusionError::Execution(
                                "Expected i32 result length".to_string()
                            )
                        })?;

                        let result_string = Self::read_wasm_string(&store, &instance, result_ptr, result_len)
                            .map_err(|e| datafusion::error::DataFusionError::Execution(
                                format!("Failed to read result string: {}", e)
                            ))?;

                        // Cleanup
                        for (ptr, len) in &allocated_ptrs {
                            let _ = Self::deallocate_wasm_string(&mut store, &instance, *ptr, *len);
                        }
                        let _ = Self::deallocate_wasm_string(&mut store, &instance, result_ptr, result_len);

                        results.push(Some(result_string));
                    }

                    Ok(Arc::new(StringArray::from(results)) as ArrayRef)
                }
                WasmParamType::I64 => {
                    let mut results: Vec<Option<i64>> = Vec::with_capacity(num_rows);

                    for row_idx in 0..num_rows {
                        // Similar logic for i64 return with mixed inputs
                        let mut has_null = false;
                        for arg in args.iter() {
                            if !arg.is_valid(row_idx) {
                                has_null = true;
                                break;
                            }
                        }

                        if has_null {
                            results.push(None);
                            continue;
                        }

                        let mut wasm_args = Vec::new();
                        let mut allocated_ptrs: Vec<(i32, i32)> = Vec::new();

                        for (arg_idx, param_type) in sig.params.iter().enumerate() {
                            match param_type {
                                WasmParamType::I64 => {
                                    let arr = args[arg_idx].as_any().downcast_ref::<Int64Array>()
                                        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                                            format!("Expected Int64Array at position {}", arg_idx)
                                        ))?;
                                    wasm_args.push(Value::I64(arr.value(row_idx)));
                                }
                                WasmParamType::F64 => {
                                    let arr = args[arg_idx].as_any().downcast_ref::<Float64Array>()
                                        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                                            format!("Expected Float64Array at position {}", arg_idx)
                                        ))?;
                                    wasm_args.push(Value::F64(arr.value(row_idx)));
                                }
                                WasmParamType::String => {
                                    let arr = args[arg_idx].as_any().downcast_ref::<StringArray>()
                                        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                                            format!("Expected StringArray at position {}", arg_idx)
                                        ))?;
                                    let s = arr.value(row_idx);
                                    let (ptr, len) = Self::allocate_wasm_string(&mut store, &instance, s)
                                        .map_err(|e| datafusion::error::DataFusionError::Execution(
                                            format!("Failed to allocate string: {}", e)
                                        ))?;
                                    allocated_ptrs.push((ptr, len));
                                    wasm_args.push(Value::I32(ptr));
                                    wasm_args.push(Value::I32(len));
                                }
                            }
                        }

                        let func = instance.exports.get_function(&wasm_func_name)
                            .map_err(|e| datafusion::error::DataFusionError::Execution(
                                format!("Function not found: {}", e)
                            ))?;

                        let result = func.call(&mut store, &wasm_args)
                            .map_err(|e| datafusion::error::DataFusionError::Execution(
                                format!("WASM execution failed: {}", e)
                            ))?;

                        let result_val = result[0].i64().ok_or_else(|| {
                            datafusion::error::DataFusionError::Execution(
                                "Expected i64 result".to_string()
                            )
                        })?;

                        for (ptr, len) in &allocated_ptrs {
                            let _ = Self::deallocate_wasm_string(&mut store, &instance, *ptr, *len);
                        }

                        results.push(Some(result_val));
                    }

                    Ok(Arc::new(Int64Array::from(results)) as ArrayRef)
                }
                WasmParamType::F64 => {
                    // Similar for f64
                    let mut results: Vec<Option<f64>> = Vec::with_capacity(num_rows);

                    for row_idx in 0..num_rows {
                        let mut has_null = false;
                        for arg in args.iter() {
                            if !arg.is_valid(row_idx) {
                                has_null = true;
                                break;
                            }
                        }

                        if has_null {
                            results.push(None);
                            continue;
                        }

                        let mut wasm_args = Vec::new();
                        let mut allocated_ptrs: Vec<(i32, i32)> = Vec::new();

                        for (arg_idx, param_type) in sig.params.iter().enumerate() {
                            match param_type {
                                WasmParamType::I64 => {
                                    let arr = args[arg_idx].as_any().downcast_ref::<Int64Array>()
                                        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                                            format!("Expected Int64Array at position {}", arg_idx)
                                        ))?;
                                    wasm_args.push(Value::I64(arr.value(row_idx)));
                                }
                                WasmParamType::F64 => {
                                    let arr = args[arg_idx].as_any().downcast_ref::<Float64Array>()
                                        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                                            format!("Expected Float64Array at position {}", arg_idx)
                                        ))?;
                                    wasm_args.push(Value::F64(arr.value(row_idx)));
                                }
                                WasmParamType::String => {
                                    let arr = args[arg_idx].as_any().downcast_ref::<StringArray>()
                                        .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                                            format!("Expected StringArray at position {}", arg_idx)
                                        ))?;
                                    let s = arr.value(row_idx);
                                    let (ptr, len) = Self::allocate_wasm_string(&mut store, &instance, s)
                                        .map_err(|e| datafusion::error::DataFusionError::Execution(
                                            format!("Failed to allocate string: {}", e)
                                        ))?;
                                    allocated_ptrs.push((ptr, len));
                                    wasm_args.push(Value::I32(ptr));
                                    wasm_args.push(Value::I32(len));
                                }
                            }
                        }

                        let func = instance.exports.get_function(&wasm_func_name)
                            .map_err(|e| datafusion::error::DataFusionError::Execution(
                                format!("Function not found: {}", e)
                            ))?;

                        let result = func.call(&mut store, &wasm_args)
                            .map_err(|e| datafusion::error::DataFusionError::Execution(
                                format!("WASM execution failed: {}", e)
                            ))?;

                        let result_val = result[0].f64().ok_or_else(|| {
                            datafusion::error::DataFusionError::Execution(
                                "Expected f64 result".to_string()
                            )
                        })?;

                        for (ptr, len) in &allocated_ptrs {
                            let _ = Self::deallocate_wasm_string(&mut store, &instance, *ptr, *len);
                        }

                        results.push(Some(result_val));
                    }

                    Ok(Arc::new(Float64Array::from(results)) as ArrayRef)
                }
            }
        });

        let udf = create_udf(
            udf_name,
            input_types,
            Arc::new(return_type),
            Volatility::Immutable,
            func,
        );

        ctx.register_udf(udf);

        tracing::info!(
            "Registered mixed-type WASM UDF '{}' from module '{}' (function '{}')",
            udf_name,
            module_name_for_log,
            wasm_func_name_for_log
        );

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

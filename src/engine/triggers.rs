//! Trigger System - Event-driven WASM execution
//!
//! Allows defining WASM functions that execute on data changes

use std::collections::HashMap;
use std::sync::Arc;
use datafusion::error::{DataFusionError, Result};
use tracing::{info, warn, error};
use wasmer::{Store, Module, Instance, imports, Function, Value, TypedFunction};
use tokio::sync::RwLock;

/// Events that can trigger WASM execution
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TriggerEvent {
    /// Before a row is inserted
    BeforeInsert,
    /// After a row is inserted
    AfterInsert,
    /// Before a row is updated
    BeforeUpdate,
    /// After a row is updated
    AfterUpdate,
    /// Before a row is deleted
    BeforeDelete,
    /// After a row is deleted
    AfterDelete,
}

/// A registered trigger
#[derive(Clone)]
pub struct Trigger {
    /// Unique trigger ID
    pub id: String,
    /// Table this trigger is attached to
    pub table: String,
    /// Event that fires this trigger
    pub event: TriggerEvent,
    /// WASM module bytes
    pub wasm_module: Vec<u8>,
    /// Whether the trigger is enabled
    pub enabled: bool,
    /// Created timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Context passed to trigger execution
#[derive(Clone, Debug)]
pub struct TriggerContext {
    /// Table name
    pub table: String,
    /// Event type
    pub event: TriggerEvent,
    /// Old row data (for update/delete)
    pub old_row: Option<serde_json::Value>,
    /// New row data (for insert/update)
    pub new_row: Option<serde_json::Value>,
    /// Transaction ID if within a transaction
    pub transaction_id: Option<u64>,
}

/// Result of trigger execution
#[derive(Clone, Debug)]
pub enum TriggerResult {
    /// Continue with the operation
    Continue,
    /// Modify the row before continuing
    Modify(serde_json::Value),
    /// Abort the operation
    Abort(String),
}

/// Cached compiled WASM module
struct CompiledModule {
    module: Module,
}

/// Registry of all triggers with WASM runtime
pub struct TriggerRegistry {
    /// Triggers by table then by event
    triggers: HashMap<String, HashMap<TriggerEvent, Vec<Trigger>>>,
    /// WASM runtime store
    store: Arc<RwLock<Store>>,
    /// Compiled module cache
    module_cache: HashMap<String, CompiledModule>,
}

impl TriggerRegistry {
    pub fn new() -> Self {
        Self {
            triggers: HashMap::new(),
            store: Arc::new(RwLock::new(Store::default())),
            module_cache: HashMap::new(),
        }
    }

    /// Register a new trigger
    pub fn register(
        &mut self,
        table: &str,
        event: TriggerEvent,
        wasm_module: Vec<u8>,
    ) -> Result<String> {
        let trigger_id = format!("{}_{:?}_{}", table, event, chrono::Utc::now().timestamp());
        
        let trigger = Trigger {
            id: trigger_id.clone(),
            table: table.to_string(),
            event,
            wasm_module,
            enabled: true,
            created_at: chrono::Utc::now(),
        };
        
        self.triggers
            .entry(table.to_string())
            .or_insert_with(HashMap::new)
            .entry(event)
            .or_insert_with(Vec::new)
            .push(trigger);
        
        info!("Registered trigger {} on {} for {:?}", trigger_id, table, event);
        Ok(trigger_id)
    }

    /// Unregister a trigger by ID
    pub fn unregister(&mut self, trigger_id: &str) -> Result<()> {
        for table_triggers in self.triggers.values_mut() {
            for event_triggers in table_triggers.values_mut() {
                event_triggers.retain(|t| t.id != trigger_id);
            }
        }
        info!("Unregistered trigger {}", trigger_id);
        Ok(())
    }

    /// Enable or disable a trigger
    pub fn set_enabled(&mut self, trigger_id: &str, enabled: bool) -> Result<()> {
        for table_triggers in self.triggers.values_mut() {
            for event_triggers in table_triggers.values_mut() {
                for trigger in event_triggers.iter_mut() {
                    if trigger.id == trigger_id {
                        trigger.enabled = enabled;
                        info!("Trigger {} enabled={}", trigger_id, enabled);
                        return Ok(());
                    }
                }
            }
        }
        Err(DataFusionError::Execution(format!("Trigger {} not found", trigger_id)))
    }

    /// Fire triggers for an event
    pub async fn fire(
        &self,
        table: &str,
        event: TriggerEvent,
        context: TriggerContext,
    ) -> Result<TriggerResult> {
        let triggers = self.triggers
            .get(table)
            .and_then(|t| t.get(&event))
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        
        let mut current_row = context.new_row.clone();
        
        for trigger in triggers.iter().filter(|t| t.enabled) {
            match self.execute_trigger(trigger, &context).await {
                Ok(TriggerResult::Continue) => continue,
                Ok(TriggerResult::Modify(new_row)) => {
                    current_row = Some(new_row);
                }
                Ok(TriggerResult::Abort(reason)) => {
                    warn!("Trigger {} aborted operation: {}", trigger.id, reason);
                    return Ok(TriggerResult::Abort(reason));
                }
                Err(e) => {
                    error!("Trigger {} failed: {}", trigger.id, e);
                    // Continue with other triggers on error
                }
            }
        }
        
        if let Some(modified_row) = current_row {
            if context.new_row.as_ref() != Some(&modified_row) {
                return Ok(TriggerResult::Modify(modified_row));
            }
        }
        
        Ok(TriggerResult::Continue)
    }

    /// Execute a single trigger - tries real WASM first, then bytecode fallback
    async fn execute_trigger(
        &self,
        trigger: &Trigger,
        context: &TriggerContext,
    ) -> Result<TriggerResult> {
        info!(
            "Executing trigger {} for {:?} on {}",
            trigger.id, trigger.event, trigger.table
        );
        
        if trigger.wasm_module.is_empty() {
            return Ok(TriggerResult::Continue);
        }
        
        // Check for WASM magic bytes (0x00 0x61 0x73 0x6D = \0asm)
        if trigger.wasm_module.len() > 8 && &trigger.wasm_module[..4] == b"\0asm" {
            // Real WASM module - execute with wasmer
            return self.execute_wasm_module(trigger, context).await;
        }
        
        // Fallback: Interpret as simple bytecode
        self.execute_bytecode(trigger, context).await
    }
    
    /// Execute a real WASM module using wasmer
    async fn execute_wasm_module(
        &self,
        trigger: &Trigger,
        context: &TriggerContext,
    ) -> Result<TriggerResult> {
        let mut store = self.store.write().await;
        
        // Compile the module
        let module = match Module::new(&store, &trigger.wasm_module) {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to compile WASM module for trigger {}: {}", trigger.id, e);
                return Err(DataFusionError::Execution(format!("WASM compile error: {}", e)));
            }
        };
        
        // Create import object (no imports for now - sandboxed execution)
        let import_object = imports! {};
        
        // Instantiate the module
        let instance = match Instance::new(&mut store, &module, &import_object) {
            Ok(i) => i,
            Err(e) => {
                error!("Failed to instantiate WASM module: {}", e);
                return Err(DataFusionError::Execution(format!("WASM instantiation error: {}", e)));
            }
        };
        
        // Try to call the "trigger" or "main" export
        let func_name = if instance.exports.get_function("trigger").is_ok() {
            "trigger"
        } else if instance.exports.get_function("main").is_ok() {
            "main"
        } else if instance.exports.get_function("_start").is_ok() {
            "_start"
        } else {
            warn!("No trigger/main/_start function found in WASM module");
            return Ok(TriggerResult::Continue);
        };
        
        // Get the function
        let func = instance.exports.get_function(func_name)
            .map_err(|e| DataFusionError::Execution(format!("Function lookup error: {}", e)))?;
        
        // Call the function with no arguments, expect i32 result
        // 0 = Continue, 1 = Abort, 2+ = Modify
        match func.call(&mut store, &[]) {
            Ok(results) => {
                if let Some(Value::I32(result_code)) = results.first() {
                    match *result_code {
                        0 => {
                            info!("WASM trigger returned: Continue");
                            Ok(TriggerResult::Continue)
                        }
                        1 => {
                            info!("WASM trigger returned: Abort");
                            Ok(TriggerResult::Abort("Trigger aborted operation".to_string()))
                        }
                        _ => {
                            info!("WASM trigger returned: Continue (code={})", result_code);
                            Ok(TriggerResult::Continue)
                        }
                    }
                } else {
                    // No return value - continue
                    Ok(TriggerResult::Continue)
                }
            }
            Err(e) => {
                error!("WASM execution error: {}", e);
                Err(DataFusionError::Execution(format!("WASM runtime error: {}", e)))
            }
        }
    }
    
    /// Execute bytecode interpreter (fallback for simple opcodes)
    async fn execute_bytecode(
        &self,
        trigger: &Trigger,
        context: &TriggerContext,
    ) -> Result<TriggerResult> {
        // Bytecode format:
        //   0x00 = NOP (continue)
        //   0x01 = ABORT (abort with message)
        //   0x02 = MODIFY (modify row)
        //   0x10 = VALIDATE (check required field)
        //   0x20 = LOG (log and continue)
        
        let opcode = trigger.wasm_module[0];
        
        match opcode {
            0x00 => {
                // NOP - continue without changes
                Ok(TriggerResult::Continue)
            }
            0x01 => {
                // ABORT - read message from remaining bytes
                let message = if trigger.wasm_module.len() > 1 {
                    String::from_utf8_lossy(&trigger.wasm_module[1..]).to_string()
                } else {
                    "Trigger aborted operation".to_string()
                };
                Ok(TriggerResult::Abort(message))
            }
            0x02 => {
                // MODIFY - apply transformation
                if let Some(ref new_row) = context.new_row {
                    let mut modified = new_row.clone();
                    
                    // Apply simple transformations based on additional bytes
                    if trigger.wasm_module.len() > 1 {
                        let transform = trigger.wasm_module[1];
                        match transform {
                            0x01 => {
                                // Add timestamp
                                if let Some(obj) = modified.as_object_mut() {
                                    obj.insert(
                                        "modified_at".to_string(),
                                        serde_json::json!(chrono::Utc::now().to_rfc3339())
                                    );
                                }
                            }
                            0x02 => {
                                // Add trigger info
                                if let Some(obj) = modified.as_object_mut() {
                                    obj.insert(
                                        "_trigger".to_string(),
                                        serde_json::json!(trigger.id)
                                    );
                                }
                            }
                            0x03 => {
                                // Uppercase string fields
                                if let Some(obj) = modified.as_object_mut() {
                                    for (_, v) in obj.iter_mut() {
                                        if let Some(s) = v.as_str() {
                                            *v = serde_json::json!(s.to_uppercase());
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    
                    Ok(TriggerResult::Modify(modified))
                } else {
                    Ok(TriggerResult::Continue)
                }
            }
            0x10 => {
                // VALIDATE - check constraints
                if let Some(ref new_row) = context.new_row {
                    let field_bytes = &trigger.wasm_module[1..];
                    if !field_bytes.is_empty() {
                        let field_name = String::from_utf8_lossy(field_bytes);
                        if let Some(obj) = new_row.as_object() {
                            if !obj.contains_key(field_name.as_ref()) {
                                return Ok(TriggerResult::Abort(
                                    format!("Required field '{}' is missing", field_name)
                                ));
                            }
                        }
                    }
                }
                Ok(TriggerResult::Continue)
            }
            0x20 => {
                // LOG - just log and continue
                info!(
                    "Trigger LOG: table={}, event={:?}, new_row={:?}",
                    context.table, context.event, context.new_row
                );
                Ok(TriggerResult::Continue)
            }
            _ => {
                // Unknown opcode - continue
                warn!("Unknown trigger opcode: 0x{:02X}", opcode);
                Ok(TriggerResult::Continue)
            }
        }
    }

    /// List all triggers for a table
    pub fn list_triggers(&self, table: &str) -> Vec<&Trigger> {
        self.triggers
            .get(table)
            .map(|t| t.values().flatten().collect())
            .unwrap_or_default()
    }

    /// Get trigger by ID
    pub fn get_trigger(&self, trigger_id: &str) -> Option<&Trigger> {
        for table_triggers in self.triggers.values() {
            for event_triggers in table_triggers.values() {
                for trigger in event_triggers {
                    if trigger.id == trigger_id {
                        return Some(trigger);
                    }
                }
            }
        }
        None
    }
}

impl Default for TriggerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_trigger_registration() {
        let mut registry = TriggerRegistry::new();
        
        let id = registry.register("users", TriggerEvent::AfterInsert, vec![0, 1, 2]).unwrap();
        assert!(!id.is_empty());
        
        let triggers = registry.list_triggers("users");
        assert_eq!(triggers.len(), 1);
    }

    #[tokio::test]
    async fn test_trigger_fire() {
        let registry = TriggerRegistry::new();
        
        let context = TriggerContext {
            table: "users".to_string(),
            event: TriggerEvent::AfterInsert,
            old_row: None,
            new_row: Some(serde_json::json!({"id": 1, "name": "test"})),
            transaction_id: None,
        };
        
        let result = registry.fire("users", TriggerEvent::AfterInsert, context).await.unwrap();
        assert!(matches!(result, TriggerResult::Continue));
    }
}

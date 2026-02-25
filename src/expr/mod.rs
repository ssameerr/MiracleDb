//! Expression Module - Expression parsing and evaluation

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Expression AST
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Expr {
    // Literals
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    
    // References
    Column(String),
    Qualified(String, String), // table.column
    
    // Binary operations
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Mod(Box<Expr>, Box<Expr>),
    
    // Comparisons
    Eq(Box<Expr>, Box<Expr>),
    Ne(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Le(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Ge(Box<Expr>, Box<Expr>),
    
    // Logical
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    
    // Patterns
    Like(Box<Expr>, String),
    In(Box<Expr>, Vec<Expr>),
    Between(Box<Expr>, Box<Expr>, Box<Expr>),
    IsNull(Box<Expr>),
    
    // Functions
    Function(String, Vec<Expr>),
    Case { when_then: Vec<(Expr, Expr)>, else_expr: Option<Box<Expr>> },
    Cast(Box<Expr>, String),
}

/// Expression value
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<Value>),
}

impl Value {
    pub fn from_json(v: &serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else {
                    Value::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => Value::String(s.clone()),
            serde_json::Value::Array(arr) => {
                Value::Array(arr.iter().map(Value::from_json).collect())
            }
            _ => Value::Null,
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::json!(*b),
            Value::Int(i) => serde_json::json!(*i),
            Value::Float(f) => serde_json::json!(*f),
            Value::String(s) => serde_json::json!(s),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| v.to_json()).collect())
            }
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Int(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::Array(arr) => !arr.is_empty(),
        }
    }
}

/// Expression evaluator
pub struct Evaluator;

impl Evaluator {
    /// Evaluate expression against a row
    pub fn eval(expr: &Expr, row: &serde_json::Value) -> Value {
        match expr {
            Expr::Null => Value::Null,
            Expr::Bool(b) => Value::Bool(*b),
            Expr::Int(i) => Value::Int(*i),
            Expr::Float(f) => Value::Float(*f),
            Expr::String(s) => Value::String(s.clone()),
            
            Expr::Column(name) => {
                row.get(name)
                    .map(Value::from_json)
                    .unwrap_or(Value::Null)
            }
            
            Expr::Qualified(_, col) => {
                row.get(col)
                    .map(Value::from_json)
                    .unwrap_or(Value::Null)
            }
            
            Expr::Add(left, right) => Self::binary_op(row, left, right, |a, b| match (a, b) {
                (Value::Int(a), Value::Int(b)) => Value::Int(a + b),
                (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
                (Value::Int(a), Value::Float(b)) => Value::Float(a as f64 + b),
                (Value::Float(a), Value::Int(b)) => Value::Float(a + b as f64),
                (Value::String(a), Value::String(b)) => Value::String(format!("{}{}", a, b)),
                _ => Value::Null,
            }),
            
            Expr::Sub(left, right) => Self::binary_op(row, left, right, |a, b| match (a, b) {
                (Value::Int(a), Value::Int(b)) => Value::Int(a - b),
                (Value::Float(a), Value::Float(b)) => Value::Float(a - b),
                _ => Value::Null,
            }),
            
            Expr::Mul(left, right) => Self::binary_op(row, left, right, |a, b| match (a, b) {
                (Value::Int(a), Value::Int(b)) => Value::Int(a * b),
                (Value::Float(a), Value::Float(b)) => Value::Float(a * b),
                _ => Value::Null,
            }),
            
            Expr::Div(left, right) => Self::binary_op(row, left, right, |a, b| match (a, b) {
                (Value::Int(a), Value::Int(b)) if b != 0 => Value::Int(a / b),
                (Value::Float(a), Value::Float(b)) if b != 0.0 => Value::Float(a / b),
                _ => Value::Null,
            }),
            
            Expr::Eq(left, right) => {
                let l = Self::eval(left, row);
                let r = Self::eval(right, row);
                Value::Bool(Self::values_equal(&l, &r))
            }
            
            Expr::Ne(left, right) => {
                let l = Self::eval(left, row);
                let r = Self::eval(right, row);
                Value::Bool(!Self::values_equal(&l, &r))
            }
            
            Expr::Lt(left, right) => Self::compare_op(row, left, right, |ord| ord == std::cmp::Ordering::Less),
            Expr::Le(left, right) => Self::compare_op(row, left, right, |ord| ord != std::cmp::Ordering::Greater),
            Expr::Gt(left, right) => Self::compare_op(row, left, right, |ord| ord == std::cmp::Ordering::Greater),
            Expr::Ge(left, right) => Self::compare_op(row, left, right, |ord| ord != std::cmp::Ordering::Less),
            
            Expr::And(left, right) => {
                let l = Self::eval(left, row);
                let r = Self::eval(right, row);
                Value::Bool(l.is_truthy() && r.is_truthy())
            }
            
            Expr::Or(left, right) => {
                let l = Self::eval(left, row);
                let r = Self::eval(right, row);
                Value::Bool(l.is_truthy() || r.is_truthy())
            }
            
            Expr::Not(inner) => {
                let v = Self::eval(inner, row);
                Value::Bool(!v.is_truthy())
            }
            
            Expr::IsNull(inner) => {
                let v = Self::eval(inner, row);
                Value::Bool(matches!(v, Value::Null))
            }
            
            Expr::Like(expr, pattern) => {
                if let Value::String(s) = Self::eval(expr, row) {
                    let regex_pattern = pattern
                        .replace('%', ".*")
                        .replace('_', ".");
                    if let Ok(re) = regex::Regex::new(&format!("^{}$", regex_pattern)) {
                        return Value::Bool(re.is_match(&s));
                    }
                }
                Value::Bool(false)
            }
            
            Expr::In(expr, list) => {
                let val = Self::eval(expr, row);
                let found = list.iter().any(|item| {
                    let item_val = Self::eval(item, row);
                    Self::values_equal(&val, &item_val)
                });
                Value::Bool(found)
            }
            
            Expr::Function(name, args) => {
                let evaluated_args: Vec<Value> = args.iter()
                    .map(|a| Self::eval(a, row))
                    .collect();
                Self::call_function(name, &evaluated_args)
            }
            
            Expr::Case { when_then, else_expr } => {
                for (when, then) in when_then {
                    if Self::eval(when, row).is_truthy() {
                        return Self::eval(then, row);
                    }
                }
                else_expr.as_ref()
                    .map(|e| Self::eval(e, row))
                    .unwrap_or(Value::Null)
            }
            
            _ => Value::Null,
        }
    }

    fn binary_op<F>(row: &serde_json::Value, left: &Expr, right: &Expr, op: F) -> Value
    where
        F: Fn(Value, Value) -> Value,
    {
        let l = Self::eval(left, row);
        let r = Self::eval(right, row);
        op(l, r)
    }

    fn compare_op<F>(row: &serde_json::Value, left: &Expr, right: &Expr, pred: F) -> Value
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        let l = Self::eval(left, row);
        let r = Self::eval(right, row);
        if let Some(ord) = Self::compare_values(&l, &r) {
            Value::Bool(pred(ord))
        } else {
            Value::Null
        }
    }

    fn values_equal(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
            (Value::String(a), Value::String(b)) => a == b,
            _ => false,
        }
    }

    fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
        match (a, b) {
            (Value::Int(a), Value::Int(b)) => Some(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }

    fn call_function(name: &str, args: &[Value]) -> Value {
        match name.to_uppercase().as_str() {
            "UPPER" => {
                if let Some(Value::String(s)) = args.first() {
                    Value::String(s.to_uppercase())
                } else {
                    Value::Null
                }
            }
            "LOWER" => {
                if let Some(Value::String(s)) = args.first() {
                    Value::String(s.to_lowercase())
                } else {
                    Value::Null
                }
            }
            "LENGTH" | "LEN" => {
                if let Some(Value::String(s)) = args.first() {
                    Value::Int(s.len() as i64)
                } else {
                    Value::Null
                }
            }
            "ABS" => {
                match args.first() {
                    Some(Value::Int(i)) => Value::Int(i.abs()),
                    Some(Value::Float(f)) => Value::Float(f.abs()),
                    _ => Value::Null,
                }
            }
            "COALESCE" => {
                args.iter()
                    .find(|v| !matches!(v, Value::Null))
                    .cloned()
                    .unwrap_or(Value::Null)
            }
            "NOW" => {
                Value::Int(chrono::Utc::now().timestamp())
            }
            _ => Value::Null,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arithmetic() {
        let row = serde_json::json!({"a": 10, "b": 3});
        let expr = Expr::Add(
            Box::new(Expr::Column("a".to_string())),
            Box::new(Expr::Column("b".to_string())),
        );
        let result = Evaluator::eval(&expr, &row);
        assert!(matches!(result, Value::Int(13)));
    }

    #[test]
    fn test_comparison() {
        let row = serde_json::json!({"age": 25});
        let expr = Expr::Gt(
            Box::new(Expr::Column("age".to_string())),
            Box::new(Expr::Int(18)),
        );
        let result = Evaluator::eval(&expr, &row);
        assert!(matches!(result, Value::Bool(true)));
    }
}

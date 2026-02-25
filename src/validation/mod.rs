//! Validation Module - Data validation utilities

use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use regex::Regex;

/// Validation result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationResult {
    pub valid: bool,
    pub errors: Vec<ValidationError>,
}

impl ValidationResult {
    pub fn ok() -> Self {
        Self { valid: true, errors: vec![] }
    }

    pub fn fail(errors: Vec<ValidationError>) -> Self {
        Self { valid: false, errors }
    }

    pub fn extend(&mut self, other: ValidationResult) {
        if !other.valid {
            self.valid = false;
            self.errors.extend(other.errors);
        }
    }
}

/// Validation error
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationError {
    pub field: String,
    pub code: String,
    pub message: String,
}

/// Validation rule
#[derive(Clone, Debug)]
pub enum ValidationRule {
    Required,
    MinLength(usize),
    MaxLength(usize),
    Min(f64),
    Max(f64),
    Pattern(String),
    Email,
    Uuid,
    Url,
    In(Vec<String>),
    NotIn(Vec<String>),
    Custom(String),
}

/// Field validator
pub struct FieldValidator {
    field: String,
    rules: Vec<ValidationRule>,
}

impl FieldValidator {
    pub fn new(field: &str) -> Self {
        Self {
            field: field.to_string(),
            rules: vec![],
        }
    }

    pub fn required(mut self) -> Self {
        self.rules.push(ValidationRule::Required);
        self
    }

    pub fn min_length(mut self, len: usize) -> Self {
        self.rules.push(ValidationRule::MinLength(len));
        self
    }

    pub fn max_length(mut self, len: usize) -> Self {
        self.rules.push(ValidationRule::MaxLength(len));
        self
    }

    pub fn min(mut self, val: f64) -> Self {
        self.rules.push(ValidationRule::Min(val));
        self
    }

    pub fn max(mut self, val: f64) -> Self {
        self.rules.push(ValidationRule::Max(val));
        self
    }

    pub fn pattern(mut self, pattern: &str) -> Self {
        self.rules.push(ValidationRule::Pattern(pattern.to_string()));
        self
    }

    pub fn email(mut self) -> Self {
        self.rules.push(ValidationRule::Email);
        self
    }

    pub fn uuid(mut self) -> Self {
        self.rules.push(ValidationRule::Uuid);
        self
    }

    pub fn url(mut self) -> Self {
        self.rules.push(ValidationRule::Url);
        self
    }

    pub fn one_of(mut self, values: Vec<&str>) -> Self {
        self.rules.push(ValidationRule::In(values.into_iter().map(|s| s.to_string()).collect()));
        self
    }

    pub fn validate(&self, value: &serde_json::Value) -> ValidationResult {
        let mut errors = Vec::new();

        for rule in &self.rules {
            if let Some(error) = self.check_rule(rule, value) {
                errors.push(error);
            }
        }

        if errors.is_empty() {
            ValidationResult::ok()
        } else {
            ValidationResult::fail(errors)
        }
    }

    fn check_rule(&self, rule: &ValidationRule, value: &serde_json::Value) -> Option<ValidationError> {
        match rule {
            ValidationRule::Required => {
                if value.is_null() {
                    Some(self.error("required", "Field is required"))
                } else {
                    None
                }
            }
            ValidationRule::MinLength(len) => {
                if let Some(s) = value.as_str() {
                    if s.len() < *len {
                        Some(self.error("min_length", &format!("Minimum length is {}", len)))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ValidationRule::MaxLength(len) => {
                if let Some(s) = value.as_str() {
                    if s.len() > *len {
                        Some(self.error("max_length", &format!("Maximum length is {}", len)))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ValidationRule::Min(min) => {
                if let Some(n) = value.as_f64() {
                    if n < *min {
                        Some(self.error("min", &format!("Minimum value is {}", min)))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ValidationRule::Max(max) => {
                if let Some(n) = value.as_f64() {
                    if n > *max {
                        Some(self.error("max", &format!("Maximum value is {}", max)))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ValidationRule::Pattern(pattern) => {
                if let Some(s) = value.as_str() {
                    if let Ok(re) = Regex::new(pattern) {
                        if !re.is_match(s) {
                            Some(self.error("pattern", "Value does not match pattern"))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ValidationRule::Email => {
                if let Some(s) = value.as_str() {
                    let email_re = Regex::new(r"^[^\s@]+@[^\s@]+\.[^\s@]+$").unwrap();
                    if !email_re.is_match(s) {
                        Some(self.error("email", "Invalid email format"))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ValidationRule::Uuid => {
                if let Some(s) = value.as_str() {
                    if uuid::Uuid::parse_str(s).is_err() {
                        Some(self.error("uuid", "Invalid UUID format"))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ValidationRule::Url => {
                if let Some(s) = value.as_str() {
                    if !s.starts_with("http://") && !s.starts_with("https://") {
                        Some(self.error("url", "Invalid URL format"))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ValidationRule::In(values) => {
                if let Some(s) = value.as_str() {
                    if !values.contains(&s.to_string()) {
                        Some(self.error("in", &format!("Value must be one of: {:?}", values)))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ValidationRule::NotIn(values) => {
                if let Some(s) = value.as_str() {
                    if values.contains(&s.to_string()) {
                        Some(self.error("not_in", &format!("Value must not be one of: {:?}", values)))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
            ValidationRule::Custom(_) => None,
        }
    }

    fn error(&self, code: &str, message: &str) -> ValidationError {
        ValidationError {
            field: self.field.clone(),
            code: code.to_string(),
            message: message.to_string(),
        }
    }
}

/// Object validator
pub struct ObjectValidator {
    validators: Vec<FieldValidator>,
}

impl ObjectValidator {
    pub fn new() -> Self {
        Self { validators: vec![] }
    }

    pub fn field(mut self, validator: FieldValidator) -> Self {
        self.validators.push(validator);
        self
    }

    pub fn validate(&self, obj: &serde_json::Value) -> ValidationResult {
        let mut result = ValidationResult::ok();

        for validator in &self.validators {
            let value = obj.get(&validator.field).unwrap_or(&serde_json::Value::Null);
            result.extend(validator.validate(value));
        }

        result
    }
}

impl Default for ObjectValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_required() {
        let validator = FieldValidator::new("name").required();
        
        let valid = validator.validate(&serde_json::json!("test"));
        assert!(valid.valid);
        
        let invalid = validator.validate(&serde_json::Value::Null);
        assert!(!invalid.valid);
    }

    #[test]
    fn test_email() {
        let validator = FieldValidator::new("email").email();
        
        let valid = validator.validate(&serde_json::json!("test@example.com"));
        assert!(valid.valid);
        
        let invalid = validator.validate(&serde_json::json!("not-an-email"));
        assert!(!invalid.valid);
    }
}

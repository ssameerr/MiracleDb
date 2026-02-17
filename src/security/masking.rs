//! Data Masking - Dynamic redaction based on policies

use std::collections::HashMap;
use sha2::{Sha256, Digest};

/// Simple masking rules for standalone PII redaction via [`apply_mask`].
///
/// Use this enum together with [`apply_mask`] when you need a lightweight,
/// policy-free way to redact a single value.  For table/column-scoped policies
/// with role exemptions use [`ColumnMaskingRule`] and [`DataMasker`] instead.
#[derive(Clone, Debug)]
pub enum MaskingRule {
    /// Mask an email address: `user@example.com` → `u***@example.com`
    Email,
    /// Mask a phone number, keeping first digit and last 3: `+1234567890` → `1******890`
    Phone,
    /// Mask a credit-card number, keeping last 4 digits: `1234567890123456` → `************3456`
    CreditCard,
    /// Replace the entire value with a custom string.
    Custom(String),
}

/// Apply a [`MaskingRule`] to a string value and return the masked result.
pub fn apply_mask(value: &str, rule: &MaskingRule) -> String {
    match rule {
        MaskingRule::Email => {
            if let Some(at) = value.find('@') {
                format!("{}***{}", &value[..1], &value[at..])
            } else {
                "***".to_string()
            }
        }
        MaskingRule::Phone => {
            let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
            if digits.len() > 4 {
                format!("{}******{}", &digits[..1], &digits[digits.len() - 3..])
            } else {
                "****".to_string()
            }
        }
        MaskingRule::CreditCard => {
            let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
            if digits.len() >= 4 {
                format!("************{}", &digits[digits.len() - 4..])
            } else {
                "****".to_string()
            }
        }
        MaskingRule::Custom(replacement) => replacement.clone(),
    }
}

/// Masking strategy used by [`DataMasker`] for full column-policy masking.
#[derive(Clone, Debug)]
pub enum MaskingStrategy {
    Full,
    ShowFirst(usize),
    ShowLast(usize),
    Email,
    Phone,
    Ssn,
    CreditCard,
    Fixed(String),
    Hash,
    Null,
}

/// A column-scoped masking policy with role exemptions.
///
/// Previously named `MaskingRule`; renamed `ColumnMaskingRule` to keep the
/// unqualified `MaskingRule` name for the simpler [`MaskingRule`] enum used
/// with [`apply_mask`].
#[derive(Clone, Debug)]
pub struct ColumnMaskingRule {
    pub table: String,
    pub column: String,
    pub strategy: MaskingStrategy,
    pub exempt_roles: Vec<String>,
    pub priority: i32,
}

/// Data masking engine
pub struct DataMasker {
    rules: HashMap<String, Vec<ColumnMaskingRule>>,
}

impl DataMasker {
    pub fn new() -> Self {
        Self { rules: HashMap::new() }
    }

    pub fn add_rule(&mut self, rule: ColumnMaskingRule) {
        let key = format!("{}.{}", rule.table, rule.column);
        self.rules.entry(key).or_default().push(rule);
    }

    pub fn mask(&self, table: &str, column: &str, value: &str, user_roles: &[String]) -> String {
        let key = format!("{}.{}", table, column);

        if let Some(rules) = self.rules.get(&key) {
            for rule in rules {
                if rule.exempt_roles.iter().any(|r| user_roles.contains(r)) {
                    continue;
                }
                return self.apply_strategy(&rule.strategy, value);
            }
        }
        value.to_string()
    }

    fn apply_strategy(&self, strategy: &MaskingStrategy, value: &str) -> String {
        match strategy {
            MaskingStrategy::Full => "*".repeat(value.len().min(10)),
            MaskingStrategy::ShowFirst(n) => {
                if value.len() <= *n { "*".repeat(value.len()) }
                else { format!("{}{}", &value[..*n], "*".repeat(value.len() - n)) }
            }
            MaskingStrategy::ShowLast(n) => {
                if value.len() <= *n { "*".repeat(value.len()) }
                else { format!("{}{}", "*".repeat(value.len() - n), &value[value.len() - n..]) }
            }
            MaskingStrategy::Email => {
                if let Some(at) = value.find('@') {
                    let (local, domain) = value.split_at(at);
                    if local.len() > 1 { format!("{}***{}", &local[..1], domain) }
                    else { format!("***{}", domain) }
                } else { "*".repeat(value.len().min(10)) }
            }
            MaskingStrategy::Phone => {
                let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
                if digits.len() >= 4 { format!("***-***-{}", &digits[digits.len()-4..]) }
                else { "***-***-****".to_string() }
            }
            MaskingStrategy::Ssn => {
                let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
                if digits.len() >= 4 { format!("***-**-{}", &digits[digits.len()-4..]) }
                else { "***-**-****".to_string() }
            }
            MaskingStrategy::CreditCard => {
                let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
                if digits.len() >= 4 { format!("****-****-****-{}", &digits[digits.len()-4..]) }
                else { "****-****-****-****".to_string() }
            }
            MaskingStrategy::Fixed(s) => s.clone(),
            MaskingStrategy::Hash => {
                let mut h = Sha256::new();
                h.update(value.as_bytes());
                format!("{:x}", h.finalize())[..16].to_string()
            }
            MaskingStrategy::Null => "NULL".to_string(),
        }
    }
}

impl Default for DataMasker {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- apply_mask / MaskingRule enum tests ---

    #[test]
    fn test_apply_mask_email() {
        assert_eq!(apply_mask("user@example.com", &MaskingRule::Email), "u***@example.com");
        assert_eq!(apply_mask("a@b.com", &MaskingRule::Email), "a***@b.com");
        assert_eq!(apply_mask("noemail", &MaskingRule::Email), "***");
    }

    #[test]
    fn test_apply_mask_phone() {
        // +1234567890 → first digit + ****** + last 3 digits
        assert_eq!(apply_mask("+1234567890", &MaskingRule::Phone), "1******890");
        // Short phone (≤4 digits)
        assert_eq!(apply_mask("123", &MaskingRule::Phone), "****");
    }

    #[test]
    fn test_apply_mask_credit_card() {
        assert_eq!(
            apply_mask("1234567890123456", &MaskingRule::CreditCard),
            "************3456"
        );
        assert_eq!(apply_mask("12", &MaskingRule::CreditCard), "****");
    }

    #[test]
    fn test_apply_mask_custom() {
        assert_eq!(
            apply_mask("anything", &MaskingRule::Custom("[REDACTED]".to_string())),
            "[REDACTED]"
        );
    }

    // --- DataMasker / ColumnMaskingRule tests ---

    #[test]
    fn test_data_masker_email_strategy() {
        let mut masker = DataMasker::new();
        masker.add_rule(ColumnMaskingRule {
            table: "users".to_string(),
            column: "email".to_string(),
            strategy: MaskingStrategy::Email,
            exempt_roles: vec![],
            priority: 0,
        });
        let result = masker.mask("users", "email", "bob@example.com", &[]);
        assert_eq!(result, "b***@example.com");
    }

    #[test]
    fn test_data_masker_credit_card_strategy() {
        let mut masker = DataMasker::new();
        masker.add_rule(ColumnMaskingRule {
            table: "payments".to_string(),
            column: "card_number".to_string(),
            strategy: MaskingStrategy::CreditCard,
            exempt_roles: vec![],
            priority: 0,
        });
        let result = masker.mask("payments", "card_number", "4111111111111111", &[]);
        assert_eq!(result, "****-****-****-1111");
    }

    #[test]
    fn test_data_masker_exempt_role_skips_masking() {
        let mut masker = DataMasker::new();
        masker.add_rule(ColumnMaskingRule {
            table: "users".to_string(),
            column: "ssn".to_string(),
            strategy: MaskingStrategy::Ssn,
            exempt_roles: vec!["admin".to_string()],
            priority: 0,
        });
        // Admin sees raw value (rule is skipped because role is exempt)
        let raw = masker.mask("users", "ssn", "123-45-6789", &["admin".to_string()]);
        assert_eq!(raw, "123-45-6789");
        // Regular user gets masked value
        let masked = masker.mask("users", "ssn", "123-45-6789", &[]);
        assert_eq!(masked, "***-**-6789");
    }
}

#[cfg(test)]
mod tests {
    use miracledb::nlp::TextToSql;
    use miracledb::quality::{QualityEngine, ValidationRule, RuleType};
    use miracledb::governance::{PIIDetector, DataClassification};
    use miracledb::financial::{Ledger, Transaction, Entry};
    use miracledb::healthcare::FhirStore;

    #[test]
    fn test_nlp_translation() {
        let sql = TextToSql::translate("Show me users");
        assert_eq!(sql, "SELECT * FROM users");
    }

    #[tokio::test]
    async fn test_quality_rules() {
        let engine = QualityEngine::new();
        engine.add_rule(ValidationRule {
            name: "email_check".to_string(),
            column: "email".to_string(),
            rule_type: RuleType::Regex { pattern: r"@".to_string() },
            enabled: true,
        }).await;

        let data = vec!["test@example.com".to_string(), "invalid-email".to_string()];
        let results = engine.validate("email", &data).await;
        
        assert_eq!(results.len(), 1);
        assert!(!results[0].passed); // Should fail due to invalid-email
    }

    #[test]
    fn test_governance_pii() {
        let detector = PIIDetector::new();
        let text = "Contact me at 555-0199 or test@example.com";
        let findings = detector.detect(text);
        
        // Should find phone and email
        assert!(findings.iter().any(|(n,_)| n == "email"));
        assert!(findings.iter().any(|(n,_)| n == "phone"));
    }

    #[test]
    fn test_financial_ledger() {
        let mut ledger = Ledger::new();
        let tx = Transaction {
            id: "tx1".to_string(),
            date: 100,
            description: "Test".to_string(),
            entries: vec![
                Entry { account: "Cash".to_string(), debit: Some(100.0), credit: None },
                Entry { account: "Sales".to_string(), debit: None, credit: Some(100.0) },
            ],
        };
        
        assert!(ledger.record(tx).is_ok());
        assert_eq!(ledger.balance("Cash"), 100.0);
    }
}

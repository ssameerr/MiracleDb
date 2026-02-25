//! Alert Module - Alert rules and management

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Serialize, Deserialize};

/// Alert severity
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
}

/// Alert state
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum AlertState {
    Pending,
    Firing,
    Resolved,
}

/// Alert rule
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AlertRule {
    pub id: String,
    pub name: String,
    pub description: String,
    pub condition: AlertCondition,
    pub severity: Severity,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub for_duration_seconds: u64,
    pub enabled: bool,
}

/// Alert condition
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AlertCondition {
    MetricThreshold {
        metric: String,
        operator: ThresholdOperator,
        value: f64,
    },
    QueryResult {
        query: String,
        expected_rows: RowCountCondition,
    },
    Absence {
        metric: String,
        for_seconds: u64,
    },
    RateChange {
        metric: String,
        percent: f64,
        direction: RateDirection,
    },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ThresholdOperator {
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Equal,
    NotEqual,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RowCountCondition {
    Zero,
    NonZero,
    Exactly(usize),
    MoreThan(usize),
    LessThan(usize),
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum RateDirection {
    Increase,
    Decrease,
    Either,
}

/// Active alert
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Alert {
    pub id: String,
    pub rule_id: String,
    pub state: AlertState,
    pub severity: Severity,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
    pub value: Option<f64>,
    pub started_at: i64,
    pub resolved_at: Option<i64>,
    pub last_evaluated_at: i64,
}

/// Alert manager
pub struct AlertManager {
    rules: RwLock<HashMap<String, AlertRule>>,
    alerts: RwLock<HashMap<String, Alert>>,
    silences: RwLock<Vec<Silence>>,
    inhibitions: RwLock<Vec<Inhibition>>,
}

/// Silence - suppress alerts
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Silence {
    pub id: String,
    pub matchers: Vec<LabelMatcher>,
    pub starts_at: i64,
    pub ends_at: i64,
    pub created_by: String,
    pub comment: String,
}

/// Label matcher
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LabelMatcher {
    pub name: String,
    pub value: String,
    pub match_type: MatchType,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum MatchType {
    Equal,
    NotEqual,
    Regex,
    NotRegex,
}

/// Inhibition - suppress alerts based on other alerts
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Inhibition {
    pub source_matchers: Vec<LabelMatcher>,
    pub target_matchers: Vec<LabelMatcher>,
    pub equal_labels: Vec<String>,
}

impl AlertManager {
    pub fn new() -> Self {
        Self {
            rules: RwLock::new(HashMap::new()),
            alerts: RwLock::new(HashMap::new()),
            silences: RwLock::new(Vec::new()),
            inhibitions: RwLock::new(Vec::new()),
        }
    }

    /// Add alert rule
    pub async fn add_rule(&self, rule: AlertRule) {
        let mut rules = self.rules.write().await;
        rules.insert(rule.id.clone(), rule);
    }

    /// Remove alert rule
    pub async fn remove_rule(&self, rule_id: &str) {
        let mut rules = self.rules.write().await;
        rules.remove(rule_id);
    }

    /// Fire an alert
    pub async fn fire(&self, rule_id: &str, value: Option<f64>, labels: HashMap<String, String>) {
        let rules = self.rules.read().await;
        let rule = match rules.get(rule_id) {
            Some(r) => r.clone(),
            None => return,
        };
        drop(rules);

        let now = chrono::Utc::now().timestamp();
        let alert_id = uuid::Uuid::new_v4().to_string();

        let alert = Alert {
            id: alert_id.clone(),
            rule_id: rule_id.to_string(),
            state: AlertState::Firing,
            severity: rule.severity,
            labels,
            annotations: rule.annotations.clone(),
            value,
            started_at: now,
            resolved_at: None,
            last_evaluated_at: now,
        };

        // Check if silenced
        if self.is_silenced(&alert).await {
            return;
        }

        let mut alerts = self.alerts.write().await;
        alerts.insert(alert_id, alert);
    }

    /// Resolve an alert
    pub async fn resolve(&self, alert_id: &str) {
        let mut alerts = self.alerts.write().await;
        if let Some(alert) = alerts.get_mut(alert_id) {
            alert.state = AlertState::Resolved;
            alert.resolved_at = Some(chrono::Utc::now().timestamp());
        }
    }

    /// Check if alert is silenced
    async fn is_silenced(&self, alert: &Alert) -> bool {
        let silences = self.silences.read().await;
        let now = chrono::Utc::now().timestamp();

        for silence in silences.iter() {
            if silence.starts_at <= now && silence.ends_at >= now {
                if self.matches_silence(alert, silence) {
                    return true;
                }
            }
        }
        false
    }

    fn matches_silence(&self, alert: &Alert, silence: &Silence) -> bool {
        silence.matchers.iter().all(|matcher| {
            alert.labels.get(&matcher.name)
                .map(|v| match matcher.match_type {
                    MatchType::Equal => v == &matcher.value,
                    MatchType::NotEqual => v != &matcher.value,
                    _ => false,
                })
                .unwrap_or(false)
        })
    }

    /// Add silence
    pub async fn add_silence(&self, silence: Silence) {
        let mut silences = self.silences.write().await;
        silences.push(silence);
    }

    /// Get active alerts
    pub async fn get_active(&self) -> Vec<Alert> {
        let alerts = self.alerts.read().await;
        alerts.values()
            .filter(|a| a.state == AlertState::Firing)
            .cloned()
            .collect()
    }

    /// Get alerts by severity
    pub async fn get_by_severity(&self, severity: Severity) -> Vec<Alert> {
        let alerts = self.alerts.read().await;
        alerts.values()
            .filter(|a| a.severity == severity && a.state == AlertState::Firing)
            .cloned()
            .collect()
    }

    /// Get alert history
    pub async fn get_history(&self, limit: usize) -> Vec<Alert> {
        let alerts = self.alerts.read().await;
        let mut all: Vec<_> = alerts.values().cloned().collect();
        all.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        all.into_iter().take(limit).collect()
    }

    /// Count alerts by severity
    pub async fn count_by_severity(&self) -> HashMap<Severity, usize> {
        let alerts = self.alerts.read().await;
        let mut counts = HashMap::new();

        for alert in alerts.values().filter(|a| a.state == AlertState::Firing) {
            *counts.entry(alert.severity).or_insert(0) += 1;
        }

        counts
    }
}

impl Default for AlertManager {
    fn default() -> Self {
        Self::new()
    }
}

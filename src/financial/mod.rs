use serde::{Serialize, Deserialize};

pub mod decimal;
use decimal::Decimal;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Money {
    pub amount: Decimal,
    pub currency: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transaction {
    pub id: String,
    pub date: i64,
    pub description: String,
    pub entries: Vec<Entry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Entry {
    pub account: String,
    pub debit: Option<Decimal>,
    pub credit: Option<Decimal>,
}

pub struct Ledger {
    transactions: Vec<Transaction>,
}

impl Ledger {
    pub fn new() -> Self {
        Self { transactions: Vec::new() }
    }

    pub fn record(&mut self, tx: Transaction) -> Result<(), String> {
        // Validate double entry (Dr == Cr)
        let total_debit: Decimal = tx.entries.iter().filter_map(|e| e.debit).fold(Decimal::zero(), |a, b| a + b);
        let total_credit: Decimal = tx.entries.iter().filter_map(|e| e.credit).fold(Decimal::zero(), |a, b| a + b);
        
        // Difference should be exaclty zero for Decimal
        if (total_debit - total_credit).abs() > Decimal::from_f64(0.0001) {
             return Err(format!("Unbalanced transaction: Dr {} != Cr {}", total_debit, total_credit));
        }
        
        self.transactions.push(tx);
        Ok(())
    }
    
    pub fn balance(&self, account: &str) -> Decimal {
        let mut balance = Decimal::zero();
        for tx in &self.transactions {
            for entry in &tx.entries {
                if entry.account == account {
                    if let Some(dr) = entry.debit { balance += dr; }
                    if let Some(cr) = entry.credit { balance -= cr; }
                }
            }
        }
        balance
    }
}

pub mod risk;
pub use risk::{PortfolioMetrics, sharpe_ratio, max_drawdown, value_at_risk};

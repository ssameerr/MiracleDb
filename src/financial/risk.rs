//! Financial risk metrics: VaR, Sharpe ratio, drawdown analysis

/// Value at Risk (VaR) calculation using historical simulation
/// Returns VaR at the given confidence level (e.g., 0.95 for 95%)
pub fn value_at_risk(returns: &[f64], confidence: f64) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }
    let mut sorted = returns.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((1.0 - confidence) * sorted.len() as f64) as usize;
    let idx = idx.min(sorted.len() - 1);
    sorted[idx].abs()
}

/// Conditional VaR (Expected Shortfall) - average loss beyond VaR threshold
pub fn conditional_var(returns: &[f64], confidence: f64) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }
    let var = value_at_risk(returns, confidence);
    let tail: Vec<f64> = returns.iter()
        .filter(|&&r| r < -var)
        .copied()
        .collect();
    if tail.is_empty() {
        var
    } else {
        tail.iter().sum::<f64>().abs() / tail.len() as f64
    }
}

/// Sharpe Ratio = (mean_return - risk_free_rate) / std_dev_return
pub fn sharpe_ratio(returns: &[f64], risk_free_rate: f64) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let excess_mean = mean - risk_free_rate;
    let variance = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (returns.len() - 1) as f64;
    let std_dev = variance.sqrt();
    if std_dev == 0.0 { 0.0 } else { excess_mean / std_dev }
}

/// Sortino Ratio - like Sharpe but only penalizes downside volatility
pub fn sortino_ratio(returns: &[f64], risk_free_rate: f64, target_return: f64) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let downside: Vec<f64> = returns.iter()
        .filter(|&&r| r < target_return)
        .map(|&r| (r - target_return).powi(2))
        .collect();

    if downside.is_empty() {
        return f64::INFINITY;
    }

    let downside_dev = (downside.iter().sum::<f64>() / returns.len() as f64).sqrt();
    if downside_dev == 0.0 { 0.0 } else { (mean - risk_free_rate) / downside_dev }
}

/// Maximum Drawdown = max peak-to-trough decline
pub fn max_drawdown(prices: &[f64]) -> f64 {
    if prices.len() < 2 {
        return 0.0;
    }
    let mut max_dd = 0.0_f64;
    let mut peak = prices[0];

    for &price in &prices[1..] {
        if price > peak {
            peak = price;
        }
        let dd = (peak - price) / peak;
        if dd > max_dd {
            max_dd = dd;
        }
    }
    max_dd
}

/// Beta - correlation of asset returns to market returns
pub fn beta(asset_returns: &[f64], market_returns: &[f64]) -> f64 {
    let n = asset_returns.len().min(market_returns.len());
    if n < 2 {
        return 1.0;
    }

    let asset = &asset_returns[..n];
    let market = &market_returns[..n];

    let asset_mean = asset.iter().sum::<f64>() / n as f64;
    let market_mean = market.iter().sum::<f64>() / n as f64;

    let covariance = asset.iter().zip(market.iter())
        .map(|(a, m)| (a - asset_mean) * (m - market_mean))
        .sum::<f64>() / (n - 1) as f64;

    let market_variance = market.iter()
        .map(|m| (m - market_mean).powi(2))
        .sum::<f64>() / (n - 1) as f64;

    if market_variance == 0.0 { 1.0 } else { covariance / market_variance }
}

/// Portfolio metrics summary
#[derive(Debug)]
pub struct PortfolioMetrics {
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub max_drawdown: f64,
    pub var_95: f64,
    pub cvar_95: f64,
    pub beta: f64,
    pub total_return: f64,
    pub annualized_return: f64,
}

impl PortfolioMetrics {
    pub fn compute(returns: &[f64], prices: &[f64], market_returns: &[f64], periods_per_year: f64) -> Self {
        let total_return = if !prices.is_empty() {
            prices.last().unwrap() / prices.first().unwrap() - 1.0
        } else if !returns.is_empty() {
            returns.iter().sum()
        } else {
            0.0
        };

        let n = returns.len() as f64;
        let annualized_return = if n > 0.0 {
            (1.0 + total_return).powf(periods_per_year / n) - 1.0
        } else {
            0.0
        };

        PortfolioMetrics {
            sharpe_ratio: sharpe_ratio(returns, 0.02 / periods_per_year),
            sortino_ratio: sortino_ratio(returns, 0.02 / periods_per_year, 0.0),
            max_drawdown: max_drawdown(prices),
            var_95: value_at_risk(returns, 0.95),
            cvar_95: conditional_var(returns, 0.95),
            beta: beta(returns, market_returns),
            total_return,
            annualized_return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sharpe_ratio() {
        let returns = vec![0.01, 0.02, -0.01, 0.03, 0.01, -0.02, 0.04];
        let sharpe = sharpe_ratio(&returns, 0.0);
        assert!(sharpe > 0.0);
    }

    #[test]
    fn test_max_drawdown() {
        let prices = vec![100.0, 110.0, 90.0, 120.0, 80.0, 130.0];
        let dd = max_drawdown(&prices);
        // Peak 120, trough 80, DD = (120-80)/120 = 0.333...
        assert!((dd - 1.0/3.0).abs() < 0.001);
    }

    #[test]
    fn test_var() {
        let returns = vec![-0.05, -0.02, 0.01, 0.03, -0.01, 0.02, -0.03, 0.04, -0.04, 0.02];
        let var95 = value_at_risk(&returns, 0.95);
        assert!(var95 > 0.0);
    }
}

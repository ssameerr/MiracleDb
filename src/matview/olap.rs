//! OLAP helpers for Materialized Views
//!
//! Provides ROLLUP and CUBE grouping-set generators used in analytical queries,
//! plus supporting types for window functions and OLAP aggregations.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Grouping-set generators
// ---------------------------------------------------------------------------

/// Generates all grouping combinations for ROLLUP.
///
/// `ROLLUP(a, b, c)` produces:
/// `(a, b, c)`, `(a, b)`, `(a)`, `()`
///
/// # Examples
/// ```
/// # use miracledb::matview::olap::rollup_combinations;
/// let cols = vec!["region".to_string(), "country".to_string(), "city".to_string()];
/// let sets = rollup_combinations(&cols);
/// assert_eq!(sets.len(), 4);
/// assert_eq!(sets[0], cols);          // all three
/// assert!(sets.last().unwrap().is_empty()); // grand total
/// ```
pub fn rollup_combinations(columns: &[String]) -> Vec<Vec<String>> {
    (0..=columns.len())
        .rev()
        .map(|n| columns[..n].to_vec())
        .collect()
}

/// Generates all grouping combinations for CUBE.
///
/// `CUBE(a, b)` produces all 2^n subsets:
/// `(a, b)`, `(a)`, `(b)`, `()`
///
/// # Examples
/// ```
/// # use miracledb::matview::olap::cube_combinations;
/// let cols = vec!["region".to_string(), "product".to_string()];
/// let sets = cube_combinations(&cols);
/// assert_eq!(sets.len(), 4); // 2^2
/// ```
pub fn cube_combinations(columns: &[String]) -> Vec<Vec<String>> {
    let n = columns.len();
    (0u32..(1u32 << n))
        .map(|mask| {
            columns
                .iter()
                .enumerate()
                .filter(|(i, _)| mask & (1 << i) != 0)
                .map(|(_, col)| col.clone())
                .collect()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// GROUPING SETS
// ---------------------------------------------------------------------------

/// Arbitrary grouping sets as used in `GROUP BY GROUPING SETS ((a,b),(a),(b),())`.
///
/// Each element of the outer `Vec` is one grouping set (a `Vec` of column names).
/// Both `rollup_combinations` and `cube_combinations` return this shape, so they
/// can be passed directly to code that processes `GroupingSets`.
pub type GroupingSets = Vec<Vec<String>>;

// ---------------------------------------------------------------------------
// Window function frame specification
// ---------------------------------------------------------------------------

/// Boundary for a window frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

/// Unit for a window frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameUnit {
    Rows,
    Range,
    Groups,
}

/// Window frame definition (`ROWS BETWEEN ... AND ...`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFrame {
    pub unit: FrameUnit,
    pub start: FrameBound,
    pub end: FrameBound,
}

impl Default for WindowFrame {
    fn default() -> Self {
        Self {
            unit: FrameUnit::Range,
            start: FrameBound::UnboundedPreceding,
            end: FrameBound::CurrentRow,
        }
    }
}

// ---------------------------------------------------------------------------
// OLAP aggregation function descriptors
// ---------------------------------------------------------------------------

/// Supported OLAP / window aggregation functions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OlapFunction {
    /// Running / cumulative sum
    Sum,
    /// Average over the window frame
    Avg,
    /// Minimum over the window frame
    Min,
    /// Maximum over the window frame
    Max,
    /// Count of non-null rows
    Count,
    /// Row number within the partition (no ties)
    RowNumber,
    /// Dense rank (no gaps)
    DenseRank,
    /// Standard rank (with gaps)
    Rank,
    /// Percent rank in `[0,1]`
    PercentRank,
    /// Cumulative distribution
    CumeDist,
    /// Value from N rows before current row
    Lag,
    /// Value from N rows after current row
    Lead,
    /// First value in the window frame
    FirstValue,
    /// Last value in the window frame
    LastValue,
    /// Nth value in the window frame
    NthValue,
}

/// A complete window / analytic function specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowSpec {
    /// The analytic function to apply.
    pub function: OlapFunction,
    /// Column(s) to pass as arguments to the function.
    pub args: Vec<String>,
    /// Partition-by columns (equivalent to `PARTITION BY`).
    pub partition_by: Vec<String>,
    /// Order-by columns (equivalent to `ORDER BY`).
    pub order_by: Vec<String>,
    /// Optional window frame; `None` means the default frame for the function.
    pub frame: Option<WindowFrame>,
}

// ---------------------------------------------------------------------------
// OLAP query plan node
// ---------------------------------------------------------------------------

/// A single OLAP aggregation step in a query plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OlapAggregation {
    /// Grouping sets to evaluate (may be the result of ROLLUP, CUBE, or an
    /// explicit GROUPING SETS clause).
    pub grouping_sets: GroupingSets,
    /// Window / analytic functions to compute alongside the grouped aggregates.
    pub window_specs: Vec<WindowSpec>,
    /// Raw SQL fragment for debugging / explain output.
    pub source_query: String,
}

impl OlapAggregation {
    /// Create a new OLAP aggregation node with ROLLUP grouping.
    pub fn with_rollup(columns: &[String], source_query: &str) -> Self {
        Self {
            grouping_sets: rollup_combinations(columns),
            window_specs: vec![],
            source_query: source_query.to_string(),
        }
    }

    /// Create a new OLAP aggregation node with CUBE grouping.
    pub fn with_cube(columns: &[String], source_query: &str) -> Self {
        Self {
            grouping_sets: cube_combinations(columns),
            window_specs: vec![],
            source_query: source_query.to_string(),
        }
    }

    /// Return the total number of grouping sets in this node.
    pub fn grouping_set_count(&self) -> usize {
        self.grouping_sets.len()
    }

    /// Add a window function specification to this aggregation node.
    pub fn add_window(mut self, spec: WindowSpec) -> Self {
        self.window_specs.push(spec);
        self
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollup_empty_columns() {
        let result = rollup_combinations(&[]);
        // ROLLUP() → one grouping set: the grand total ()
        assert_eq!(result, vec![vec![] as Vec<String>]);
    }

    #[test]
    fn rollup_single_column() {
        let cols = vec!["region".to_string()];
        let result = rollup_combinations(&cols);
        assert_eq!(result, vec![vec!["region".to_string()], vec![]]);
    }

    #[test]
    fn rollup_three_columns() {
        let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let result = rollup_combinations(&cols);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0], cols);
        assert_eq!(result[1], vec!["a".to_string(), "b".to_string()]);
        assert_eq!(result[2], vec!["a".to_string()]);
        assert!(result[3].is_empty());
    }

    #[test]
    fn cube_empty_columns() {
        let result = cube_combinations(&[]);
        // CUBE() → one grouping set: ()
        assert_eq!(result, vec![vec![] as Vec<String>]);
    }

    #[test]
    fn cube_single_column() {
        let cols = vec!["x".to_string()];
        let result = cube_combinations(&cols);
        // mask=0 → (), mask=1 → (x)
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn cube_two_columns() {
        let cols = vec!["a".to_string(), "b".to_string()];
        let result = cube_combinations(&cols);
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn cube_three_columns_count() {
        let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        // 2^3 = 8 subsets
        assert_eq!(cube_combinations(&cols).len(), 8);
    }

    #[test]
    fn olap_aggregation_rollup_count() {
        let cols = vec!["year".to_string(), "quarter".to_string(), "month".to_string()];
        let agg = OlapAggregation::with_rollup(&cols, "SELECT year, quarter, month, SUM(sales) FROM facts GROUP BY ROLLUP(year, quarter, month)");
        // ROLLUP(3 cols) → 4 sets
        assert_eq!(agg.grouping_set_count(), 4);
    }

    #[test]
    fn olap_aggregation_cube_count() {
        let cols = vec!["region".to_string(), "product".to_string()];
        let agg = OlapAggregation::with_cube(&cols, "SELECT region, product, SUM(revenue) FROM facts GROUP BY CUBE(region, product)");
        // CUBE(2 cols) → 4 sets
        assert_eq!(agg.grouping_set_count(), 4);
    }

    #[test]
    fn window_spec_default_frame() {
        let frame = WindowFrame::default();
        assert_eq!(frame.unit, FrameUnit::Range);
        assert_eq!(frame.start, FrameBound::UnboundedPreceding);
        assert_eq!(frame.end, FrameBound::CurrentRow);
    }

    #[test]
    fn window_spec_round_trip_json() {
        let spec = WindowSpec {
            function: OlapFunction::Rank,
            args: vec![],
            partition_by: vec!["dept".to_string()],
            order_by: vec!["salary".to_string()],
            frame: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        let back: WindowSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(back.partition_by, spec.partition_by);
    }
}

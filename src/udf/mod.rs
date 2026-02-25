pub mod geo;
pub mod wasm;
#[cfg(feature = "ml")]
pub mod onnx;
pub mod search;
#[cfg(feature = "nlp")]
pub mod candle;

use datafusion::execution::context::SessionContext;

/// Register all built-in UDFs
pub fn register_all_udfs(ctx: &SessionContext) {
    geo::register_geo_functions(ctx);
    wasm::register_wasm_functions(ctx);
    search::register_search_functions(ctx);
    crate::timeseries::register_timeseries_functions(ctx);
    // Note: Candle UDFs are registered separately in engine initialization
    // because they require a CandleEngine instance
}

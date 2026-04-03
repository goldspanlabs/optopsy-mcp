//! Natural-language Trading DSL for the Rhai backtesting engine.
//!
//! This module provides a two-layer DSL system:
//!
//! 1. **Transpiler** (`parser` + `codegen`): Converts indent-based, sentence-style
//!    `.trading` scripts into valid Rhai source code that the existing engine can execute.
//!
//! 2. **Custom Syntax** (`syntax`): Registers inline Rhai syntactic sugar (e.g.,
//!    `buy 100 shares`, `exit_position "reason"`) so that generated or hand-written
//!    Rhai scripts can use DSL-like patterns.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use optopsy_mcp::scripting::dsl;
//!
//! let trading_dsl = r#"
//! strategy "My Strategy"
//!   symbol AAPL
//!   interval daily
//!   data ohlcv
//!   indicators sma:200
//!
//! on each bar
//!   skip when has positions
//!   when close > sma(200) then
//!     buy size_by_equity(1.0) shares
//! "#;
//!
//! let rhai_source = dsl::transpile(trading_dsl)?;
//! // rhai_source is now valid Rhai that can be passed to run_script_backtest()
//! ```

pub mod codegen;
pub mod error;
pub mod parser;
pub mod syntax;
#[cfg(test)]
mod tests;
mod validate;

pub use error::DslError;
pub use syntax::register_dsl_syntax;

/// Transpile a `.trading` DSL source string into valid Rhai source code.
///
/// This is the main entry point for the DSL system. It parses the indent-based
/// natural-language script and emits equivalent Rhai code that works with the
/// existing `run_script_backtest()` engine.
///
/// # Errors
///
/// Returns `DslError` with line numbers if the DSL source is malformed, or if
/// intraday-only keywords are used with a non-intraday interval.
pub fn transpile(source: &str) -> Result<String, DslError> {
    let program = parser::parse(source)?;
    validate::check_interval_time_keywords(&program)?;
    validate::check_portfolio_access(&program)?;
    validate::check_quantifiers(&program)?;
    Ok(codegen::generate(&program))
}

/// Check if a source string looks like Trading DSL (vs. plain Rhai).
///
/// Checks whether the first non-comment, non-empty line starts with `strategy `.
pub fn is_trading_dsl(source: &str) -> bool {
    let trimmed = source.trim();
    for line in trimmed.lines() {
        let line = line.trim();
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        // First non-comment line decides
        return line.starts_with("strategy ");
    }
    false
}

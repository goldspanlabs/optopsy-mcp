//! Code generator: transpiles the DSL IR into valid Rhai source code.
//!
//! The generated Rhai uses the same callback conventions as hand-written scripts:
//! `config()`, `on_bar(ctx)`, `on_exit_check(ctx, pos)`, etc.
//!
//! Expression rewriting automatically qualifies bare identifiers (like `close`,
//! `sma(200)`) with `ctx.` so DSL authors never write `ctx.` explicitly.

// This is a code generator — push_str(&format!(...)) is the natural pattern.
#![allow(clippy::format_push_string)]

use std::collections::HashSet;

use super::parser::*;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Generate Rhai source code from a parsed DSL program.
pub fn generate(program: &DslProgram) -> String {
    let mut out = String::with_capacity(2048);

    // Collect top-level variable names (params + state) so that `set` inside
    // callbacks can distinguish reassignment from new local variable declaration.
    let mut scope_vars: HashSet<String> = HashSet::new();
    for p in &program.params {
        scope_vars.insert(p.name.clone());
    }
    for s in &program.states {
        scope_vars.insert(s.name.clone());
    }

    out.push_str("// Auto-generated from Trading DSL — do not edit by hand.\n\n");

    // Extern params
    for p in &program.params {
        generate_param(&mut out, p);
    }
    if !program.params.is_empty() {
        out.push('\n');
    }

    // State variables
    for s in &program.states {
        out.push_str(&format!("let {} = {};\n", s.name, s.default));
    }
    if !program.states.is_empty() {
        out.push('\n');
    }

    // config()
    if let Some(ref strat) = program.strategy {
        generate_config(&mut out, strat);
        out.push('\n');
    }

    // on_bar(ctx)
    if let Some(ref stmts) = program.on_bar {
        generate_on_bar(&mut out, stmts, &scope_vars);
        out.push('\n');
    }

    // on_exit_check(ctx, pos)
    if let Some(ref stmts) = program.on_exit_check {
        generate_on_exit_check(&mut out, stmts, &scope_vars);
        out.push('\n');
    }

    // on_position_opened(ctx, pos)
    if let Some(ref stmts) = program.on_position_opened {
        generate_callback(
            &mut out,
            "on_position_opened",
            "ctx, pos",
            stmts,
            CallbackKind::SideEffect,
            &scope_vars,
        );
        out.push('\n');
    }

    // on_position_closed(ctx, pos, exit_type)
    if let Some(ref stmts) = program.on_position_closed {
        generate_callback(
            &mut out,
            "on_position_closed",
            "ctx, pos, exit_type",
            stmts,
            CallbackKind::SideEffect,
            &scope_vars,
        );
        out.push('\n');
    }

    // on_end(ctx)
    if let Some(ref stmts) = program.on_end {
        generate_callback(
            &mut out,
            "on_end",
            "ctx",
            stmts,
            CallbackKind::SideEffect,
            &scope_vars,
        );
        out.push('\n');
    }

    out
}

// ---------------------------------------------------------------------------
// Callback kinds (controls how actions are emitted)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum CallbackKind {
    /// Accumulates actions in `__actions` and returns the array (on_bar).
    ActionArray,
    /// Returns a single action directly (on_exit_check).
    SingleAction,
    /// No return value — side effects only.
    SideEffect,
}

// ---------------------------------------------------------------------------
// Config generation
// ---------------------------------------------------------------------------

/// Map config values: universal param keywords become `params.XXX`,
/// numeric values pass through, and other identifiers are quoted as strings.
fn config_value(val: &str) -> String {
    match val {
        "SYMBOL" => "params.SYMBOL".to_string(),
        "CAPITAL" => "params.CAPITAL".to_string(),
        v if v.parse::<f64>().is_ok() => v.to_string(),
        v if v.starts_with('"') => v.to_string(),
        v if v.starts_with("params.") => v.to_string(),
        other => format!("\"{other}\""),
    }
}

fn generate_config(out: &mut String, s: &StrategyBlock) {
    out.push_str("fn config() {\n");
    out.push_str("    #{\n");
    out.push_str(&format!("        symbol: {},\n", config_value(&s.symbol)));
    out.push_str(&format!("        capital: {},\n", config_value(&s.capital)));
    out.push_str(&format!("        interval: \"{}\",\n", s.interval));

    // data block
    out.push_str("        data: #{\n");
    out.push_str(&format!("            ohlcv: {},\n", s.data_ohlcv));
    out.push_str(&format!("            options: {},\n", s.data_options));
    if !s.indicators.is_empty() {
        let ind_list: Vec<String> = s.indicators.iter().map(|i| format!("\"{i}\"")).collect();
        out.push_str(&format!(
            "            indicators: [{}],\n",
            ind_list.join(", ")
        ));
    }
    out.push_str("        },\n");

    // engine block (only if any engine settings present)
    let has_engine = s.slippage.is_some() || s.expiration_filter.is_some();
    if has_engine {
        out.push_str("        engine: #{\n");
        if let Some(ref slip) = s.slippage {
            out.push_str(&format!("            slippage: \"{slip}\",\n"));
        }
        if let Some(ref ef) = s.expiration_filter {
            out.push_str(&format!("            expiration_filter: \"{ef}\",\n"));
        }
        out.push_str("        },\n");
    }

    // defaults block (only if any defaults present)
    if s.max_positions.is_some() {
        out.push_str("        defaults: #{\n");
        if let Some(mp) = s.max_positions {
            out.push_str(&format!("            max_positions: {mp},\n"));
        }
        out.push_str("        },\n");
    }

    // cross_symbols
    if !s.cross_symbols.is_empty() {
        let syms: Vec<String> = s.cross_symbols.iter().map(|s| format!("\"{s}\"")).collect();
        out.push_str(&format!("        cross_symbols: [{}],\n", syms.join(", ")));
    }

    out.push_str("    }\n");
    out.push_str("}\n");
}

// ---------------------------------------------------------------------------
// Param generation
// ---------------------------------------------------------------------------

fn generate_param(out: &mut String, p: &ParamDecl) {
    if p.choices.is_empty() {
        out.push_str(&format!(
            "let {} = extern(\"{}\", {}, \"{}\");\n",
            p.name, p.name, p.default, p.description
        ));
    } else {
        let choices_list: Vec<String> = p.choices.iter().map(|c| format!("\"{c}\"")).collect();
        out.push_str(&format!(
            "let {} = extern(\"{}\", {}, \"{}\", [{}]);\n",
            p.name,
            p.name,
            p.default,
            p.description,
            choices_list.join(", ")
        ));
    }
}

// ---------------------------------------------------------------------------
// on_bar generation
// ---------------------------------------------------------------------------

fn generate_on_bar(out: &mut String, stmts: &[Stmt], scope_vars: &HashSet<String>) {
    out.push_str("fn on_bar(ctx) {\n");
    out.push_str("    let __actions = [];\n");
    generate_stmts(out, stmts, 1, CallbackKind::ActionArray, scope_vars);
    out.push_str("    __actions\n");
    out.push_str("}\n");
}

// ---------------------------------------------------------------------------
// on_exit_check generation
// ---------------------------------------------------------------------------

fn generate_on_exit_check(out: &mut String, stmts: &[Stmt], scope_vars: &HashSet<String>) {
    out.push_str("fn on_exit_check(ctx, pos) {\n");
    generate_stmts(out, stmts, 1, CallbackKind::SingleAction, scope_vars);
    // If no explicit return, default to hold
    out.push_str("    hold_position()\n");
    out.push_str("}\n");
}

// ---------------------------------------------------------------------------
// Generic callback generation
// ---------------------------------------------------------------------------

fn generate_callback(
    out: &mut String,
    name: &str,
    params: &str,
    stmts: &[Stmt],
    kind: CallbackKind,
    scope_vars: &HashSet<String>,
) {
    out.push_str(&format!("fn {name}({params}) {{\n"));
    if kind == CallbackKind::ActionArray {
        out.push_str("    let __actions = [];\n");
    }
    generate_stmts(out, stmts, 1, kind, scope_vars);
    if kind == CallbackKind::ActionArray {
        out.push_str("    __actions\n");
    }
    out.push_str("}\n");
}

// ---------------------------------------------------------------------------
// Statement generation (recursive)
// ---------------------------------------------------------------------------

fn generate_stmts(
    out: &mut String,
    stmts: &[Stmt],
    depth: usize,
    kind: CallbackKind,
    scope_vars: &HashSet<String>,
) {
    let indent = "    ".repeat(depth);

    for stmt in stmts {
        match stmt {
            Stmt::Require { indicators, .. } => {
                let ind_list: Vec<String> = indicators.iter().map(|i| format!("\"{i}\"")).collect();
                let ret_val = match kind {
                    CallbackKind::ActionArray => "return [];",
                    CallbackKind::SingleAction => "return hold_position();",
                    CallbackKind::SideEffect => "return;",
                };
                out.push_str(&format!(
                    "{indent}if !ctx.indicators_ready([{}]) {{ {ret_val} }}\n",
                    ind_list.join(", ")
                ));
            }

            Stmt::SkipWhen { condition, .. } => {
                let cond = rewrite_expr(condition);
                let ret_val = match kind {
                    CallbackKind::ActionArray => "return [];",
                    CallbackKind::SingleAction => "return hold_position();",
                    CallbackKind::SideEffect => "return;",
                };
                out.push_str(&format!("{indent}if {cond} {{ {ret_val} }}\n"));
            }

            Stmt::Set { name, expr, .. } => {
                let rhs = rewrite_expr(expr);
                if scope_vars.contains(name) {
                    // Reassign existing top-level state/param variable
                    out.push_str(&format!("{indent}{name} = {rhs};\n"));
                } else {
                    // Declare new local variable
                    out.push_str(&format!("{indent}let {name} = {rhs};\n"));
                }
            }

            Stmt::When {
                condition,
                then_body,
                else_body,
                ..
            } => {
                let cond = rewrite_expr(condition);
                out.push_str(&format!("{indent}if {cond} {{\n"));
                generate_stmts(out, then_body, depth + 1, kind, scope_vars);
                out.push_str(&format!("{indent}}}"));

                // Flatten chained when/otherwise into if/else-if/else iteratively
                let mut current_else = else_body.as_deref();
                while let Some(else_stmts) = current_else {
                    // Single When in else_body → emit as `else if`
                    if else_stmts.len() == 1 {
                        if let Stmt::When {
                            condition: ec,
                            then_body: et,
                            else_body: ee,
                            ..
                        } = &else_stmts[0]
                        {
                            let ec_rw = rewrite_expr(ec);
                            out.push_str(&format!(" else if {ec_rw} {{\n"));
                            generate_stmts(out, et, depth + 1, kind, scope_vars);
                            out.push_str(&format!("{indent}}}"));
                            current_else = ee.as_deref();
                            continue;
                        }
                    }
                    // Non-When else body → emit as final `else`
                    out.push_str(" else {\n");
                    generate_stmts(out, else_stmts, depth + 1, kind, scope_vars);
                    out.push_str(&format!("{indent}}}"));
                    break;
                }
                out.push('\n');
            }

            Stmt::Buy { qty_expr, .. } => {
                let qty = rewrite_expr(qty_expr);
                match kind {
                    CallbackKind::ActionArray => {
                        out.push_str(&format!("{indent}__actions.push(buy_stock({qty}));\n"));
                    }
                    _ => {
                        out.push_str(&format!("{indent}buy_stock({qty});\n"));
                    }
                }
            }

            Stmt::Sell { qty_expr, .. } => {
                let qty = rewrite_expr(qty_expr);
                // Emit quantity validation guard
                out.push_str(&format!("{indent}let __sell_qty = {qty};\n"));
                out.push_str(&format!("{indent}if __sell_qty > 0 {{\n"));
                match kind {
                    CallbackKind::ActionArray => {
                        out.push_str(&format!(
                            "{indent}    __actions.push(sell_stock(__sell_qty));\n"
                        ));
                    }
                    _ => {
                        out.push_str(&format!("{indent}    sell_stock(__sell_qty);\n"));
                    }
                }
                out.push_str(&format!("{indent}}}\n"));
            }

            Stmt::HoldPosition { .. } => match kind {
                CallbackKind::SingleAction => {
                    out.push_str(&format!("{indent}return hold_position();\n"));
                }
                _ => {
                    out.push_str(&format!("{indent}hold_position();\n"));
                }
            },

            Stmt::ClosePosition { reason, .. } => match kind {
                CallbackKind::SingleAction => {
                    out.push_str(&format!("{indent}return close_position(\"{reason}\");\n"));
                }
                CallbackKind::ActionArray => {
                    out.push_str(&format!(
                        "{indent}__actions.push(close_position(\"{reason}\"));\n"
                    ));
                }
                CallbackKind::SideEffect => {
                    out.push_str(&format!("{indent}close_position(\"{reason}\");\n"));
                }
            },

            Stmt::ClosePositionById {
                id_expr, reason, ..
            } => {
                let id = rewrite_expr(id_expr);
                match kind {
                    CallbackKind::SingleAction => {
                        out.push_str(&format!(
                            "{indent}return close_position_id({id}, \"{reason}\");\n"
                        ));
                    }
                    CallbackKind::ActionArray => {
                        out.push_str(&format!(
                            "{indent}__actions.push(close_position_id({id}, \"{reason}\"));\n"
                        ));
                    }
                    CallbackKind::SideEffect => {
                        out.push_str(&format!("{indent}close_position_id({id}, \"{reason}\");\n"));
                    }
                }
            }

            Stmt::StopBacktest { reason, .. } => match kind {
                CallbackKind::ActionArray => {
                    out.push_str(&format!(
                        "{indent}__actions.push(stop_backtest(\"{reason}\"));\n"
                    ));
                }
                CallbackKind::SingleAction => {
                    out.push_str(&format!("{indent}return stop_backtest(\"{reason}\");\n"));
                }
                CallbackKind::SideEffect => {
                    out.push_str(&format!("{indent}stop_backtest(\"{reason}\");\n"));
                }
            },

            Stmt::OpenStrategy { call, .. } => {
                let rw = rewrite_expr(call);
                match kind {
                    CallbackKind::ActionArray => {
                        out.push_str(&format!("{indent}let __spread = {rw};\n"));
                        out.push_str(&format!(
                            "{indent}if __spread != () {{ __actions.push(__spread); }}\n"
                        ));
                    }
                    _ => {
                        out.push_str(&format!("{indent}{rw};\n"));
                    }
                }
            }

            Stmt::Plot {
                name,
                expr,
                display,
                ..
            } => {
                let rw = rewrite_expr(expr);
                if let Some(ref disp) = display {
                    out.push_str(&format!(
                        "{indent}ctx.plot_with(\"{name}\", {rw}, \"{disp}\");\n"
                    ));
                } else {
                    out.push_str(&format!("{indent}ctx.plot(\"{name}\", {rw});\n"));
                }
            }

            Stmt::AddTo { expr, name, .. } => {
                let rw = rewrite_expr(expr);
                out.push_str(&format!("{indent}{name} += {rw};\n"));
            }

            Stmt::SubtractFrom { expr, name, .. } => {
                let rw = rewrite_expr(expr);
                out.push_str(&format!("{indent}{name} -= {rw};\n"));
            }

            Stmt::MultiplyBy { name, expr, .. } => {
                let rw = rewrite_expr(expr);
                out.push_str(&format!("{indent}{name} *= {rw};\n"));
            }

            Stmt::DivideBy { name, expr, .. } => {
                let rw = rewrite_expr(expr);
                out.push_str(&format!("{indent}{name} /= {rw};\n"));
            }

            Stmt::ForEach {
                var,
                iterable,
                body,
                ..
            } => {
                let iter_rw = rewrite_expr(iterable);
                out.push_str(&format!("{indent}for {var} in {iter_rw} {{\n"));
                generate_stmts(out, body, depth + 1, kind, scope_vars);
                out.push_str(&format!("{indent}}}\n"));
            }

            Stmt::Return { expr, .. } => {
                let rw = rewrite_expr(expr);
                out.push_str(&format!("{indent}return {rw};\n"));
            }

            Stmt::Raw { code, .. } => {
                out.push_str(&format!("{indent}{code}\n"));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Expression rewriting
// ---------------------------------------------------------------------------

/// Known `ctx` properties (accessed without parentheses).
const CTX_PROPERTIES: &[&str] = &[
    "close",
    "open",
    "high",
    "low",
    "volume",
    "cash",
    "equity",
    "position_count",
    "unrealized_pnl",
    "realized_pnl",
    "total_exposure",
    "bar_idx",
    "date",
    "datetime",
];

/// Known `ctx` methods (accessed with parentheses).
const CTX_METHODS: &[&str] = &[
    // Indicators
    "sma",
    "ema",
    "rsi",
    "atr",
    "macd_line",
    "macd_signal",
    "macd_hist",
    "bbands_upper",
    "bbands_mid",
    "bbands_lower",
    "stochastic",
    "cci",
    "obv",
    "adx",
    "plus_di",
    "minus_di",
    "psar",
    "supertrend",
    "keltner_upper",
    "keltner_lower",
    "donchian_upper",
    "donchian_mid",
    "donchian_lower",
    "tr",
    "williams_r",
    "mfi",
    "rank",
    "iv_rank",
    // Generic
    "indicator",
    "indicator_with",
    // Lookback
    "sma_at",
    "ema_at",
    "rsi_at",
    "indicator_at",
    "crossed_above",
    "crossed_below",
    // Range queries
    "highest_high",
    "lowest_low",
    "highest_close",
    "lowest_close",
    // Date/time
    "day_of_week",
    "month",
    "day_of_month",
    "hour",
    "minute",
    "week_of_year",
    // Portfolio
    "has_positions",
    "positions",
    "price_of",
    "price_of_col",
    "indicators_ready",
    // Position sizing
    "size_by_equity",
    "size_by_risk",
    "size_by_volatility",
    "size_by_kelly",
    // Strategy building
    "build_strategy",
    "price",
    // Strategy constructors
    "long_call",
    "short_call",
    "long_put",
    "short_put",
    "covered_call",
    "bull_call_spread",
    "bear_call_spread",
    "bull_put_spread",
    "bear_put_spread",
    "long_straddle",
    "short_straddle",
    "long_strangle",
    "short_strangle",
    "long_call_butterfly",
    "short_call_butterfly",
    "long_put_butterfly",
    "short_put_butterfly",
    "long_call_condor",
    "short_call_condor",
    "long_put_condor",
    "short_put_condor",
    "iron_condor",
    "reverse_iron_condor",
    "iron_butterfly",
    "reverse_iron_butterfly",
    "call_calendar",
    "put_calendar",
    "call_diagonal",
    "put_diagonal",
    "double_calendar",
    "double_diagonal",
    // Plotting
    "plot",
    "plot_with",
];

/// Rewrite a DSL expression into a valid Rhai expression.
///
/// - Bare context properties (`close`, `equity`) → `ctx.close`, `ctx.equity`
/// - Bare context methods (`sma(200)`) → `ctx.sma(200)`
/// - `has positions` → `ctx.has_positions()`
/// - `no positions` → `!ctx.has_positions()`
/// - `and` → `&&`, `or` → `||`
pub fn rewrite_expr(expr: &str) -> String {
    let chars: Vec<char> = expr.chars().collect();
    let mut result = String::with_capacity(expr.len() + 32);
    let mut i = 0;

    while i < chars.len() {
        // Skip string literals (preserve contents verbatim)
        if chars[i] == '"' {
            result.push(chars[i]);
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    result.push(chars[i]);
                    i += 1;
                }
                if i < chars.len() {
                    result.push(chars[i]);
                    i += 1;
                }
            }
            if i < chars.len() {
                result.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // Extract a word (alphanumeric + underscore)
        if chars[i].is_alphabetic() || chars[i] == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            // Collect word from chars (safe for non-ASCII)
            let word: String = chars[start..i].iter().collect();

            // Check if preceded by a dot (already qualified)
            let preceded_by_dot = start > 0 && chars[start - 1] == '.';

            if preceded_by_dot {
                result.push_str(&word);
            } else if word == "has" && matches_word_at(&chars, i, "positions") {
                // "has positions" → ctx.has_positions()
                i += "positions".len() + 1; // skip space + "positions"
                result.push_str("ctx.has_positions()");
            } else if word == "no" && matches_word_at(&chars, i, "positions") {
                // "no positions" → !ctx.has_positions()
                i += "positions".len() + 1;
                result.push_str("!ctx.has_positions()");
            } else if word == "and" {
                result.push_str("&&");
            } else if word == "or" {
                result.push_str("||");
            } else if word == "not" {
                result.push('!');
            } else if is_ctx_property(&word) && !is_followed_by_paren(&chars, i) {
                result.push_str("ctx.");
                result.push_str(&word);
            } else if is_ctx_method(&word) && is_followed_by_paren(&chars, i) {
                result.push_str("ctx.");
                result.push_str(&word);
            } else {
                result.push_str(&word);
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }

    result
}

fn is_ctx_property(word: &str) -> bool {
    CTX_PROPERTIES.contains(&word)
}

fn is_ctx_method(word: &str) -> bool {
    CTX_METHODS.contains(&word)
}

fn is_followed_by_paren(chars: &[char], pos: usize) -> bool {
    let mut j = pos;
    while j < chars.len() && chars[j].is_whitespace() {
        j += 1;
    }
    j < chars.len() && chars[j] == '('
}

/// Check if `expected` word appears at `pos` (after optional whitespace).
fn matches_word_at(chars: &[char], pos: usize, expected: &str) -> bool {
    let mut j = pos;
    // Skip whitespace
    while j < chars.len() && chars[j] == ' ' {
        j += 1;
    }
    let expected_chars: Vec<char> = expected.chars().collect();
    if j + expected_chars.len() > chars.len() {
        return false;
    }
    for (k, &ec) in expected_chars.iter().enumerate() {
        if chars[j + k] != ec {
            return false;
        }
    }
    // Ensure it's a full word (not a prefix of a longer identifier)
    let end = j + expected_chars.len();
    end >= chars.len() || (!chars[end].is_alphanumeric() && chars[end] != '_')
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rewrite_properties() {
        assert_eq!(rewrite_expr("close > 100"), "ctx.close > 100");
        assert_eq!(rewrite_expr("open + high"), "ctx.open + ctx.high");
    }

    #[test]
    fn test_rewrite_methods() {
        assert_eq!(rewrite_expr("sma(200)"), "ctx.sma(200)");
        assert_eq!(
            rewrite_expr("sma(50) > sma(200)"),
            "ctx.sma(50) > ctx.sma(200)"
        );
    }

    #[test]
    fn test_rewrite_dot_qualified_untouched() {
        // Properties after a dot are not rewritten (pos.pnl_pct stays as-is)
        // Users shouldn't write ctx. in DSL, but if they use pos. it should work:
        assert_eq!(rewrite_expr("pos.pnl_pct > 0.5"), "pos.pnl_pct > 0.5");
    }

    #[test]
    fn test_rewrite_has_positions() {
        assert_eq!(rewrite_expr("has positions"), "ctx.has_positions()");
        assert_eq!(rewrite_expr("no positions"), "!ctx.has_positions()");
    }

    #[test]
    fn test_rewrite_boolean_operators() {
        assert_eq!(
            rewrite_expr("close > 100 and rsi(14) < 30"),
            "ctx.close > 100 && ctx.rsi(14) < 30"
        );
        assert_eq!(
            rewrite_expr("close > 100 or close < 50"),
            "ctx.close > 100 || ctx.close < 50"
        );
    }

    #[test]
    fn test_rewrite_preserves_strings() {
        assert_eq!(rewrite_expr("\"close is high\""), "\"close is high\"");
    }

    #[test]
    fn test_rewrite_strategy_call() {
        assert_eq!(
            rewrite_expr("iron_condor(0.30, 0.30, 45)"),
            "ctx.iron_condor(0.30, 0.30, 45)"
        );
    }

    #[test]
    fn test_rewrite_user_vars_untouched() {
        assert_eq!(rewrite_expr("THRESHOLD * 2"), "THRESHOLD * 2");
        assert_eq!(rewrite_expr("my_counter + 1"), "my_counter + 1");
    }

    #[test]
    fn test_rewrite_mixed_expression() {
        assert_eq!(
            rewrite_expr("close > sma(200) * (1 + THRESHOLD) and rsi(14) < 30"),
            "ctx.close > ctx.sma(200) * (1 + THRESHOLD) && ctx.rsi(14) < 30"
        );
    }

    #[test]
    fn test_rewrite_position_sizing() {
        assert_eq!(
            rewrite_expr("size_by_equity(1.0)"),
            "ctx.size_by_equity(1.0)"
        );
        assert_eq!(
            rewrite_expr("size_by_risk(0.02, close - atr(14) * 2)"),
            "ctx.size_by_risk(0.02, ctx.close - ctx.atr(14) * 2)"
        );
    }
}

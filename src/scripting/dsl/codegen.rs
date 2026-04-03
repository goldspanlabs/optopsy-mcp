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
// Auto-detection of indicators from DSL body expressions
// ---------------------------------------------------------------------------

/// Indicators that require pre-computation (must appear in config.data.indicators).
const PRECOMPUTED_INDICATORS: &[&str] = &[
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
];

/// Walk all statement blocks in the program and collect `"name:period"` indicator
/// specs from expression text and `require` directives.
fn collect_indicators(program: &DslProgram) -> Vec<String> {
    let mut specs = Vec::new();
    let mut seen = HashSet::new();

    // Helper to process an Option<Vec<Stmt>>
    let mut process = |stmts: &Option<Vec<Stmt>>| {
        if let Some(ref s) = stmts {
            collect_from_stmts(s, &mut specs, &mut seen);
        }
    };

    process(&program.on_bar);
    process(&program.on_exit_check);
    process(&program.on_position_opened);
    process(&program.on_position_closed);
    process(&program.on_end);

    // Procedural body
    collect_from_stmts(&program.body, &mut specs, &mut seen);

    specs
}

/// Recursively walk statements, scanning every expression string for indicator
/// call patterns and collecting `require` indicator lists.
fn collect_from_stmts(stmts: &[Stmt], specs: &mut Vec<String>, seen: &mut HashSet<String>) {
    for stmt in stmts {
        match stmt {
            Stmt::Require { indicators, .. } => {
                for ind in indicators {
                    if seen.insert(ind.clone()) {
                        specs.push(ind.clone());
                    }
                }
            }
            Stmt::SkipWhen { condition, .. } => scan_expr(condition, specs, seen),
            Stmt::Set { expr, .. } => scan_expr(expr, specs, seen),
            Stmt::When {
                condition,
                then_body,
                else_body,
                ..
            } => {
                scan_expr(condition, specs, seen);
                collect_from_stmts(then_body, specs, seen);
                if let Some(ref eb) = else_body {
                    collect_from_stmts(eb, specs, seen);
                }
            }
            Stmt::Buy {
                qty_expr,
                order_type,
                ..
            } => {
                scan_expr(qty_expr, specs, seen);
                scan_order_modifier(order_type, specs, seen);
            }
            Stmt::Sell {
                qty_expr,
                order_type,
                ..
            } => {
                scan_expr(qty_expr, specs, seen);
                scan_order_modifier(order_type, specs, seen);
            }
            Stmt::OpenStrategy { call, .. } => scan_expr(call, specs, seen),
            Stmt::Plot { expr, .. } => scan_expr(expr, specs, seen),
            Stmt::AddTo { expr, .. } => scan_expr(expr, specs, seen),
            Stmt::SubtractFrom { expr, .. } => scan_expr(expr, specs, seen),
            Stmt::MultiplyBy { expr, .. } => scan_expr(expr, specs, seen),
            Stmt::DivideBy { expr, .. } => scan_expr(expr, specs, seen),
            Stmt::ForEach { iterable, body, .. } => {
                scan_expr(iterable, specs, seen);
                collect_from_stmts(body, specs, seen);
            }
            Stmt::Return { expr, .. } => scan_expr(expr, specs, seen),
            Stmt::Raw { code, .. } => scan_expr(code, specs, seen),
            Stmt::TryOpen { call, body, .. } => {
                scan_expr(call, specs, seen);
                collect_from_stmts(body, specs, seen);
            }
            // Variants with no expressions to scan
            Stmt::HoldPosition { .. }
            | Stmt::ClosePosition { .. }
            | Stmt::ClosePositionById { .. }
            | Stmt::StopBacktest { .. }
            | Stmt::CancelOrders { .. } => {}
        }
    }
}

/// Scan an order modifier for indicator patterns in price expressions.
fn scan_order_modifier(om: &OrderModifier, specs: &mut Vec<String>, seen: &mut HashSet<String>) {
    match om {
        OrderModifier::Limit { price } | OrderModifier::Stop { price } => {
            scan_expr(price, specs, seen);
        }
        OrderModifier::Market => {}
    }
}

/// Character-level scanner that finds `WORD(NUMBER)` patterns where WORD is in
/// `PRECOMPUTED_INDICATORS`. Extracts as `"word:number"`. Also handles `WORD()`
/// for zero-arg indicators (emitted as just `"word"`).
fn scan_expr(expr: &str, specs: &mut Vec<String>, seen: &mut HashSet<String>) {
    let bytes = expr.as_bytes();
    let len = bytes.len();
    let mut i = 0;

    while i < len {
        // Look for the start of an identifier
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word = &expr[start..i];

            // Check if this word is a known indicator followed by '('
            if i < len && bytes[i] == b'(' && PRECOMPUTED_INDICATORS.contains(&word) {
                let paren_start = i + 1;
                // Find the matching ')'
                if let Some(paren_end) = expr[paren_start..].find(')') {
                    let arg = expr[paren_start..paren_start + paren_end].trim();
                    // Only extract if arg is a numeric literal (not a variable like BB_PERIOD)
                    let is_numeric =
                        arg.is_empty() || arg.bytes().all(|b| b.is_ascii_digit() || b == b'.');
                    if is_numeric {
                        let spec = if arg.is_empty() {
                            word.to_string()
                        } else {
                            format!("{word}:{arg}")
                        };
                        if seen.insert(spec.clone()) {
                            specs.push(spec);
                        }
                    }
                    i = paren_start + paren_end + 1;
                }
            }
        } else {
            i += 1;
        }
    }
}

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

    // Metadata headers (for ScriptMeta extraction)
    if let Some(ref strat) = program.strategy {
        out.push_str(&format!("//! name: {}\n", strat.name));
        if let Some(ref desc) = strat.description {
            out.push_str(&format!("//! description: {desc}\n"));
        }
        if let Some(ref cat) = strat.category {
            out.push_str(&format!("//! category: {cat}\n"));
        }
        if let Some(ref hyp) = strat.hypothesis {
            out.push_str(&format!("//! hypothesis: {hyp}\n"));
        }
        if !strat.tags.is_empty() {
            out.push_str(&format!("//! tags: {}\n", strat.tags.join(", ")));
        }
        if !strat.regime.is_empty() {
            out.push_str(&format!("//! regime: {}\n", strat.regime.join(", ")));
        }
        out.push('\n');
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
        generate_config(&mut out, strat, program);
        out.push('\n');
    }

    // Procedural mode: body statements become on_bar
    if !program.body.is_empty() {
        generate_on_bar(&mut out, &program.body, &scope_vars);
        out.push('\n');
    }

    // on_bar(ctx) — callback mode
    if program.body.is_empty() {
        if let Some(ref stmts) = program.on_bar {
            generate_on_bar(&mut out, stmts, &scope_vars);
            out.push('\n');
        }
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

fn generate_config(out: &mut String, s: &StrategyBlock, program: &DslProgram) {
    out.push_str("fn config() {\n");
    out.push_str("    #{\n");
    out.push_str(&format!("        symbol: {},\n", config_value(&s.symbol)));
    out.push_str(&format!("        capital: {},\n", config_value(&s.capital)));
    out.push_str(&format!("        interval: \"{}\",\n", s.interval));

    // data block
    out.push_str("        data: #{\n");
    out.push_str(&format!("            ohlcv: {},\n", s.data_ohlcv));
    out.push_str(&format!("            options: {},\n", s.data_options));

    // Merge explicit indicators with auto-detected ones
    let auto_detected = collect_indicators(program);
    let mut merged: Vec<String> = s.indicators.clone();
    let explicit_set: HashSet<&str> = s.indicators.iter().map(|s| s.as_str()).collect();
    for spec in &auto_detected {
        if !explicit_set.contains(spec.as_str()) {
            merged.push(spec.clone());
        }
    }
    if !merged.is_empty() {
        let ind_list: Vec<String> = merged.iter().map(|i| format!("\"{i}\"")).collect();
        out.push_str(&format!(
            "            indicators: [{}],\n",
            ind_list.join(", ")
        ));
    }
    if !s.cross_symbols.is_empty() {
        let syms: Vec<String> = s.cross_symbols.iter().map(|s| format!("\"{s}\"")).collect();
        out.push_str(&format!(
            "            cross_symbols: [{}],\n",
            syms.join(", ")
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

    if s.procedural {
        out.push_str("        procedural: true,\n");
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
// Exit modifier emission
// ---------------------------------------------------------------------------

fn emit_exit_modifiers(out: &mut String, mods: &ExitModifiers, indent: &str) {
    if let Some(ref sl) = mods.stop_loss {
        match sl {
            OrderExitSpec::Percent(v) => {
                out.push_str(&format!("{indent}__order.stop_loss_pct = {v};\n"));
            }
            OrderExitSpec::Dollar(v) => {
                out.push_str(&format!("{indent}__order.stop_loss_dollar = {v};\n"));
            }
        }
    }
    if let Some(ref pt) = mods.profit_target {
        match pt {
            OrderExitSpec::Percent(v) => {
                out.push_str(&format!("{indent}__order.profit_target_pct = {v};\n"));
            }
            OrderExitSpec::Dollar(v) => {
                out.push_str(&format!("{indent}__order.profit_target_dollar = {v};\n"));
            }
        }
    }
    if let Some(ref ts) = mods.trailing_stop {
        match ts {
            OrderExitSpec::Percent(v) => {
                out.push_str(&format!("{indent}__order.trailing_stop_pct = {v};\n"));
            }
            OrderExitSpec::Dollar(v) => {
                out.push_str(&format!("{indent}__order.trailing_stop_dollar = {v};\n"));
            }
        }
    }
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
                if scope_vars.contains(name) || name.contains('.') {
                    // Reassign existing top-level state/param variable or dotted path
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

            Stmt::Buy {
                qty_expr,
                order_type,
                exit_modifiers,
                ..
            } => {
                let qty = rewrite_expr(qty_expr);
                let call = match order_type {
                    OrderModifier::Market => format!("buy_stock({qty})"),
                    OrderModifier::Limit { price } => {
                        let p = rewrite_expr(price);
                        format!("buy_limit({qty}, {p})")
                    }
                    OrderModifier::Stop { price } => {
                        let p = rewrite_expr(price);
                        format!("buy_stop({qty}, {p})")
                    }
                };

                let has_mods = exit_modifiers.stop_loss.is_some()
                    || exit_modifiers.profit_target.is_some()
                    || exit_modifiers.trailing_stop.is_some();

                if has_mods && kind == CallbackKind::ActionArray {
                    out.push_str(&format!("{indent}let __order = {call};\n"));
                    emit_exit_modifiers(out, exit_modifiers, &indent);
                    out.push_str(&format!("{indent}__actions.push(__order);\n"));
                } else {
                    match kind {
                        CallbackKind::ActionArray => {
                            out.push_str(&format!("{indent}__actions.push({call});\n"));
                        }
                        _ => {
                            out.push_str(&format!("{indent}{call};\n"));
                        }
                    }
                }
            }

            Stmt::Sell {
                qty_expr,
                order_type,
                exit_modifiers,
                ..
            } => {
                let qty = rewrite_expr(qty_expr);
                // Build the guarded call with __sell_qty directly to avoid
                // string-replace corruption when qty appears in price expressions.
                let call_with_guard = match order_type {
                    OrderModifier::Market => "sell_stock(__sell_qty)".to_string(),
                    OrderModifier::Limit { price } => {
                        let p = rewrite_expr(price);
                        format!("sell_limit(__sell_qty, {p})")
                    }
                    OrderModifier::Stop { price } => {
                        let p = rewrite_expr(price);
                        format!("sell_stop(__sell_qty, {p})")
                    }
                };

                let has_mods = exit_modifiers.stop_loss.is_some()
                    || exit_modifiers.profit_target.is_some()
                    || exit_modifiers.trailing_stop.is_some();

                // Emit quantity validation guard
                out.push_str(&format!("{indent}let __sell_qty = {qty};\n"));
                out.push_str(&format!("{indent}if __sell_qty > 0 {{\n"));
                if has_mods && kind == CallbackKind::ActionArray {
                    out.push_str(&format!("{indent}    let __order = {call_with_guard};\n"));
                    emit_exit_modifiers(out, exit_modifiers, &format!("{indent}    "));
                    out.push_str(&format!("{indent}    __actions.push(__order);\n"));
                } else if kind == CallbackKind::ActionArray {
                    out.push_str(&format!("{indent}    __actions.push({call_with_guard});\n"));
                } else {
                    out.push_str(&format!("{indent}    {call_with_guard};\n"));
                }
                out.push_str(&format!("{indent}}}\n"));
            }

            Stmt::CancelOrders { signal, .. } => match signal {
                Some(s) => match kind {
                    CallbackKind::ActionArray => {
                        out.push_str(&format!(
                            "{indent}__actions.push(cancel_orders(\"{s}\"));\n"
                        ));
                    }
                    _ => {
                        out.push_str(&format!("{indent}cancel_orders(\"{s}\");\n"));
                    }
                },
                None => match kind {
                    CallbackKind::ActionArray => {
                        out.push_str(&format!("{indent}__actions.push(cancel_orders());\n"));
                    }
                    _ => {
                        out.push_str(&format!("{indent}cancel_orders();\n"));
                    }
                },
            },

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

            Stmt::TryOpen {
                call,
                var_name,
                body,
                ..
            } => {
                let rw = rewrite_expr(call);
                out.push_str(&format!("{indent}let {var_name} = {rw};\n"));
                out.push_str(&format!("{indent}if {var_name} != () {{\n"));
                generate_stmts(out, body, depth + 1, kind, scope_vars);
                if kind == CallbackKind::ActionArray {
                    let inner_indent = "    ".repeat(depth + 1);
                    out.push_str(&format!("{inner_indent}__actions.push({var_name});\n"));
                }
                out.push_str(&format!("{indent}}}\n"));
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

/// Context methods that can be used without parentheses in DSL.
/// Unlike CTX_PROPERTIES (which emit `ctx.X`), these emit `ctx.X()` since
/// they are Rhai functions, not getters.
const CTX_CALLABLE_PROPERTIES: &[&str] = &[
    "day_of_week",
    "month",
    "day_of_month",
    "hour",
    "minute",
    "week_of_year",
    "time",
    "is_first_bar",
    "is_last_bar",
    "is_expiry_week",
    "is_quarter_end",
    "trading_days_left",
    "minutes_since_open",
];

/// Map day names to their numeric value (1=Monday..7=Sunday).
pub fn day_name_to_number(word: &str) -> Option<i64> {
    match word {
        "monday" => Some(1),
        "tuesday" => Some(2),
        "wednesday" => Some(3),
        "thursday" => Some(4),
        "friday" => Some(5),
        "saturday" => Some(6),
        "sunday" => Some(7),
        _ => None,
    }
}

/// Map month names to their numeric value (1=January..12=December).
pub fn month_name_to_number(word: &str) -> Option<i64> {
    match word {
        "january" => Some(1),
        "february" => Some(2),
        "march" => Some(3),
        "april" => Some(4),
        "may" => Some(5),
        "june" => Some(6),
        "july" => Some(7),
        "august" => Some(8),
        "september" => Some(9),
        "october" => Some(10),
        "november" => Some(11),
        "december" => Some(12),
        _ => None,
    }
}

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
    "adjusted_close",
    // Position awareness (next-bar execution model)
    "market_position",
    "entry_price",
    "bars_since_entry",
    "current_shares",
    "open_profit",
    "max_profit",
    "max_loss",
    "pending_orders_count",
];

/// Known `ctx` methods (accessed with parentheses).
const CTX_METHODS: &[&str] = &[
    // OHLCV lookback forms: close(1) → ctx.close(1)
    "close",
    "open",
    "high",
    "low",
    "volume",
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
    // Time/session
    "time",
    "is_first_bar",
    "is_last_bar",
    "is_expiry_week",
    "is_quarter_end",
    "trading_days_left",
    "minutes_since_open",
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

/// Indicators that have `_at(period, bars_ago)` lookback variants.
const INDICATORS_WITH_AT: &[&str] = &["sma", "ema", "rsi"];

/// OHLCV properties that support lookback via `close[N]` → `ctx.close(N)`.
const OHLCV_PROPERTIES: &[&str] = &["close", "open", "high", "low", "volume"];

/// Try to parse a `[N]` lookback suffix starting at position `i`.
/// Returns `Some((N, new_i))` if found, `None` otherwise.
fn try_parse_lookback(chars: &[char], pos: usize) -> Option<(usize, usize)> {
    let mut j = pos;
    // Skip optional whitespace
    while j < chars.len() && chars[j].is_whitespace() {
        j += 1;
    }
    if j >= chars.len() || chars[j] != '[' {
        return None;
    }
    j += 1; // skip '['
            // Parse digits
    let num_start = j;
    while j < chars.len() && chars[j].is_ascii_digit() {
        j += 1;
    }
    if j == num_start || j >= chars.len() || chars[j] != ']' {
        return None;
    }
    let num_str: String = chars[num_start..j].iter().collect();
    let n: usize = num_str.parse().ok()?;
    j += 1; // skip ']'
    Some((n, j))
}

/// Consume balanced parentheses starting at position `pos` (which should point to '(').
/// Returns `(contents_inside_parens, new_i)` where `new_i` is past the closing ')'.
fn consume_parens(chars: &[char], pos: usize) -> Option<(String, usize)> {
    if pos >= chars.len() || chars[pos] != '(' {
        return None;
    }
    let mut j = pos + 1;
    let mut depth = 1;
    let mut contents = String::new();
    while j < chars.len() && depth > 0 {
        if chars[j] == '(' {
            depth += 1;
        } else if chars[j] == ')' {
            depth -= 1;
            if depth == 0 {
                j += 1; // skip closing ')'
                return Some((contents, j));
            }
        }
        contents.push(chars[j]);
        j += 1;
    }
    None // unbalanced
}

/// Check if `[` follows at current position (skipping whitespace).
fn is_followed_by_bracket(chars: &[char], pos: usize) -> bool {
    let mut j = pos;
    while j < chars.len() && chars[j].is_whitespace() {
        j += 1;
    }
    j < chars.len() && chars[j] == '['
}

// ---------------------------------------------------------------------------
// Crossover pre-processing
// ---------------------------------------------------------------------------

/// Extract the trailing operand from a string (the last expression before a keyword).
/// Handles `sma(200)` (walk back to find matching `(`), `close` (walk back through
/// alphanumerics), etc.
fn extract_trailing_expr(s: &str) -> String {
    let s = s.trim_end();
    if s.is_empty() {
        return String::new();
    }
    let chars: Vec<char> = s.chars().collect();
    let end = chars.len();

    // If it ends with ')' — walk back to find matching '('
    if chars[end - 1] == ')' {
        let mut depth = 1;
        let mut j = end - 2;
        loop {
            if chars[j] == ')' {
                depth += 1;
            } else if chars[j] == '(' {
                depth -= 1;
                if depth == 0 {
                    // Now walk back through the function name
                    let paren_start = j;
                    if paren_start > 0 {
                        j -= 1;
                        while j > 0 && (chars[j].is_alphanumeric() || chars[j] == '_') {
                            j -= 1;
                        }
                        if chars[j].is_alphanumeric() || chars[j] == '_' {
                            return chars[j..end].iter().collect();
                        }
                        return chars[j + 1..end].iter().collect();
                    }
                    return chars[paren_start..end].iter().collect();
                }
            }
            if j == 0 {
                break;
            }
            j -= 1;
        }
        return chars[..end].iter().collect();
    }

    // Walk back through alphanumerics/underscores
    let mut j = end - 1;
    while j > 0 && (chars[j].is_alphanumeric() || chars[j] == '_' || chars[j] == '.') {
        j -= 1;
    }
    if chars[j].is_alphanumeric() || chars[j] == '_' {
        chars[j..end].iter().collect()
    } else {
        chars[j + 1..end].iter().collect()
    }
}

/// Extract the leading operand from a string (the first expression after a keyword).
/// Handles `sma(200)`, `close`, `30`, `70.5`.
fn extract_leading_expr(s: &str) -> String {
    let s = s.trim_start();
    if s.is_empty() {
        return String::new();
    }
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;

    // Walk through the identifier part
    while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.') {
        i += 1;
    }

    // If followed by '(', consume balanced parens
    if i < chars.len() && chars[i] == '(' {
        let mut depth = 1;
        i += 1;
        while i < chars.len() && depth > 0 {
            if chars[i] == '(' {
                depth += 1;
            } else if chars[i] == ')' {
                depth -= 1;
            }
            i += 1;
        }
    }

    chars[..i].iter().collect()
}

/// Convert `sma(200)` → `sma:200`, `close` → `close`.
fn to_indicator_spec(expr: &str) -> String {
    let expr = expr.trim();
    if let Some(paren_pos) = expr.find('(') {
        let name = &expr[..paren_pos];
        let rest = &expr[paren_pos + 1..];
        if let Some(close_pos) = rest.find(')') {
            let args = &rest[..close_pos];
            return format!("{name}:{args}");
        }
    }
    expr.to_string()
}

/// Generate the current-bar form: `close` → `ctx.close`, `sma(50)` → `ctx.sma(50)`.
fn make_current_expr(expr: &str) -> String {
    let expr = expr.trim();
    if let Some(paren_pos) = expr.find('(') {
        let name = &expr[..paren_pos];
        let rest = &expr[paren_pos..];
        format!("ctx.{name}{rest}")
    } else {
        format!("ctx.{expr}")
    }
}

/// Generate the lookback form: `close` → `ctx.close(1)`, `sma(50)` → `ctx.sma_at(50, 1)`.
fn make_lookback_expr(expr: &str) -> String {
    let expr = expr.trim();
    if let Some(paren_pos) = expr.find('(') {
        let name = &expr[..paren_pos];
        let rest = &expr[paren_pos + 1..];
        if let Some(close_pos) = rest.find(')') {
            let args = &rest[..close_pos];
            if INDICATORS_WITH_AT.contains(&name) {
                return format!("ctx.{name}_at({args}, 1)");
            }
            return format!("ctx.indicator_at(\"{name}\", {args}, 1)");
        }
    }
    // OHLCV property — close → ctx.close(1)
    format!("ctx.{expr}(1)")
}

/// Returns true if the string looks like a numeric literal (integer or float).
/// Pre-process `concat(arg1, arg2, ...)` into Rhai string concatenation.
/// Each argument is converted to string via `.to_string()` (unless it's already a string literal),
/// then joined with ` + `.
fn preprocess_concat(expr: &str) -> String {
    let mut result = expr.to_string();
    while let Some(start) = result.find("concat(") {
        // Find matching closing paren
        let after = start + "concat(".len();
        let mut depth = 1;
        let mut end = after;
        let bytes = result.as_bytes();
        while end < bytes.len() && depth > 0 {
            if bytes[end] == b'(' {
                depth += 1;
            } else if bytes[end] == b')' {
                depth -= 1;
            }
            end += 1;
        }
        if depth != 0 {
            break; // Unmatched paren, leave as-is
        }
        let args_str = &result[after..end - 1];
        // Split on commas (respecting nested parens)
        let args = split_concat_args(args_str);
        let parts: Vec<String> = args
            .iter()
            .map(|a| {
                let a = a.trim();
                if a.starts_with('"') {
                    a.to_string()
                } else {
                    format!("({a}).to_string()")
                }
            })
            .collect();
        let replacement = parts.join(" + ");
        result = format!("{}{}{}", &result[..start], replacement, &result[end..]);
    }
    result
}

/// Split concat arguments on top-level commas (respecting nested parens).
fn split_concat_args(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut depth = 0;
    for ch in s.chars() {
        if ch == '(' {
            depth += 1;
            current.push(ch);
        } else if ch == ')' {
            depth -= 1;
            current.push(ch);
        } else if ch == ',' && depth == 0 {
            args.push(current.clone());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    if !current.is_empty() {
        args.push(current);
    }
    args
}

fn is_numeric_literal(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() {
        return false;
    }
    // Try parsing as f64
    s.parse::<f64>().is_ok()
}

/// Pre-process `X crosses above Y` and `X crosses below Y` patterns.
///
/// For two indicators/properties (right side is NOT a literal number):
///   `sma(50) crosses above sma(200)` → `crossed_above("sma:50", "sma:200")`
///
/// For indicator vs literal (right side IS a number):
///   `rsi(14) crosses above 30` → `(ctx.rsi(14) > 30 && ctx.rsi_at(14, 1) <= 30)`
fn preprocess_crossovers(expr: &str) -> String {
    let mut result = expr.to_string();

    for keyword in &["crosses above", "crosses below"] {
        // Process all occurrences from left to right
        loop {
            let Some(kw_pos) = result.find(keyword) else {
                break;
            };

            let before = &result[..kw_pos];
            let after = &result[kw_pos + keyword.len()..];

            let lhs = extract_trailing_expr(before);
            let rhs = extract_leading_expr(after);

            if lhs.is_empty() || rhs.is_empty() {
                break;
            }

            // Calculate the exact byte range to replace.
            // before = result[..kw_pos], lhs is at the end of before (possibly with trailing space)
            let before_trimmed = before.trim_end();
            let lhs_start = before_trimmed.len() - lhs.len();

            // Find where rhs ends in 'after'
            let trimmed_after = after.trim_start();
            let leading_spaces = after.len() - trimmed_after.len();
            let rhs_end_in_after = leading_spaces + rhs.len();
            let replace_end = kw_pos + keyword.len() + rhs_end_in_after;

            let is_above = *keyword == "crosses above";

            let replacement = if is_numeric_literal(&rhs) {
                // Literal number case — emit fully qualified ctx expressions
                let curr = make_current_expr(&lhs);
                let lookback = make_lookback_expr(&lhs);
                if is_above {
                    format!("({curr} > {rhs} && {lookback} <= {rhs})")
                } else {
                    format!("({curr} < {rhs} && {lookback} >= {rhs})")
                }
            } else {
                // Two indicators/properties — emit crossed_above/below call
                let lhs_spec = to_indicator_spec(&lhs);
                let rhs_spec = to_indicator_spec(&rhs);
                let fn_name = if is_above {
                    "crossed_above"
                } else {
                    "crossed_below"
                };
                format!("{fn_name}(\"{lhs_spec}\", \"{rhs_spec}\")")
            };

            result = format!(
                "{}{}{}",
                &result[..lhs_start],
                replacement,
                &result[replace_end..]
            );
        }
    }

    result
}

/// Try to match a time literal (`HH:MM`) at position `pos` in `chars`.
/// Returns `Some((quoted_string, end_pos))` on success, `None` otherwise.
pub fn try_parse_time_literal(chars: &[char], pos: usize) -> Option<(String, usize)> {
    // Must start with a digit, not preceded by alphanumeric/underscore
    if !chars[pos].is_ascii_digit() {
        return None;
    }
    if pos > 0
        && (chars[pos - 1].is_alphanumeric() || chars[pos - 1] == '_' || chars[pos - 1] == ':')
    {
        return None;
    }

    // Consume 1-2 digits for hour
    let mut j = pos;
    while j < chars.len() && chars[j].is_ascii_digit() && j - pos < 2 {
        j += 1;
    }

    // Expect ':'
    if j >= chars.len() || chars[j] != ':' {
        return None;
    }
    let colon = j;
    j += 1;

    // Consume exactly 2 digits for minute
    let min_start = j;
    while j < chars.len() && chars[j].is_ascii_digit() && j - min_start < 2 {
        j += 1;
    }
    if j - min_start != 2 {
        return None;
    }

    // Must end at a word boundary: reject continuations like "10:00:00", "10:00pm", "10:00_et"
    if j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '_' || chars[j] == ':') {
        return None;
    }

    let hour: u32 = chars[pos..colon].iter().collect::<String>().parse().ok()?;
    let min: u32 = chars[min_start..j]
        .iter()
        .collect::<String>()
        .parse()
        .ok()?;

    // Validation happens in validate.rs; here we only convert valid-looking patterns.
    // Out-of-range values (25:00) still get quoted so the validator can report them.
    Some((format!("\"{hour:02}:{min:02}\""), j))
}

/// Pre-process time literals (`HH:MM`) into quoted strings (`"HH:MM"`).
///
/// Matches `\d{1,2}:\d{2}` that are NOT inside string literals and NOT preceded
/// by an alphanumeric/underscore character (to avoid matching Rhai map syntax etc.).
fn preprocess_time_literals(expr: &str) -> String {
    let chars: Vec<char> = expr.chars().collect();
    let mut result = String::with_capacity(expr.len() + 16);
    let mut i = 0;

    while i < chars.len() {
        // Skip string literals
        if chars[i] == '"' {
            let end = skip_string_literal(&chars, i);
            for c in &chars[i..end] {
                result.push(*c);
            }
            i = end;
            continue;
        }

        // Try time literal
        if let Some((quoted, end)) = try_parse_time_literal(&chars, i) {
            result.push_str(&quoted);
            i = end;
            continue;
        }

        result.push(chars[i]);
        i += 1;
    }

    result
}

/// Skip past a string literal starting at `pos` (which points to the opening `"`).
/// Returns the position after the closing `"`.
pub fn skip_string_literal(chars: &[char], pos: usize) -> usize {
    let mut i = pos + 1; // skip opening "
    while i < chars.len() && chars[i] != '"' {
        if chars[i] == '\\' && i + 1 < chars.len() {
            i += 2; // skip backslash + escaped char
        } else {
            i += 1;
        }
    }
    if i < chars.len() {
        i + 1 // skip closing "
    } else {
        i
    }
}

/// Pre-process Python-style inline if/else ternary expressions.
///
/// `VALUE if CONDITION else ELSE_VALUE` → `if CONDITION { VALUE } else { ELSE_VALUE }`
///
/// Supports chaining: `A if C1 else B if C2 else C`
/// → `if C1 { A } else if C2 { B } else { C }`
///
/// The `if` keyword must appear as a whole word at the top level (not inside
/// parentheses or string literals) to avoid conflicts with other uses.
fn preprocess_inline_if(expr: &str) -> String {
    // Find the first top-level ` if ` keyword (not inside parens or strings)
    let Some((value_part, rest)) = split_at_top_level_keyword(expr, " if ") else {
        return expr.to_string();
    };

    // Find the matching ` else ` in the rest
    let Some((condition, else_and_tail)) = split_at_top_level_keyword(rest, " else ") else {
        // `if` without `else` — not an inline ternary, leave as-is
        return expr.to_string();
    };

    // Separate the else expression from any trailing content (e.g., `, arg2` in function args)
    let (else_expr, tail) = split_at_top_level_comma(else_and_tail);

    // Recursively process the else branch (handles chaining)
    let else_processed = preprocess_inline_if(else_expr.trim());

    // Check if the else branch is itself an if-expression (chained)
    let else_rhai = if else_processed.starts_with("if ") {
        // Chained: `else if C2 { B } else { C }`
        format!("else {else_processed}")
    } else {
        format!("else {{ {else_processed} }}")
    };

    format!(
        "if {} {{ {} }} {}{}",
        condition.trim(),
        value_part.trim(),
        else_rhai,
        tail
    )
}

/// Split an expression at the first top-level comma.
/// Returns `(before_comma, comma_and_rest)` where `comma_and_rest` includes the comma.
/// If no top-level comma found, returns `(expr, "")`.
fn split_at_top_level_comma(expr: &str) -> (&str, &str) {
    let bytes = expr.as_bytes();
    let mut i = 0;
    let mut paren_depth = 0;
    let mut in_string = false;

    while i < bytes.len() {
        let ch = bytes[i];
        if ch == b'"' && !in_string {
            in_string = true;
        } else if in_string {
            if ch == b'\\' {
                i += 1;
            } else if ch == b'"' {
                in_string = false;
            }
        } else if ch == b'(' {
            paren_depth += 1;
        } else if ch == b')' {
            paren_depth -= 1;
        } else if paren_depth == 0 && ch == b',' {
            return (&expr[..i], &expr[i..]);
        }
        i += 1;
    }
    (expr, "")
}

/// Split an expression at the first top-level occurrence of `keyword`.
/// "Top-level" means not inside parentheses or string literals.
/// Returns `(before, after)` or `None` if keyword not found.
fn split_at_top_level_keyword<'a>(expr: &'a str, keyword: &str) -> Option<(&'a str, &'a str)> {
    let bytes = expr.as_bytes();
    let kw_bytes = keyword.as_bytes();
    let kw_len = kw_bytes.len();

    if bytes.len() < kw_len {
        return None;
    }

    let mut i = 0;
    let mut paren_depth = 0;
    let mut in_string = false;

    while i + kw_len <= bytes.len() {
        let ch = bytes[i];

        // Track string literals
        if ch == b'"' && !in_string {
            in_string = true;
            i += 1;
            continue;
        }
        if in_string {
            if ch == b'\\' {
                i += 2; // skip escape
                continue;
            }
            if ch == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }

        // Track parentheses
        if ch == b'(' {
            paren_depth += 1;
            i += 1;
            continue;
        }
        if ch == b')' {
            paren_depth -= 1;
            i += 1;
            continue;
        }

        // Only match at top level (not inside parens)
        if paren_depth == 0 && &bytes[i..i + kw_len] == kw_bytes {
            return Some((&expr[..i], &expr[i + kw_len..]));
        }

        i += 1;
    }

    None
}

/// Rewrite a DSL expression into a valid Rhai expression.
///
/// - Inline if/else: `V if C else E` → `if C { V } else { E }`
/// - Bare context properties (`close`, `equity`) → `ctx.close`, `ctx.equity`
/// - Bare context methods (`sma(200)`) → `ctx.sma(200)`
/// - Callable properties (`day_of_week`, `time`) → `ctx.day_of_week()`, `ctx.time()`
/// - Day names (`monday`..`sunday`) → numeric values (1..7)
/// - Month names (`january`..`december`) → numeric values (1..12)
/// - Time literals (`10:00`, `15:30`) → quoted strings (`"10:00"`, `"15:30"`)
/// - `has positions` → `ctx.has_positions()`
/// - `no positions` → `!ctx.has_positions()`
/// - `and` → `&&`, `or` → `||`
/// - `X crosses above Y` / `X crosses below Y` → crossover expressions
pub fn rewrite_expr(expr: &str) -> String {
    let expr = preprocess_inline_if(expr);
    let expr = preprocess_crossovers(&expr);
    let expr = preprocess_concat(&expr);
    let expr = preprocess_time_literals(&expr);
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
            } else if let Some(num) = day_name_to_number(&word) {
                result.push_str(&num.to_string());
            } else if let Some(num) = month_name_to_number(&word) {
                result.push_str(&num.to_string());
            } else if CTX_CALLABLE_PROPERTIES.contains(&word.as_str())
                && !is_followed_by_paren(&chars, i)
            {
                // Callable properties without parens: `day_of_week` → `ctx.day_of_week()`
                result.push_str(&format!("ctx.{word}()"));
            } else if word == "has" {
                if let Some(end) = matches_word_at(&chars, i, "positions") {
                    // "has positions" → ctx.has_positions()
                    i = end;
                    result.push_str("ctx.has_positions()");
                } else {
                    result.push_str(&word);
                }
            } else if word == "no" {
                if let Some(end) = matches_word_at(&chars, i, "positions") {
                    // "no positions" → !ctx.has_positions()
                    i = end;
                    result.push_str("!ctx.has_positions()");
                } else {
                    result.push('!');
                }
            } else if word == "and" {
                result.push_str("&&");
            } else if word == "or" {
                result.push_str("||");
            } else if word == "not" {
                result.push('!');
            } else if is_ctx_property(&word) && !is_followed_by_paren(&chars, i) {
                // Check for lookback: close[N] → ctx.close(N), close[0] → ctx.close
                if OHLCV_PROPERTIES.contains(&word.as_str()) {
                    if let Some((n, new_i)) = try_parse_lookback(&chars, i) {
                        if n == 0 {
                            result.push_str("ctx.");
                            result.push_str(&word);
                        } else {
                            result.push_str(&format!("ctx.{word}({n})"));
                        }
                        i = new_i;
                    } else {
                        result.push_str("ctx.");
                        result.push_str(&word);
                    }
                } else {
                    result.push_str("ctx.");
                    result.push_str(&word);
                }
            } else if is_ctx_method(&word) && is_followed_by_paren(&chars, i) {
                // Eagerly consume the parenthesized arguments so we can check for [N]
                let paren_pos = {
                    let mut j = i;
                    while j < chars.len() && chars[j].is_whitespace() {
                        j += 1;
                    }
                    j
                };
                if let Some((args_inner, after_paren)) = consume_parens(&chars, paren_pos) {
                    // Rewrite args (they may contain DSL expressions too)
                    let rewritten_args = rewrite_expr(&args_inner);
                    // Check for lookback [N] after the closing )
                    if is_followed_by_bracket(&chars, after_paren) {
                        if let Some((n, new_i)) = try_parse_lookback(&chars, after_paren) {
                            if n == 0 {
                                // sma(200)[0] → ctx.sma(200)
                                result.push_str(&format!("ctx.{word}({rewritten_args})"));
                            } else if INDICATORS_WITH_AT.contains(&word.as_str()) {
                                // sma(200)[1] → ctx.sma_at(200, 1)
                                result.push_str(&format!("ctx.{word}_at({rewritten_args}, {n})"));
                            } else {
                                // Fallback: indicator_at("name", period, N)
                                result.push_str(&format!(
                                    "ctx.indicator_at(\"{word}\", {rewritten_args}, {n})"
                                ));
                            }
                            i = new_i;
                        } else {
                            // [ but not a valid lookback — emit normally
                            result.push_str(&format!("ctx.{word}({rewritten_args})"));
                            i = after_paren;
                        }
                    } else {
                        // No lookback — emit normally
                        result.push_str(&format!("ctx.{word}({rewritten_args})"));
                        i = after_paren;
                    }
                } else {
                    // Couldn't parse parens, fall through
                    result.push_str("ctx.");
                    result.push_str(&word);
                }
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
/// Check if `expected` word appears at `pos` (after optional whitespace).
/// Returns `Some(end_pos)` if matched, where `end_pos` is the position after the word.
fn matches_word_at(chars: &[char], pos: usize, expected: &str) -> Option<usize> {
    let mut j = pos;
    // Skip whitespace
    while j < chars.len() && chars[j] == ' ' {
        j += 1;
    }
    let expected_chars: Vec<char> = expected.chars().collect();
    if j + expected_chars.len() > chars.len() {
        return None;
    }
    for (k, &ec) in expected_chars.iter().enumerate() {
        if chars[j + k] != ec {
            return None;
        }
    }
    // Ensure it's a full word (not a prefix of a longer identifier)
    let end = j + expected_chars.len();
    if end < chars.len() && (chars[end].is_alphanumeric() || chars[end] == '_') {
        return None;
    }
    Some(end)
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
    fn test_rewrite_concat() {
        assert_eq!(
            rewrite_expr(r#"concat("Cycle ", count + 1)"#),
            r#""Cycle " + (count + 1).to_string()"#
        );
    }

    #[test]
    fn test_rewrite_concat_all_strings() {
        assert_eq!(
            rewrite_expr(r#"concat("hello", " ", "world")"#),
            r#""hello" + " " + "world""#
        );
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

    // -----------------------------------------------------------------------
    // Lookback syntax tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_rewrite_property_lookback() {
        assert_eq!(rewrite_expr("close[1]"), "ctx.close(1)");
        assert_eq!(rewrite_expr("high[3]"), "ctx.high(3)");
        assert_eq!(rewrite_expr("volume[2]"), "ctx.volume(2)");
    }

    #[test]
    fn test_rewrite_property_lookback_zero_optimized() {
        assert_eq!(rewrite_expr("close[0]"), "ctx.close");
        assert_eq!(rewrite_expr("high[0]"), "ctx.high");
    }

    #[test]
    fn test_rewrite_indicator_lookback() {
        assert_eq!(rewrite_expr("sma(200)[1]"), "ctx.sma_at(200, 1)");
        assert_eq!(rewrite_expr("ema(20)[2]"), "ctx.ema_at(20, 2)");
        assert_eq!(rewrite_expr("rsi(14)[1]"), "ctx.rsi_at(14, 1)");
    }

    #[test]
    fn test_rewrite_indicator_lookback_zero_optimized() {
        assert_eq!(rewrite_expr("sma(200)[0]"), "ctx.sma(200)");
    }

    #[test]
    fn test_rewrite_lookback_in_expression() {
        assert_eq!(
            rewrite_expr("close[1] > close[2]"),
            "ctx.close(1) > ctx.close(2)"
        );
    }

    #[test]
    fn test_rewrite_lookback_no_false_positives() {
        assert_eq!(rewrite_expr("pos.legs[0]"), "pos.legs[0]");
        assert_eq!(rewrite_expr("my_array[1]"), "my_array[1]");
    }

    // -----------------------------------------------------------------------
    // Crossover syntax tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_rewrite_crosses_above_two_indicators() {
        assert_eq!(
            rewrite_expr("sma(50) crosses above sma(200)"),
            "ctx.crossed_above(\"sma:50\", \"sma:200\")"
        );
    }

    #[test]
    fn test_rewrite_crosses_below_indicator_and_property() {
        assert_eq!(
            rewrite_expr("close crosses below ema(20)"),
            "ctx.crossed_below(\"close\", \"ema:20\")"
        );
    }

    #[test]
    fn test_rewrite_crosses_above_with_literal() {
        assert_eq!(
            rewrite_expr("rsi(14) crosses above 30"),
            "(ctx.rsi(14) > 30 && ctx.rsi_at(14, 1) <= 30)"
        );
    }

    #[test]
    fn test_rewrite_crosses_below_with_literal() {
        assert_eq!(
            rewrite_expr("rsi(14) crosses below 70"),
            "(ctx.rsi(14) < 70 && ctx.rsi_at(14, 1) >= 70)"
        );
    }

    #[test]
    fn test_rewrite_crosses_above_property_with_literal() {
        assert_eq!(
            rewrite_expr("close crosses above 150.0"),
            "(ctx.close > 150.0 && ctx.close(1) <= 150.0)"
        );
    }

    #[test]
    fn test_rewrite_crosses_in_compound_expression() {
        // crosses combined with other conditions using 'and'
        let result = rewrite_expr("sma(50) crosses above sma(200) and rsi(14) > 50");
        assert!(result.contains("ctx.crossed_above(\"sma:50\", \"sma:200\")"));
        assert!(result.contains("ctx.rsi(14) > 50"));
    }

    // -----------------------------------------------------------------------
    // Time keyword rewrite tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_rewrite_callable_properties_without_parens() {
        assert_eq!(rewrite_expr("day_of_week == 1"), "ctx.day_of_week() == 1");
        assert_eq!(rewrite_expr("month == 12"), "ctx.month() == 12");
        assert_eq!(rewrite_expr("day_of_month > 25"), "ctx.day_of_month() > 25");
        assert_eq!(rewrite_expr("hour > 10"), "ctx.hour() > 10");
        assert_eq!(rewrite_expr("minute == 30"), "ctx.minute() == 30");
        assert_eq!(
            rewrite_expr("week_of_year == 52"),
            "ctx.week_of_year() == 52"
        );
    }

    #[test]
    fn test_rewrite_callable_properties_with_parens() {
        // When used with parens, they go through CTX_METHODS path
        assert_eq!(rewrite_expr("day_of_week()"), "ctx.day_of_week()");
        assert_eq!(rewrite_expr("month()"), "ctx.month()");
        assert_eq!(rewrite_expr("time()"), "ctx.time()");
    }

    #[test]
    fn test_rewrite_time_property() {
        assert_eq!(rewrite_expr("time < \"10:00\""), "ctx.time() < \"10:00\"");
    }

    #[test]
    fn test_rewrite_new_callable_properties() {
        assert_eq!(rewrite_expr("is_first_bar"), "ctx.is_first_bar()");
        assert_eq!(rewrite_expr("is_last_bar"), "ctx.is_last_bar()");
        assert_eq!(rewrite_expr("is_expiry_week"), "ctx.is_expiry_week()");
        assert_eq!(rewrite_expr("is_quarter_end"), "ctx.is_quarter_end()");
        assert_eq!(
            rewrite_expr("trading_days_left < 3"),
            "ctx.trading_days_left() < 3"
        );
        assert_eq!(
            rewrite_expr("minutes_since_open > 30"),
            "ctx.minutes_since_open() > 30"
        );
    }

    #[test]
    fn test_rewrite_day_names() {
        assert_eq!(rewrite_expr("monday"), "1");
        assert_eq!(rewrite_expr("tuesday"), "2");
        assert_eq!(rewrite_expr("wednesday"), "3");
        assert_eq!(rewrite_expr("thursday"), "4");
        assert_eq!(rewrite_expr("friday"), "5");
        assert_eq!(rewrite_expr("saturday"), "6");
        assert_eq!(rewrite_expr("sunday"), "7");
    }

    #[test]
    fn test_rewrite_month_names() {
        assert_eq!(rewrite_expr("january"), "1");
        assert_eq!(rewrite_expr("february"), "2");
        assert_eq!(rewrite_expr("march"), "3");
        assert_eq!(rewrite_expr("april"), "4");
        assert_eq!(rewrite_expr("may"), "5");
        assert_eq!(rewrite_expr("june"), "6");
        assert_eq!(rewrite_expr("july"), "7");
        assert_eq!(rewrite_expr("august"), "8");
        assert_eq!(rewrite_expr("september"), "9");
        assert_eq!(rewrite_expr("october"), "10");
        assert_eq!(rewrite_expr("november"), "11");
        assert_eq!(rewrite_expr("december"), "12");
    }

    #[test]
    fn test_rewrite_day_name_in_expression() {
        assert_eq!(
            rewrite_expr("day_of_week == monday"),
            "ctx.day_of_week() == 1"
        );
        assert_eq!(
            rewrite_expr("day_of_week != friday and month == december"),
            "ctx.day_of_week() != 5 && ctx.month() == 12"
        );
    }

    #[test]
    fn test_rewrite_time_literal() {
        assert_eq!(rewrite_expr("10:00"), "\"10:00\"");
        assert_eq!(rewrite_expr("9:30"), "\"09:30\"");
        assert_eq!(rewrite_expr("15:30"), "\"15:30\"");
        assert_eq!(rewrite_expr("0:00"), "\"00:00\"");
        assert_eq!(rewrite_expr("23:59"), "\"23:59\"");
    }

    #[test]
    fn test_rewrite_time_literal_in_expression() {
        assert_eq!(rewrite_expr("time < 10:00"), "ctx.time() < \"10:00\"");
        assert_eq!(
            rewrite_expr("time > 15:30 and time < 16:00"),
            "ctx.time() > \"15:30\" && ctx.time() < \"16:00\""
        );
    }

    #[test]
    fn test_rewrite_time_literal_not_in_string() {
        // Time literals inside strings should NOT be double-quoted
        assert_eq!(rewrite_expr("\"10:00\""), "\"10:00\"");
    }

    #[test]
    fn test_rewrite_combined_time_and_indicators() {
        assert_eq!(
            rewrite_expr("day_of_week == monday and close > sma(200)"),
            "ctx.day_of_week() == 1 && ctx.close > ctx.sma(200)"
        );
    }

    #[test]
    fn test_rewrite_callable_property_not_dot_qualified() {
        // After a dot, callable properties should NOT be rewritten
        assert_eq!(rewrite_expr("config.time"), "config.time");
    }

    #[test]
    fn test_time_literal_not_after_colon() {
        // Rhai map syntax "field: 10" should NOT match as time literal
        // "sma:200" indicator specs should not match either
        assert_eq!(rewrite_expr("sma:200"), "sma:200");
        // A colon-preceded digit pair should pass through unchanged
        assert_eq!(rewrite_expr("x:10:00"), "x:10:00");
    }

    #[test]
    fn test_skip_string_literal_escaped_quote() {
        // Escaped quote at end should not cause issues
        assert_eq!(
            rewrite_expr(r#"close > 0 and "test\"" == "test\"""#),
            r#"ctx.close > 0 && "test\"" == "test\"""#
        );
    }

    // -----------------------------------------------------------------------
    // Inline if/else ternary tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_inline_if_simple() {
        assert_eq!(
            rewrite_expr("0.15 if vix > 20 else 0.30"),
            "if vix > 20 { 0.15 } else { 0.30 }"
        );
    }

    #[test]
    fn test_inline_if_with_indicators() {
        assert_eq!(
            rewrite_expr("0.15 if rsi(14) > 70 else 0.30"),
            "if ctx.rsi(14) > 70 { 0.15 } else { 0.30 }"
        );
    }

    #[test]
    fn test_inline_if_chained() {
        assert_eq!(
            rewrite_expr("0.15 if rsi(14) > 70 else 0.30 if rsi(14) < 30 else 0.20"),
            "if ctx.rsi(14) > 70 { 0.15 } else if ctx.rsi(14) < 30 { 0.30 } else { 0.20 }"
        );
    }

    #[test]
    fn test_inline_if_with_ctx_properties() {
        assert_eq!(
            rewrite_expr("close * 1.05 if close > sma(200) else close * 0.95"),
            "if ctx.close > ctx.sma(200) { ctx.close * 1.05 } else { ctx.close * 0.95 }"
        );
    }

    #[test]
    fn test_inline_if_no_else_passes_through() {
        // `if` without `else` is not an inline ternary — leave as-is
        // (it will be handled by the `when...then` parser)
        assert_eq!(rewrite_expr("close > 100"), "ctx.close > 100");
    }

    #[test]
    fn test_inline_if_inside_function_args() {
        // Inline if/else inside function args should work, bounded by comma
        assert_eq!(
            rewrite_expr("size_by_risk(0.02 if atr(14) > 1 else 0.01, close)"),
            "ctx.size_by_risk(if ctx.atr(14) > 1 { 0.02 } else { 0.01 }, ctx.close)"
        );
    }

    #[test]
    fn test_inline_if_with_boolean_operators() {
        assert_eq!(
            rewrite_expr("0.15 if close > sma(200) and rsi(14) > 50 else 0.30"),
            "if ctx.close > ctx.sma(200) && ctx.rsi(14) > 50 { 0.15 } else { 0.30 }"
        );
    }
}

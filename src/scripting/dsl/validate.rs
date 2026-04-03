//! Compile-time validation for DSL programs.
//!
//! Checks for semantic errors that the parser accepts structurally but are
//! invalid given the strategy configuration (e.g., using intraday-only keywords
//! with a daily interval).

use super::codegen::{day_name_to_number, month_name_to_number};
use super::error::DslError;
use super::parser::{DslProgram, Stmt};

/// Keywords that are only meaningful for intraday intervals.
/// Using these with a daily interval is a compile error.
const INTRADAY_ONLY_KEYWORDS: &[&str] =
    &["time", "is_first_bar", "is_last_bar", "minutes_since_open"];

/// Intervals that are NOT intraday. Only includes intervals actually
/// supported by `Interval::parse()` to avoid false acceptance.
fn is_non_intraday(interval: &str) -> bool {
    matches!(interval, "daily" | "1d")
}

/// Check that intraday-only time keywords are not used with non-intraday intervals,
/// that all time literals are valid (hour 0-23, minute 0-59), and that reserved
/// day/month names are not used as variable names.
pub fn check_interval_time_keywords(program: &DslProgram) -> Result<(), DslError> {
    let interval = program
        .strategy
        .as_ref()
        .map(|s| s.interval.as_str())
        .unwrap_or("daily");

    let check_intraday = is_non_intraday(interval);

    // Scan all statement blocks
    let blocks: Vec<&Option<Vec<Stmt>>> = vec![
        &program.on_bar,
        &program.on_exit_check,
        &program.on_position_opened,
        &program.on_position_closed,
        &program.on_end,
    ];

    for block in blocks.into_iter().flatten() {
        check_stmts(block, interval, check_intraday)?;
    }

    // Also check procedural body
    check_stmts(&program.body, interval, check_intraday)?;

    // Check that reserved day/month names are not used as extern/state variable names
    check_reserved_names(program)?;

    Ok(())
}

/// Check if a name collides with a reserved day/month keyword.
fn is_reserved_name(name: &str) -> bool {
    let lower = name.to_lowercase();
    day_name_to_number(&lower).is_some() || month_name_to_number(&lower).is_some()
}

/// Reject variable names that collide with reserved day/month keywords.
/// Checks extern, state, set, for-each, and try-open-as identifiers.
fn check_reserved_names(program: &DslProgram) -> Result<(), DslError> {
    for p in &program.params {
        if is_reserved_name(&p.name) {
            return Err(DslError::general(format!(
                "extern `{}` conflicts with reserved day/month name. \
                 Choose a different variable name.",
                p.name
            )));
        }
    }
    for s in &program.states {
        if is_reserved_name(&s.name) {
            return Err(DslError::general(format!(
                "state `{}` conflicts with reserved day/month name. \
                 Choose a different variable name.",
                s.name
            )));
        }
    }

    // Check all statement blocks for identifier-introducing statements
    let blocks: Vec<&Option<Vec<Stmt>>> = vec![
        &program.on_bar,
        &program.on_exit_check,
        &program.on_position_opened,
        &program.on_position_closed,
        &program.on_end,
    ];
    for block in blocks.into_iter().flatten() {
        check_reserved_names_in_stmts(block)?;
    }
    check_reserved_names_in_stmts(&program.body)?;

    Ok(())
}

/// Recursively check statements for reserved day/month names used as identifiers.
fn check_reserved_names_in_stmts(stmts: &[Stmt]) -> Result<(), DslError> {
    for stmt in stmts {
        match stmt {
            Stmt::Set { name, line, .. } => {
                if is_reserved_name(name) {
                    return Err(DslError::new(
                        *line,
                        format!(
                            "variable `{name}` conflicts with reserved day/month name. \
                             Choose a different variable name."
                        ),
                    ));
                }
            }
            Stmt::ForEach {
                var, body, line, ..
            } => {
                if is_reserved_name(var) {
                    return Err(DslError::new(
                        *line,
                        format!(
                            "loop variable `{var}` conflicts with reserved day/month name. \
                             Choose a different variable name."
                        ),
                    ));
                }
                check_reserved_names_in_stmts(body)?;
            }
            Stmt::TryOpen {
                var_name,
                body,
                line,
                ..
            } => {
                if is_reserved_name(var_name) {
                    return Err(DslError::new(
                        *line,
                        format!(
                            "variable `{var_name}` conflicts with reserved day/month name. \
                             Choose a different variable name."
                        ),
                    ));
                }
                check_reserved_names_in_stmts(body)?;
            }
            Stmt::When {
                then_body,
                else_body,
                ..
            } => {
                check_reserved_names_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_reserved_names_in_stmts(eb)?;
                }
            }
            Stmt::WhenAnyAll {
                binding_var,
                capture_as,
                then_body,
                else_body,
                line,
                ..
            } => {
                if is_reserved_name(binding_var) {
                    return Err(DslError::new(
                        *line,
                        format!(
                            "variable `{binding_var}` conflicts with reserved day/month name. \
                             Choose a different variable name."
                        ),
                    ));
                }
                if let Some(ref cap) = capture_as {
                    if is_reserved_name(cap) {
                        return Err(DslError::new(
                            *line,
                            format!(
                                "variable `{cap}` conflicts with reserved day/month name. \
                                 Choose a different variable name."
                            ),
                        ));
                    }
                }
                check_reserved_names_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_reserved_names_in_stmts(eb)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Recursively scan statements for intraday-only keywords and invalid time literals.
fn check_stmts(stmts: &[Stmt], interval: &str, check_intraday: bool) -> Result<(), DslError> {
    for stmt in stmts {
        match stmt {
            Stmt::SkipWhen { condition, line } => {
                check_expr(condition, *line, interval, check_intraday)?;
            }
            Stmt::Set { expr, line, .. } => {
                check_expr(expr, *line, interval, check_intraday)?;
            }
            Stmt::When {
                condition,
                then_body,
                else_body,
                line,
            } => {
                check_expr(condition, *line, interval, check_intraday)?;
                check_stmts(then_body, interval, check_intraday)?;
                if let Some(ref eb) = else_body {
                    check_stmts(eb, interval, check_intraday)?;
                }
            }
            Stmt::ForEach {
                iterable,
                body,
                line,
                ..
            } => {
                check_expr(iterable, *line, interval, check_intraday)?;
                check_stmts(body, interval, check_intraday)?;
            }
            Stmt::TryOpen { body, .. } => {
                check_stmts(body, interval, check_intraday)?;
            }
            Stmt::Return { expr, line } => {
                check_expr(expr, *line, interval, check_intraday)?;
            }
            Stmt::Raw { code, line } => {
                check_expr(code, *line, interval, check_intraday)?;
            }
            Stmt::WhenAnyAll {
                condition,
                then_body,
                else_body,
                line,
                ..
            } => {
                check_expr(condition, *line, interval, check_intraday)?;
                check_stmts(then_body, interval, check_intraday)?;
                if let Some(ref eb) = else_body {
                    check_stmts(eb, interval, check_intraday)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Check a single expression for intraday-only keywords and invalid time literals.
fn check_expr(
    expr: &str,
    line: usize,
    interval: &str,
    check_intraday: bool,
) -> Result<(), DslError> {
    // Always validate time literals
    check_time_literals(expr, line)?;

    // Only check intraday keywords if interval is non-intraday
    // Strip string literals first to avoid false positives on e.g. `note == "time"`
    if check_intraday {
        let stripped = strip_string_literals(expr);
        for keyword in INTRADAY_ONLY_KEYWORDS {
            if contains_whole_word(&stripped, keyword) {
                let hint = match *keyword {
                    "time" => "Use `day_of_week`, `month`, or `day_of_month` instead.",
                    "is_first_bar" | "is_last_bar" => {
                        "These are only meaningful for intraday intervals (1m, 5m, 15m, etc.)."
                    }
                    "minutes_since_open" => {
                        "This is only meaningful for intraday intervals (1m, 5m, 15m, etc.)."
                    }
                    _ => "",
                };
                return Err(DslError::new(
                    line,
                    format!(
                        "`{keyword}` is only available for intraday intervals (1m, 5m, 15m, etc.). \
                         Your strategy uses `interval {interval}`. {hint}"
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Validate that all time literals (HH:MM patterns) in an expression have valid
/// hour (0-23) and minute (0-59) values.
///
/// Reuses `try_parse_time_literal` from codegen for pattern matching, then
/// checks the range of the parsed values.
fn check_time_literals(expr: &str, line: usize) -> Result<(), DslError> {
    use super::codegen::{skip_string_literal, try_parse_time_literal};

    let chars: Vec<char> = expr.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '"' {
            i = skip_string_literal(&chars, i);
            continue;
        }

        if let Some((quoted, end)) = try_parse_time_literal(&chars, i) {
            // Extract HH:MM from the quoted string `"HH:MM"`
            let inner = &quoted[1..quoted.len() - 1]; // strip quotes
            if let Some((hour_str, min_str)) = inner.split_once(':') {
                let hour: u32 = hour_str.parse().unwrap_or(99);
                let min: u32 = min_str.parse().unwrap_or(99);
                if hour > 23 || min > 59 {
                    return Err(DslError::new(
                        line,
                        format!(
                            "invalid time literal `{inner}`. \
                             Hour must be 0-23 and minute must be 0-59 (24-hour format)."
                        ),
                    ));
                }
            }
            i = end;
            continue;
        }

        i += 1;
    }
    Ok(())
}

/// Remove the contents of string literals from an expression, replacing them with spaces.
/// `"note == \"time\""` → `"note ==         "` so keyword scanning doesn't false-positive.
fn strip_string_literals(expr: &str) -> String {
    use super::codegen::skip_string_literal;
    let chars: Vec<char> = expr.chars().collect();
    let mut result = String::with_capacity(expr.len());
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '"' {
            let end = skip_string_literal(&chars, i);
            // Replace the entire string literal (including quotes) with spaces
            for _ in i..end {
                result.push(' ');
            }
            i = end;
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }
    result
}

/// Check if `expr` contains `word` as a whole word (not part of a larger identifier).
fn contains_whole_word(expr: &str, word: &str) -> bool {
    let bytes = expr.as_bytes();
    let word_len = word.len();

    if word_len == 0 || bytes.len() < word_len {
        return false;
    }

    for i in 0..=bytes.len() - word_len {
        if &bytes[i..i + word_len] == word.as_bytes() {
            // Check word boundary before
            let before_ok =
                i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
            // Check word boundary after
            let after_pos = i + word_len;
            let after_ok = after_pos >= bytes.len()
                || !(bytes[after_pos].is_ascii_alphanumeric() || bytes[after_pos] == b'_');
            if before_ok && after_ok {
                return true;
            }
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Portfolio namespace validation
// ---------------------------------------------------------------------------

/// Valid portfolio namespace properties.
const PORTFOLIO_PROPERTIES: &[&str] = &[
    "cash",
    "equity",
    "unrealized_pnl",
    "realized_pnl",
    "total_exposure",
    "exposure_pct",
    "net_delta",
    "long_delta",
    "short_delta",
    "position_count",
    "long_count",
    "short_count",
    "max_position_pnl",
    "min_position_pnl",
    "drawdown",
    "peak_equity",
];

/// Check for invalid `portfolio.X` property accesses and `set portfolio.X` assignments.
pub fn check_portfolio_access(program: &DslProgram) -> Result<(), DslError> {
    let blocks: Vec<&Option<Vec<Stmt>>> = vec![
        &program.on_bar,
        &program.on_exit_check,
        &program.on_position_opened,
        &program.on_position_closed,
        &program.on_end,
    ];

    for block in blocks.into_iter().flatten() {
        check_portfolio_in_stmts(block)?;
    }
    check_portfolio_in_stmts(&program.body)?;

    Ok(())
}

fn check_portfolio_in_stmts(stmts: &[Stmt]) -> Result<(), DslError> {
    for stmt in stmts {
        match stmt {
            Stmt::Set { name, expr, line } => {
                if name.starts_with("portfolio.") {
                    return Err(DslError::new(
                        *line,
                        "cannot assign to `portfolio` — it is read-only",
                    ));
                }
                check_portfolio_expr(expr, *line)?;
            }
            Stmt::SkipWhen { condition, line } => {
                check_portfolio_expr(condition, *line)?;
            }
            Stmt::When {
                condition,
                then_body,
                else_body,
                line,
            } => {
                check_portfolio_expr(condition, *line)?;
                check_portfolio_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_portfolio_in_stmts(eb)?;
                }
            }
            Stmt::ForEach {
                iterable,
                body,
                line,
                ..
            } => {
                check_portfolio_expr(iterable, *line)?;
                check_portfolio_in_stmts(body)?;
            }
            Stmt::TryOpen {
                call, body, line, ..
            } => {
                check_portfolio_expr(call, *line)?;
                check_portfolio_in_stmts(body)?;
            }
            Stmt::WhenAnyAll {
                condition,
                then_body,
                else_body,
                line,
                ..
            } => {
                check_portfolio_expr(condition, *line)?;
                check_portfolio_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_portfolio_in_stmts(eb)?;
                }
            }
            Stmt::Buy { qty_expr, line, .. } | Stmt::Sell { qty_expr, line, .. } => {
                check_portfolio_expr(qty_expr, *line)?;
            }
            Stmt::Plot { expr, line, .. } | Stmt::Return { expr, line, .. } => {
                check_portfolio_expr(expr, *line)?;
            }
            Stmt::OpenStrategy { call, line, .. } => {
                check_portfolio_expr(call, *line)?;
            }
            Stmt::ClosePositionById { id_expr, line, .. } => {
                check_portfolio_expr(id_expr, *line)?;
            }
            Stmt::AddTo { expr, line, .. }
            | Stmt::SubtractFrom { expr, line, .. }
            | Stmt::MultiplyBy { expr, line, .. }
            | Stmt::DivideBy { expr, line, .. } => {
                check_portfolio_expr(expr, *line)?;
            }
            Stmt::Raw { code, line } => {
                check_portfolio_expr(code, *line)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn check_portfolio_expr(expr: &str, line: usize) -> Result<(), DslError> {
    let stripped = strip_string_literals(expr);
    let prefix = "portfolio.";
    let mut search = stripped.as_str();
    while let Some(pos) = search.find(prefix) {
        let after = &search[pos + prefix.len()..];
        let prop_end = after
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(after.len());
        let prop = &after[..prop_end];

        if !prop.is_empty() && !PORTFOLIO_PROPERTIES.contains(&prop) {
            return Err(DslError::new(
                line,
                format!(
                    "unknown portfolio property `{prop}`. Valid properties: {}",
                    PORTFOLIO_PROPERTIES.join(", ")
                ),
            ));
        }
        search = &after[prop_end..];
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Quantifier validation
// ---------------------------------------------------------------------------

/// Valid leg field names for quantifier conditions.
const LEG_FIELDS: &[&str] = &[
    "delta",
    "strike",
    "current_price",
    "entry_price",
    "option_type",
    "side",
    "qty",
    "expiration",
];

/// Numeric-only leg fields (valid for sum/min/max/avg).
const NUMERIC_LEG_FIELDS: &[&str] = &["delta", "strike", "current_price", "entry_price", "qty"];

/// Check that quantifier statements (`when any/all`) are only used where `pos` is
/// in scope: inside `on_exit_check` (implicit) or nested inside `for each pos in positions`.
/// Also validates leg field references.
pub fn check_quantifiers(program: &DslProgram) -> Result<(), DslError> {
    // In on_exit_check, quantifiers are allowed at any nesting level (pos is implicit)
    if let Some(ref stmts) = program.on_exit_check {
        check_quantifier_fields_in_stmts(stmts)?;
    }

    // In other blocks, quantifiers are only allowed inside `for each pos in positions`
    let other_blocks: Vec<(&str, &Option<Vec<Stmt>>)> = vec![
        ("on each bar", &program.on_bar),
        ("on position opened", &program.on_position_opened),
        ("on position closed", &program.on_position_closed),
        ("on end", &program.on_end),
    ];

    for (block_name, block) in other_blocks {
        if let Some(ref stmts) = block {
            check_no_quantifiers_outside_pos_scope(stmts, block_name, false)?;
        }
    }
    check_no_quantifiers_outside_pos_scope(&program.body, "procedural body", false)?;

    // Check aggregation methods in all blocks for field validity
    check_aggregation_fields(program)?;

    Ok(())
}

/// Recursively check that quantifiers only appear inside a `for each pos in positions` scope.
fn check_no_quantifiers_outside_pos_scope(
    stmts: &[Stmt],
    block_name: &str,
    in_pos_scope: bool,
) -> Result<(), DslError> {
    for stmt in stmts {
        match stmt {
            Stmt::WhenAnyAll {
                line,
                then_body,
                else_body,
                ..
            } if !in_pos_scope => {
                return Err(DslError::new(
                    *line,
                    format!(
                        "quantifiers (`when any/all`) require `pos` to be in scope. \
                         Found in `{block_name}` outside a `for each pos in positions` block. \
                         Either move to `on exit check` or wrap in `for each pos in positions`."
                    ),
                ));
            }
            Stmt::WhenAnyAll {
                then_body,
                else_body,
                ..
            } => {
                // in_pos_scope is true here, validate fields
                check_quantifier_fields_in_stmts(std::slice::from_ref(stmt))?;
                check_no_quantifiers_outside_pos_scope(then_body, block_name, true)?;
                if let Some(ref eb) = else_body {
                    check_no_quantifiers_outside_pos_scope(eb, block_name, true)?;
                }
            }
            Stmt::When {
                then_body,
                else_body,
                ..
            } => {
                check_no_quantifiers_outside_pos_scope(then_body, block_name, in_pos_scope)?;
                if let Some(ref eb) = else_body {
                    check_no_quantifiers_outside_pos_scope(eb, block_name, in_pos_scope)?;
                }
            }
            Stmt::ForEach {
                var,
                iterable,
                body,
                ..
            } => {
                let enters_pos_scope = var == "pos" && iterable == "positions";
                check_no_quantifiers_outside_pos_scope(
                    body,
                    block_name,
                    in_pos_scope || enters_pos_scope,
                )?;
            }
            Stmt::TryOpen { body, .. } => {
                check_no_quantifiers_outside_pos_scope(body, block_name, in_pos_scope)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn check_quantifier_fields_in_stmts(stmts: &[Stmt]) -> Result<(), DslError> {
    for stmt in stmts {
        match stmt {
            Stmt::WhenAnyAll {
                condition,
                line,
                then_body,
                else_body,
                ..
            } => {
                check_condition_fields(condition, *line)?;
                check_quantifier_fields_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_quantifier_fields_in_stmts(eb)?;
                }
            }
            Stmt::When {
                then_body,
                else_body,
                ..
            } => {
                check_quantifier_fields_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_quantifier_fields_in_stmts(eb)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Check that the condition in a quantifier only references known leg fields.
fn check_condition_fields(condition: &str, line: usize) -> Result<(), DslError> {
    let chars: Vec<char> = condition.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '"' {
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    i += 1;
                }
                i += 1;
            }
            if i < chars.len() {
                i += 1;
            }
            continue;
        }
        if chars[i].is_alphabetic() || chars[i] == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let preceded_by_dot = start > 0 && chars[start - 1] == '.';
            // Skip logical operators and known non-field tokens
            if !preceded_by_dot
                && !["and", "or", "not", "true", "false"].contains(&word.as_str())
                && word.parse::<f64>().is_err()
                && !LEG_FIELDS.contains(&word.as_str())
            {
                return Err(DslError::new(
                    line,
                    format!(
                        "unknown leg field `{word}` in quantifier condition. \
                         Valid fields: {}",
                        LEG_FIELDS.join(", ")
                    ),
                ));
            }
        } else {
            i += 1;
        }
    }
    Ok(())
}

/// Check aggregation expressions like pos.legs.sum(FIELD) for valid numeric fields.
fn check_aggregation_fields(program: &DslProgram) -> Result<(), DslError> {
    let blocks: Vec<&Option<Vec<Stmt>>> = vec![
        &program.on_bar,
        &program.on_exit_check,
        &program.on_position_opened,
        &program.on_position_closed,
        &program.on_end,
    ];
    for block in blocks.into_iter().flatten() {
        check_aggregation_in_stmts(block)?;
    }
    check_aggregation_in_stmts(&program.body)?;
    Ok(())
}

fn check_aggregation_in_stmts(stmts: &[Stmt]) -> Result<(), DslError> {
    for stmt in stmts {
        match stmt {
            Stmt::SkipWhen { condition, line } => {
                check_aggregation_in_expr(condition, *line)?;
            }
            Stmt::Set { expr, line, .. } => {
                check_aggregation_in_expr(expr, *line)?;
            }
            Stmt::When {
                condition,
                then_body,
                else_body,
                line,
            } => {
                check_aggregation_in_expr(condition, *line)?;
                check_aggregation_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_aggregation_in_stmts(eb)?;
                }
            }
            Stmt::WhenAnyAll {
                condition,
                then_body,
                else_body,
                line,
                ..
            } => {
                check_aggregation_in_expr(condition, *line)?;
                check_aggregation_in_stmts(then_body)?;
                if let Some(ref eb) = else_body {
                    check_aggregation_in_stmts(eb)?;
                }
            }
            Stmt::ForEach { body, .. } => {
                check_aggregation_in_stmts(body)?;
            }
            Stmt::TryOpen {
                call, body, line, ..
            } => {
                check_aggregation_in_expr(call, *line)?;
                check_aggregation_in_stmts(body)?;
            }
            Stmt::Buy { qty_expr, line, .. } | Stmt::Sell { qty_expr, line, .. } => {
                check_aggregation_in_expr(qty_expr, *line)?;
            }
            Stmt::Plot { expr, line, .. } | Stmt::Return { expr, line, .. } => {
                check_aggregation_in_expr(expr, *line)?;
            }
            Stmt::OpenStrategy { call, line, .. } => {
                check_aggregation_in_expr(call, *line)?;
            }
            Stmt::AddTo { expr, line, .. }
            | Stmt::SubtractFrom { expr, line, .. }
            | Stmt::MultiplyBy { expr, line, .. }
            | Stmt::DivideBy { expr, line, .. } => {
                check_aggregation_in_expr(expr, *line)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Check a single expression for aggregation methods using non-numeric fields.
/// Loops over ALL occurrences, not just the first.
fn check_aggregation_in_expr(expr: &str, line: usize) -> Result<(), DslError> {
    for method in &["sum", "min", "max", "avg"] {
        let pattern = format!(".{method}(");
        let mut search_from = 0;
        while let Some(rel_pos) = expr[search_from..].find(&pattern) {
            let pos = search_from + rel_pos;
            let after = &expr[pos + pattern.len()..];
            if let Some(end) = after.find(')') {
                let field = after[..end].trim();
                if !NUMERIC_LEG_FIELDS.contains(&field) {
                    return Err(DslError::new(
                        line,
                        format!(
                            "`{method}()` requires a numeric field, got `{field}`. \
                             Valid numeric fields: {}",
                            NUMERIC_LEG_FIELDS.join(", ")
                        ),
                    ));
                }
            }
            search_from = pos + pattern.len();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contains_whole_word() {
        assert!(contains_whole_word("time < 10:00", "time"));
        assert!(contains_whole_word(
            "is_first_bar and close > 100",
            "is_first_bar"
        ));
        assert!(!contains_whole_word("runtime_error", "time"));
        assert!(!contains_whole_word("my_time_var", "time"));
        assert!(!contains_whole_word("timeout", "time"));
    }

    #[test]
    fn test_valid_time_literals() {
        assert!(check_time_literals("time < 10:00", 1).is_ok());
        assert!(check_time_literals("time > 15:30", 1).is_ok());
        assert!(check_time_literals("time == 9:30", 1).is_ok());
        assert!(check_time_literals("time < 0:00", 1).is_ok());
        assert!(check_time_literals("time < 23:59", 1).is_ok());
    }

    #[test]
    fn test_invalid_time_hour() {
        let err = check_time_literals("time < 25:00", 1).unwrap_err();
        assert!(err.message.contains("25:00"), "Got: {}", err.message);
        assert!(err.message.contains("0-23"), "Got: {}", err.message);
    }

    #[test]
    fn test_invalid_time_minute() {
        let err = check_time_literals("time < 10:60", 1).unwrap_err();
        assert!(err.message.contains("10:60"), "Got: {}", err.message);
        assert!(err.message.contains("0-59"), "Got: {}", err.message);
    }

    #[test]
    fn test_invalid_time_both() {
        let err = check_time_literals("time < 99:99", 1).unwrap_err();
        assert!(err.message.contains("99:99"), "Got: {}", err.message);
    }

    #[test]
    fn test_time_literal_inside_string_not_checked() {
        // Time literals inside strings should be ignored
        assert!(check_time_literals(r#""meeting at 25:99""#, 1).is_ok());
    }

    #[test]
    fn test_no_false_positive_on_non_time_colons() {
        // Rhai-style expressions with colons shouldn't trigger
        assert!(check_time_literals("sma(200)", 1).is_ok());
        assert!(check_time_literals("close > 100", 1).is_ok());
    }
}

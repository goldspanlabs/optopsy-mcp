//! Compile-time validation for DSL programs.
//!
//! Checks for semantic errors that the parser accepts structurally but are
//! invalid given the strategy configuration (e.g., using intraday-only keywords
//! with a daily interval).

use super::error::DslError;
use super::parser::{DslProgram, Stmt};

/// Keywords that are only meaningful for intraday intervals.
/// Using these with `daily`, `weekly`, or `monthly` intervals is a compile error.
const INTRADAY_ONLY_KEYWORDS: &[&str] =
    &["time", "is_first_bar", "is_last_bar", "minutes_since_open"];

/// Intervals that are NOT intraday.
fn is_non_intraday(interval: &str) -> bool {
    matches!(interval, "daily" | "weekly" | "monthly")
}

/// Check that intraday-only time keywords are not used with non-intraday intervals,
/// and that all time literals are valid (hour 0-23, minute 0-59).
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
    if check_intraday {
        for keyword in INTRADAY_ONLY_KEYWORDS {
            if contains_whole_word(expr, keyword) {
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
fn check_time_literals(expr: &str, line: usize) -> Result<(), DslError> {
    let bytes = expr.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        // Skip string literals
        if bytes[i] == b'"' {
            i += 1;
            while i < bytes.len() && bytes[i] != b'"' {
                if bytes[i] == b'\\' {
                    i += 1;
                }
                i += 1;
            }
            i += 1; // skip closing quote
            continue;
        }

        // Look for digit that could start a time literal
        if bytes[i].is_ascii_digit() {
            // Must not be preceded by alphanumeric/underscore
            let preceded_by_alnum =
                i > 0 && (bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');
            if !preceded_by_alnum {
                let start = i;
                // Consume 1-2 digits
                while i < bytes.len() && bytes[i].is_ascii_digit() && i - start < 2 {
                    i += 1;
                }
                let hour_len = i - start;
                // Check for colon followed by exactly 2 digits
                if hour_len >= 1 && i < bytes.len() && bytes[i] == b':' {
                    let colon_pos = i;
                    i += 1;
                    let min_start = i;
                    while i < bytes.len() && bytes[i].is_ascii_digit() && i - min_start < 2 {
                        i += 1;
                    }
                    let min_len = i - min_start;
                    let followed_by_more =
                        i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b':');
                    if min_len == 2 && !followed_by_more {
                        // This is a time literal — validate it
                        let hour_str = std::str::from_utf8(&bytes[start..colon_pos]).unwrap_or("0");
                        let min_str =
                            std::str::from_utf8(&bytes[min_start..min_start + 2]).unwrap_or("0");
                        let hour: u32 = hour_str.parse().unwrap_or(99);
                        let min: u32 = min_str.parse().unwrap_or(99);
                        if hour > 23 || min > 59 {
                            return Err(DslError::new(
                                line,
                                format!(
                                    "invalid time literal `{hour_str}:{min_str}`. \
                                     Hour must be 0-23 and minute must be 0-59 (24-hour format)."
                                ),
                            ));
                        }
                        continue;
                    }
                }
            }
        }
        i += 1;
    }
    Ok(())
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

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

/// Check that intraday-only time keywords are not used with non-intraday intervals.
pub fn check_interval_time_keywords(program: &DslProgram) -> Result<(), DslError> {
    let Some(ref strategy) = program.strategy else {
        return Ok(());
    };

    if !is_non_intraday(&strategy.interval) {
        return Ok(()); // Intraday interval — all keywords allowed
    }

    // Scan all statement blocks for intraday-only keywords
    let blocks: Vec<&Option<Vec<Stmt>>> = vec![
        &program.on_bar,
        &program.on_exit_check,
        &program.on_position_opened,
        &program.on_position_closed,
        &program.on_end,
    ];

    for block in blocks.into_iter().flatten() {
        check_stmts(block, &strategy.interval)?;
    }

    // Also check procedural body
    check_stmts(&program.body, &strategy.interval)?;

    Ok(())
}

/// Recursively scan statements for intraday-only keywords in expressions.
fn check_stmts(stmts: &[Stmt], interval: &str) -> Result<(), DslError> {
    for stmt in stmts {
        match stmt {
            Stmt::SkipWhen { condition, line } => {
                check_expr(condition, *line, interval)?;
            }
            Stmt::Set { expr, line, .. } => {
                check_expr(expr, *line, interval)?;
            }
            Stmt::When {
                condition,
                then_body,
                else_body,
                line,
            } => {
                check_expr(condition, *line, interval)?;
                check_stmts(then_body, interval)?;
                if let Some(ref eb) = else_body {
                    check_stmts(eb, interval)?;
                }
            }
            Stmt::ForEach {
                iterable,
                body,
                line,
                ..
            } => {
                check_expr(iterable, *line, interval)?;
                check_stmts(body, interval)?;
            }
            Stmt::TryOpen { body, .. } => {
                check_stmts(body, interval)?;
            }
            Stmt::Return { expr, line } => {
                check_expr(expr, *line, interval)?;
            }
            Stmt::Raw { code, line } => {
                check_expr(code, *line, interval)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Check a single expression string for intraday-only keywords.
fn check_expr(expr: &str, line: usize, interval: &str) -> Result<(), DslError> {
    // Scan for whole-word matches of intraday-only keywords
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
}

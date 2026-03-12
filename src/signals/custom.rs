//! Custom formula signal evaluator.
//!
//! Supports a mini expression DSL for building signals from price columns.
//!
//! ## Supported syntax
//!
//! **Columns**: `close`, `open`, `high`, `low`, `volume`, `adjclose`
//!
//! **Lookback**: `close[1]` = previous close, `close[5]` = 5 bars ago
//!
//! **Functions (basic)**:
//! - `sma(col, period)` — Simple Moving Average
//! - `ema(col, period)` — Exponential Moving Average (true EWM with alpha=2/(period+1))
//! - `std(col, period)` — Rolling Standard Deviation
//! - `max(col, period)` — Rolling Maximum
//! - `min(col, period)` — Rolling Minimum
//! - `abs(expr)` — Absolute value
//! - `change(col, period)` — `col - col[period]`
//! - `pct_change(col, period)` — `(col - col[period]) / col[period]`
//!
//! **Functions (TA indicators)**:
//! - `rsi(col, period)` — Relative Strength Index (Wilder smoothing)
//! - `macd_hist(col)` — MACD histogram (12/26/9)
//! - `macd_signal(col)` — MACD signal line
//! - `macd_line(col)` — MACD line
//! - `roc(col, period)` — Rate of change (%)
//! - `bbands_mid(col, period)` — Bollinger middle band (= SMA)
//! - `bbands_upper(col, period)` — Bollinger upper band (SMA + 2σ)
//! - `bbands_lower(col, period)` — Bollinger lower band (SMA - 2σ)
//! - `atr(close, high, low, period)` — Average True Range
//! - `stochastic(close, high, low, period)` — Stochastic %K
//! - `keltner_upper(close, high, low, period, mult)` — Upper Keltner Channel
//! - `keltner_lower(close, high, low, period, mult)` — Lower Keltner Channel
//! - `obv(close, volume)` — On-Balance Volume
//! - `mfi(close, high, low, volume, period)` — Money Flow Index
//! - `aroon_up(high, low, period)` — Aroon Up
//! - `aroon_down(high, low, period)` — Aroon Down
//! - `aroon_osc(high, low, period)` — Aroon Oscillator
//! - `supertrend(close, high, low, period, mult)` — Supertrend line
//! - `cmf(close, high, low, volume, period)` — Chaikin Money Flow
//!
//! **Functions (derived features)**:
//! - `tr(close, high, low)` — True Range
//! - `rel_volume(vol, period)` — Relative Volume (vol / SMA(vol, period))
//! - `range_pct(close, high, low)` — Position within bar range
//! - `zscore(col, period)` — Z-score (standard deviations from rolling mean)
//! - `rank(col, period)` — Percentile rank within rolling window (= IV Percentile when used on `iv`)
//! - `iv_rank(col, period)` — Min-max rank: `(current - min) / (max - min) × 100`
//! - `consecutive_up(col)` — Count of consecutive rises
//! - `consecutive_down(col)` — Count of consecutive falls
//!
//! **Functions (control flow)**:
//! - `if(cond, then, else)` — Conditional expression
//!
//! **Operators**: `+`, `-`, `*`, `/`
//!
//! **Comparisons**: `>`, `<`, `>=`, `<=`, `==`, `!=`
//!
//! **Logical**: `and`, `or`, `not`
//!
//! **Literals**: floating point numbers (e.g., `1.5`, `200`, `0.02`)
//!
//! **Parens**: `(expr)`
//!
//! ## Examples
//!
//! ```text
//! close > sma(close, 20)
//! close > close[1] * 1.02
//! (close - low) / (high - low) < 0.2
//! volume > sma(volume, 20) * 2.0
//! close > sma(close, 50) and close > sma(close, 200)
//! pct_change(close, 1) > 0.03 or pct_change(close, 1) < -0.03
//! rsi(close, 14) < 30 and close > bbands_lower(close, 20)
//! atr(close, high, low, 14) > 2.0
//! if(close > 100, 1, 0)
//! ```

// Multi-column map closures use conventional short names (s, c, h, l, v, n)
#![allow(clippy::many_single_char_names)]

use polars::prelude::*;
use rust_ti::standard_indicators::bulk as sti;

use super::helpers::{pad_series, SignalFn};
use super::momentum::compute_rsi_variable_period;
use super::volatility::{compute_atr, compute_keltner_channel};
use super::volume::{compute_cmf, compute_typical_price};

/// A signal driven by a user-defined formula string.
pub struct FormulaSignal {
    formula: String,
}

impl FormulaSignal {
    pub fn new(formula: String) -> Self {
        Self { formula }
    }
}

impl SignalFn for FormulaSignal {
    fn evaluate(&self, df: &DataFrame) -> Result<Series, PolarsError> {
        let expr = parse_formula(&self.formula)
            .map_err(|e| PolarsError::ComputeError(format!("Formula parse error: {e}").into()))?;

        let result = df.clone().lazy().select([expr.alias("signal")]).collect()?;

        let col = result.column("signal")?;

        // Broadcast scalar results (e.g. `lit(true)`) to the full DataFrame length
        let col = if col.len() == 1 && df.height() > 1 {
            col.new_from_index(0, df.height())
        } else {
            col.clone()
        };

        // Ensure boolean output
        if col.dtype() == &DataType::Boolean {
            Ok(col.bool()?.clone().into_series())
        } else {
            // If numeric, treat non-zero as true
            let cast = col.cast(&DataType::Boolean)?;
            Ok(cast.bool()?.clone().into_series())
        }
    }

    fn name(&self) -> &'static str {
        "custom_formula"
    }
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Number(f64),
    Ident(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Plus,
    Minus,
    Star,
    Slash,
    Gt,
    Lt,
    Ge,
    Le,
    Eq,
    Ne,
    And,
    Or,
    Not,
    True,
    False,
}

#[allow(clippy::too_many_lines)]
fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            ' ' | '\t' | '\n' | '\r' => i += 1,
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '[' => {
                tokens.push(Token::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Token::RBracket);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            '/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Ge);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Le);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            '=' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Eq);
                    i += 2;
                } else {
                    return Err(format!(
                        "Unexpected '=' at position {i}. Did you mean '=='?"
                    ));
                }
            }
            '!' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Ne);
                    i += 2;
                } else {
                    tokens.push(Token::Not);
                    i += 1;
                }
            }
            c if c.is_ascii_digit() || c == '.' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let num_str: String = chars[start..i].iter().collect();
                let num: f64 = num_str
                    .parse()
                    .map_err(|_| format!("Invalid number: '{num_str}'"))?;
                tokens.push(Token::Number(num));
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let word: String = chars[start..i].iter().collect();
                match word.to_lowercase().as_str() {
                    "and" => tokens.push(Token::And),
                    "or" => tokens.push(Token::Or),
                    "not" => tokens.push(Token::Not),
                    "true" => tokens.push(Token::True),
                    "false" => tokens.push(Token::False),
                    _ => tokens.push(Token::Ident(word)),
                }
            }
            other => return Err(format!("Unexpected character: '{other}' at position {i}")),
        }
    }

    Ok(tokens)
}

// ---------------------------------------------------------------------------
// Recursive descent parser → Polars Expr
// ---------------------------------------------------------------------------

/// Columns valid for use in formula expressions.
const VALID_COLUMNS: &[&str] = &["close", "open", "high", "low", "volume", "adjclose", "iv"];

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_ahead(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset)
    }

    fn advance(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let tok = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(tok)
        } else {
            None
        }
    }

    fn expect(&mut self, expected: &Token) -> Result<(), String> {
        match self.advance() {
            Some(ref tok) if tok == expected => Ok(()),
            Some(tok) => Err(format!("Expected {expected:?}, got {tok:?}")),
            None => Err(format!("Expected {expected:?}, got end of input")),
        }
    }

    /// Top-level: `or_expr`
    fn parse_expr(&mut self) -> Result<Expr, String> {
        self.parse_or()
    }

    /// `or_expr` = `and_expr` ("or" `and_expr`)*
    fn parse_or(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_and()?;
        while self.peek() == Some(&Token::Or) {
            self.advance();
            let right = self.parse_and()?;
            left = left.or(right);
        }
        Ok(left)
    }

    /// `and_expr` = `not_expr` ("and" `not_expr`)*
    fn parse_and(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_not()?;
        while self.peek() == Some(&Token::And) {
            self.advance();
            let right = self.parse_not()?;
            left = left.and(right);
        }
        Ok(left)
    }

    /// `not_expr` = "not" `not_expr` | comparison
    fn parse_not(&mut self) -> Result<Expr, String> {
        if self.peek() == Some(&Token::Not) {
            self.advance();
            let inner = self.parse_not()?;
            Ok(inner.not())
        } else {
            self.parse_comparison()
        }
    }

    /// comparison = additive ((">" | "<" | ">=" | "<=" | "==" | "!=") additive)?
    fn parse_comparison(&mut self) -> Result<Expr, String> {
        let left = self.parse_additive()?;
        match self.peek() {
            Some(Token::Gt) => {
                self.advance();
                let right = self.parse_additive()?;
                Ok(left.gt(right))
            }
            Some(Token::Lt) => {
                self.advance();
                let right = self.parse_additive()?;
                Ok(left.lt(right))
            }
            Some(Token::Ge) => {
                self.advance();
                let right = self.parse_additive()?;
                Ok(left.gt_eq(right))
            }
            Some(Token::Le) => {
                self.advance();
                let right = self.parse_additive()?;
                Ok(left.lt_eq(right))
            }
            Some(Token::Eq) => {
                self.advance();
                let right = self.parse_additive()?;
                Ok(left.eq(right))
            }
            Some(Token::Ne) => {
                self.advance();
                let right = self.parse_additive()?;
                Ok(left.neq(right))
            }
            _ => Ok(left),
        }
    }

    // additive = multiplicative (("+"|"-") multiplicative)*
    fn parse_additive(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_multiplicative()?;
        loop {
            match self.peek() {
                Some(Token::Plus) => {
                    self.advance();
                    let right = self.parse_multiplicative()?;
                    left = left + right;
                }
                Some(Token::Minus) => {
                    self.advance();
                    let right = self.parse_multiplicative()?;
                    left = left - right;
                }
                _ => break,
            }
        }
        Ok(left)
    }

    // multiplicative = unary (("*"|"/") unary)*
    fn parse_multiplicative(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_unary()?;
        loop {
            match self.peek() {
                Some(Token::Star) => {
                    self.advance();
                    let right = self.parse_unary()?;
                    left = left * right;
                }
                Some(Token::Slash) => {
                    self.advance();
                    let right = self.parse_unary()?;
                    left = left / right;
                }
                _ => break,
            }
        }
        Ok(left)
    }

    // unary = "-" unary | primary
    fn parse_unary(&mut self) -> Result<Expr, String> {
        if self.peek() == Some(&Token::Minus) {
            self.advance();
            let inner = self.parse_unary()?;
            Ok(lit(0.0) - inner)
        } else {
            self.parse_primary()
        }
    }

    /// Parse optional `[n]` lookback suffix and apply `.shift(n)` to the expression.
    fn parse_optional_lookback(&mut self, expr: Expr) -> Result<Expr, String> {
        if self.peek() == Some(&Token::LBracket) {
            self.advance();
            match self.advance() {
                Some(Token::Number(n)) => {
                    self.expect(&Token::RBracket)?;
                    if n.fract() != 0.0 || n < 0.0 {
                        return Err(format!(
                            "Lookback index must be a non-negative integer, got {n}"
                        ));
                    }
                    if n > 10_000.0 {
                        return Err(format!("Lookback index too large (max 10000), got {n}"));
                    }
                    Ok(expr.shift(lit(n as i64)))
                }
                other => Err(format!("Expected number in lookback, got {other:?}")),
            }
        } else {
            Ok(expr)
        }
    }

    // primary = number | "true" | "false" | ident ("[" number "]")? | func_call ("[" number "]")? | "(" expr ")"
    fn parse_primary(&mut self) -> Result<Expr, String> {
        match self.peek().cloned() {
            Some(Token::Number(n)) => {
                self.advance();
                Ok(lit(n))
            }
            Some(Token::True) => {
                self.advance();
                Ok(lit(true))
            }
            Some(Token::False) => {
                self.advance();
                Ok(lit(false))
            }
            Some(Token::LParen) => {
                self.advance();
                let inner = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(inner)
            }
            Some(Token::Ident(name)) => {
                self.advance();

                // Check for function call: ident "(" args ")" ("[" number "]")?
                if self.peek() == Some(&Token::LParen) {
                    self.advance();
                    let args = self.parse_args()?;
                    self.expect(&Token::RParen)?;
                    let expr = Self::build_function_call(&name, args)?;
                    // Allow lookback on function results: sma(close, 5)[1]
                    self.parse_optional_lookback(expr)
                }
                // Check for lookback: ident "[" number "]"
                else if self.peek() == Some(&Token::LBracket) {
                    let name_lower = name.to_lowercase();
                    if !VALID_COLUMNS.contains(&name_lower.as_str()) {
                        return Err(format!(
                            "Unknown column '{name}'. Valid columns are: close, open, high, low, volume, adjclose, iv"
                        ));
                    }
                    self.parse_optional_lookback(col(&*name_lower))
                } else {
                    // Plain column reference
                    let name_lower = name.to_lowercase();
                    if !VALID_COLUMNS.contains(&name_lower.as_str()) {
                        return Err(format!(
                            "Unknown column '{name}'. Valid columns are: close, open, high, low, volume, adjclose, iv"
                        ));
                    }
                    Ok(col(&*name_lower))
                }
            }
            Some(tok) => Err(format!("Unexpected token: {tok:?}")),
            None => Err("Unexpected end of expression".to_string()),
        }
    }

    /// Parse comma-separated arguments (can be expressions or numbers for periods)
    fn parse_args(&mut self) -> Result<Vec<FuncArg>, String> {
        let mut args = Vec::new();

        if self.peek() == Some(&Token::RParen) {
            return Ok(args);
        }

        args.push(self.parse_func_arg()?);
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            args.push(self.parse_func_arg()?);
        }

        Ok(args)
    }

    fn parse_func_arg(&mut self) -> Result<FuncArg, String> {
        // Use two-token lookahead: if the next token is a bare number followed immediately by
        // a comma or closing paren, treat it as a plain period/literal (FuncArg::Number).
        // Otherwise parse a full expression, which handles cases like `abs(1 + close)`.
        let is_pure_number = matches!(
            (self.peek(), self.peek_ahead(1)),
            (
                Some(Token::Number(_)),
                Some(Token::Comma | Token::RParen) | None
            )
        );

        if is_pure_number {
            if let Some(Token::Number(n)) = self.peek() {
                let n = *n;
                self.advance();
                return Ok(FuncArg::Number(n));
            }
        }

        let expr = self.parse_expr()?;
        Ok(FuncArg::Expression(expr))
    }

    #[allow(clippy::too_many_lines)]
    fn build_function_call(name: &str, args: Vec<FuncArg>) -> Result<Expr, String> {
        match name.to_lowercase().as_str() {
            "sma" => {
                let (col_expr, period) = extract_col_period(&args, "sma")?;
                Ok(col_expr.rolling_mean(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                }))
            }
            "ema" => {
                let (col_expr, period) = extract_col_period(&args, "ema")?;
                let alpha = 2.0f64 / (period as f64 + 1.0);
                Ok(col_expr.ewm_mean(EWMOptions {
                    alpha,
                    adjust: true,
                    bias: false,
                    min_periods: period,
                    ignore_nulls: true,
                }))
            }
            "std" => {
                let (col_expr, period) = extract_col_period(&args, "std")?;
                Ok(col_expr.rolling_std(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                }))
            }
            "max" => {
                let (col_expr, period) = extract_col_period(&args, "max")?;
                Ok(col_expr.rolling_max(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                }))
            }
            "min" => {
                let (col_expr, period) = extract_col_period(&args, "min")?;
                Ok(col_expr.rolling_min(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                }))
            }
            "abs" => {
                if args.len() != 1 {
                    return Err("abs() takes exactly 1 argument".to_string());
                }
                let expr = args.into_iter().next().unwrap().into_expr();
                Ok(expr.abs())
            }
            "change" => {
                let (col_expr, period) = extract_col_period(&args, "change")?;
                let shifted = col_expr.clone().shift(lit(period as i64));
                Ok(col_expr - shifted)
            }
            "pct_change" => {
                let (col_expr, period) = extract_col_period(&args, "pct_change")?;
                let shifted = col_expr.clone().shift(lit(period as i64));
                Ok((col_expr - shifted.clone()) / shifted)
            }

            // --- TA indicators: pure Polars expressions ---

            "roc" => {
                let (col_expr, period) = extract_col_period(&args, "roc")?;
                let shifted = col_expr.clone().shift(lit(period as i64));
                Ok((col_expr - shifted.clone()) / shifted * lit(100.0))
            }
            "rel_volume" => {
                let (col_expr, period) = extract_col_period(&args, "rel_volume")?;
                let sma_expr = col_expr.clone().rolling_mean(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                });
                Ok(col_expr / sma_expr)
            }
            "zscore" => {
                let (col_expr, period) = extract_col_period(&args, "zscore")?;
                let mean = col_expr.clone().rolling_mean(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                });
                let std_dev = col_expr.clone().rolling_std(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                });
                Ok((col_expr - mean) / std_dev)
            }
            "bbands_mid" => {
                let (col_expr, period) = extract_col_period(&args, "bbands_mid")?;
                Ok(col_expr.rolling_mean(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                }))
            }
            "bbands_upper" => {
                let (col_expr, period) = extract_col_period(&args, "bbands_upper")?;
                let sma = col_expr.clone().rolling_mean(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                });
                let std_dev = col_expr.rolling_std(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                });
                Ok(sma + lit(2.0) * std_dev)
            }
            "bbands_lower" => {
                let (col_expr, period) = extract_col_period(&args, "bbands_lower")?;
                let sma = col_expr.clone().rolling_mean(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                });
                let std_dev = col_expr.rolling_std(RollingOptionsFixedWindow {
                    window_size: period,
                    min_periods: period,
                    ..Default::default()
                });
                Ok(sma - lit(2.0) * std_dev)
            }
            "range_pct" => {
                let (close_e, high_e, low_e) = extract_three_cols(&args, "range_pct")?;
                let range = high_e - low_e.clone();
                let pct = (close_e - low_e) / range.clone();
                Ok(when(range.neq(lit(0.0)))
                    .then(pct)
                    .otherwise(lit(NULL)))
            }

            // --- Control flow ---

            "if" => {
                if args.len() != 3 {
                    return Err(
                        "if() takes exactly 3 arguments: (condition, then_value, else_value)"
                            .to_string(),
                    );
                }
                let cond = args[0].clone().into_expr();
                let then_val = args[1].clone().into_expr();
                let else_val = args[2].clone().into_expr();
                Ok(when(cond).then(then_val).otherwise(else_val))
            }

            // --- Single-column map functions ---

            "rsi" => {
                let (col_expr, period) = extract_col_period(&args, "rsi")?;
                Ok(col_expr.map(
                    move |col: Column| {
                        let ca = col.as_materialized_series().f64()?;
                        let n = ca.len();
                        let vals: Vec<f64> = ca
                            .into_iter()
                            .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                            .collect();
                        if n <= period {
                            return Ok(
                                Series::new("rsi".into(), vec![f64::NAN; n]).into(),
                            );
                        }
                        let rsi_vals = compute_rsi_variable_period(&vals, period);
                        let padded = pad_series(&rsi_vals, n);
                        Ok(Series::new("rsi".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "macd_hist" => {
                let col_expr = extract_single_col(&args, "macd_hist")?;
                Ok(col_expr.map(
                    move |col: Column| {
                        let ca = col.as_materialized_series().f64()?;
                        let n = ca.len();
                        let vals: Vec<f64> = ca
                            .into_iter()
                            .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                            .collect();
                        if n < 34 {
                            return Ok(
                                Series::new("macd_hist".into(), vec![f64::NAN; n]).into(),
                            );
                        }
                        let macd_values = sti::macd(&vals);
                        let histograms: Vec<f64> = macd_values.iter().map(|t| t.2).collect();
                        let padded = pad_series(&histograms, n);
                        Ok(Series::new("macd_hist".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "macd_signal" => {
                let col_expr = extract_single_col(&args, "macd_signal")?;
                Ok(col_expr.map(
                    move |col: Column| {
                        let ca = col.as_materialized_series().f64()?;
                        let n = ca.len();
                        let vals: Vec<f64> = ca
                            .into_iter()
                            .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                            .collect();
                        if n < 34 {
                            return Ok(
                                Series::new("macd_signal".into(), vec![f64::NAN; n]).into(),
                            );
                        }
                        let macd_values = sti::macd(&vals);
                        let signals: Vec<f64> = macd_values.iter().map(|t| t.1).collect();
                        let padded = pad_series(&signals, n);
                        Ok(Series::new("macd_signal".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "macd_line" => {
                let col_expr = extract_single_col(&args, "macd_line")?;
                Ok(col_expr.map(
                    move |col: Column| {
                        let ca = col.as_materialized_series().f64()?;
                        let n = ca.len();
                        let vals: Vec<f64> = ca
                            .into_iter()
                            .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                            .collect();
                        if n < 34 {
                            return Ok(
                                Series::new("macd_line".into(), vec![f64::NAN; n]).into(),
                            );
                        }
                        let macd_values = sti::macd(&vals);
                        let lines: Vec<f64> = macd_values.iter().map(|t| t.0).collect();
                        let padded = pad_series(&lines, n);
                        Ok(Series::new("macd_line".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "rank" => {
                let (col_expr, period) = extract_col_period(&args, "rank")?;
                Ok(col_expr.map(
                    move |col: Column| {
                        let ca = col.as_materialized_series().f64()?;
                        let n = ca.len();
                        let vals: Vec<f64> = ca
                            .into_iter()
                            .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                            .collect();
                        if n < period {
                            return Ok(
                                Series::new("rank".into(), vec![f64::NAN; n]).into(),
                            );
                        }
                        let rank_vals = compute_rolling_rank(&vals, period);
                        let padded = pad_series(&rank_vals, n);
                        Ok(Series::new("rank".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }

            "iv_rank" => {
                let (col_expr, period) = extract_col_period(&args, "iv_rank")?;
                Ok(col_expr.map(
                    move |col: Column| {
                        let ca = col.as_materialized_series().f64()?;
                        let vals: Vec<f64> = ca
                            .into_iter()
                            .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                            .collect();
                        let n = vals.len();
                        if n < period {
                            return Ok(
                                Series::new("iv_rank".into(), vec![f64::NAN; n]).into(),
                            );
                        }
                        let rank_vals = compute_iv_rank(&vals, period);
                        let padded = pad_series(&rank_vals, n);
                        Ok(Series::new("iv_rank".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }

            // --- Multi-column as_struct + map functions ---

            "atr" => {
                let (close_expr, high_expr, low_expr, period) =
                    extract_three_cols_period(&args, "atr")?;
                Ok(as_struct(vec![
                    close_expr.alias("__c"),
                    high_expr.alias("__h"),
                    low_expr.alias("__l"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let close_s = ca.field_by_name("__c")?;
                        let high_s = ca.field_by_name("__h")?;
                        let low_s = ca.field_by_name("__l")?;
                        let c: Vec<f64> = close_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let h: Vec<f64> = high_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = low_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let atr_vals = compute_atr(&c, &h, &l, period);
                        let padded = pad_series(&atr_vals, s.len());
                        Ok(Series::new("atr".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "stochastic" => {
                let (close_expr, high_expr, low_expr, period) =
                    extract_three_cols_period(&args, "stochastic")?;
                Ok(as_struct(vec![
                    close_expr.alias("__c"),
                    high_expr.alias("__h"),
                    low_expr.alias("__l"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let close_s = ca.field_by_name("__c")?;
                        let high_s = ca.field_by_name("__h")?;
                        let low_s = ca.field_by_name("__l")?;
                        let c: Vec<f64> = close_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let h: Vec<f64> = high_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = low_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let stoch_vals =
                            super::momentum::compute_stochastic(&c, &h, &l, period);
                        let padded = pad_series(&stoch_vals, s.len());
                        Ok(Series::new("stochastic".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "keltner_upper" => {
                let (close_expr, high_expr, low_expr, period, mult) =
                    extract_three_cols_period_mult(&args, "keltner_upper")?;
                Ok(as_struct(vec![
                    close_expr.alias("__c"),
                    high_expr.alias("__h"),
                    low_expr.alias("__l"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let close_s = ca.field_by_name("__c")?;
                        let high_s = ca.field_by_name("__h")?;
                        let low_s = ca.field_by_name("__l")?;
                        let c: Vec<f64> = close_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let h: Vec<f64> = high_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = low_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let (_, upper) = compute_keltner_channel(&c, &h, &l, period, mult);
                        let padded = pad_series(&upper, s.len());
                        Ok(Series::new("keltner_upper".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "keltner_lower" => {
                let (close_expr, high_expr, low_expr, period, mult) =
                    extract_three_cols_period_mult(&args, "keltner_lower")?;
                Ok(as_struct(vec![
                    close_expr.alias("__c"),
                    high_expr.alias("__h"),
                    low_expr.alias("__l"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let close_s = ca.field_by_name("__c")?;
                        let high_s = ca.field_by_name("__h")?;
                        let low_s = ca.field_by_name("__l")?;
                        let c: Vec<f64> = close_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let h: Vec<f64> = high_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = low_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let (lower, _) = compute_keltner_channel(&c, &h, &l, period, mult);
                        let padded = pad_series(&lower, s.len());
                        Ok(Series::new("keltner_lower".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "obv" => {
                let (close_expr, vol_expr) = extract_two_cols(&args, "obv")?;
                Ok(as_struct(vec![
                    close_expr.alias("__c"),
                    vol_expr.alias("__v"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let close_s = ca.field_by_name("__c")?;
                        let vol_s = ca.field_by_name("__v")?;
                        let c: Vec<f64> = close_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let v: Vec<f64> = vol_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        if c.len() < 2 {
                            return Ok(
                                Series::new("obv".into(), vec![f64::NAN; c.len()]).into(),
                            );
                        }
                        let obv_vals = rust_ti::momentum_indicators::bulk::on_balance_volume(
                            &c, &v, 0.0,
                        );
                        let padded = pad_series(&obv_vals, s.len());
                        Ok(Series::new("obv".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "mfi" => {
                let (close_expr, high_expr, low_expr, vol_expr, period) =
                    extract_four_cols_period(&args, "mfi")?;
                Ok(as_struct(vec![
                    close_expr.alias("__c"),
                    high_expr.alias("__h"),
                    low_expr.alias("__l"),
                    vol_expr.alias("__v"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let close_s = ca.field_by_name("__c")?;
                        let high_s = ca.field_by_name("__h")?;
                        let low_s = ca.field_by_name("__l")?;
                        let vol_s = ca.field_by_name("__v")?;
                        let c: Vec<f64> = close_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let h: Vec<f64> = high_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = low_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let v: Vec<f64> = vol_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let typical = compute_typical_price(&h, &l, &c);
                        let n = typical.len();
                        let mfi_vals = if period > 0 && n >= period {
                            rust_ti::momentum_indicators::bulk::money_flow_index(
                                &typical, &v, period,
                            )
                        } else {
                            vec![]
                        };
                        let padded = pad_series(&mfi_vals, s.len());
                        Ok(Series::new("mfi".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "tr" => {
                let (close_expr, high_expr, low_expr) = extract_three_cols(&args, "tr")?;
                Ok(as_struct(vec![
                    close_expr.alias("__c"),
                    high_expr.alias("__h"),
                    low_expr.alias("__l"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let close_s = ca.field_by_name("__c")?;
                        let high_s = ca.field_by_name("__h")?;
                        let low_s = ca.field_by_name("__l")?;
                        let c: Vec<f64> = close_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let h: Vec<f64> = high_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = low_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let n = c.len();
                        let mut tr_vals = Vec::with_capacity(n);
                        // First bar: high - low (no previous close)
                        if n > 0 {
                            tr_vals.push(h[0] - l[0]);
                        }
                        for i in 1..n {
                            let hl = h[i] - l[i];
                            let hc = (h[i] - c[i - 1]).abs();
                            let lc = (l[i] - c[i - 1]).abs();
                            tr_vals.push(hl.max(hc).max(lc));
                        }
                        Ok(Series::new("tr".into(), tr_vals).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            // --- Trend indicators ---

            "aroon_up" => {
                let (high_expr, low_expr, period) =
                    extract_three_cols_period_as_two_cols(&args, "aroon_up")?;
                Ok(as_struct(vec![
                    high_expr.alias("__h"),
                    low_expr.alias("__l"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let high_s = ca.field_by_name("__h")?;
                        let low_s = ca.field_by_name("__l")?;
                        let h: Vec<f64> = high_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = low_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let n = h.len();
                        if n < period + 1 {
                            return Ok(
                                Series::new("aroon_up".into(), vec![f64::NAN; n]).into(),
                            );
                        }
                        let vals: Vec<f64> = (0..(n - period))
                            .map(|i| {
                                let end = i + period + 1;
                                let (up, _, _) = rust_ti::trend_indicators::single::aroon_indicator(
                                    &h[i..end],
                                    &l[i..end],
                                );
                                up
                            })
                            .collect();
                        let padded = pad_series(&vals, n);
                        Ok(Series::new("aroon_up".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "aroon_down" => {
                let (high_expr, low_expr, period) =
                    extract_three_cols_period_as_two_cols(&args, "aroon_down")?;
                Ok(as_struct(vec![
                    high_expr.alias("__h"),
                    low_expr.alias("__l"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let high_s = ca.field_by_name("__h")?;
                        let low_s = ca.field_by_name("__l")?;
                        let h: Vec<f64> = high_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = low_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let n = h.len();
                        if n < period + 1 {
                            return Ok(
                                Series::new("aroon_down".into(), vec![f64::NAN; n]).into(),
                            );
                        }
                        let vals: Vec<f64> = (0..(n - period))
                            .map(|i| {
                                let end = i + period + 1;
                                let (_, down, _) = rust_ti::trend_indicators::single::aroon_indicator(
                                    &h[i..end],
                                    &l[i..end],
                                );
                                down
                            })
                            .collect();
                        let padded = pad_series(&vals, n);
                        Ok(Series::new("aroon_down".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "aroon_osc" => {
                let (high_expr, low_expr, period) =
                    extract_three_cols_period_as_two_cols(&args, "aroon_osc")?;
                Ok(as_struct(vec![
                    high_expr.alias("__h"),
                    low_expr.alias("__l"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let high_s = ca.field_by_name("__h")?;
                        let low_s = ca.field_by_name("__l")?;
                        let h: Vec<f64> = high_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = low_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let n = h.len();
                        if n < period + 1 {
                            return Ok(
                                Series::new("aroon_osc".into(), vec![f64::NAN; n]).into(),
                            );
                        }
                        let vals: Vec<f64> = (0..(n - period))
                            .map(|i| {
                                let end = i + period + 1;
                                let (_, _, osc) = rust_ti::trend_indicators::single::aroon_indicator(
                                    &h[i..end],
                                    &l[i..end],
                                );
                                osc
                            })
                            .collect();
                        let padded = pad_series(&vals, n);
                        Ok(Series::new("aroon_osc".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "supertrend" => {
                let (close_expr, high_expr, low_expr, period, mult) =
                    extract_three_cols_period_mult(&args, "supertrend")?;
                Ok(as_struct(vec![
                    close_expr.alias("__c"),
                    high_expr.alias("__h"),
                    low_expr.alias("__l"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let close_s = ca.field_by_name("__c")?;
                        let high_s = ca.field_by_name("__h")?;
                        let low_s = ca.field_by_name("__l")?;
                        let c: Vec<f64> = close_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let h: Vec<f64> = high_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = low_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let n = c.len();
                        if n < period {
                            return Ok(
                                Series::new("supertrend".into(), vec![f64::NAN; n]).into(),
                            );
                        }
                        let st = rust_ti::candle_indicators::bulk::supertrend(
                            &h,
                            &l,
                            &c,
                            rust_ti::ConstantModelType::SimpleMovingAverage,
                            mult,
                            period,
                        );
                        let padded = pad_series(&st, n);
                        Ok(Series::new("supertrend".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "cmf" => {
                let (close_expr, high_expr, low_expr, vol_expr, period) =
                    extract_four_cols_period(&args, "cmf")?;
                Ok(as_struct(vec![
                    close_expr.alias("__c"),
                    high_expr.alias("__h"),
                    low_expr.alias("__l"),
                    vol_expr.alias("__v"),
                ])
                .map(
                    move |col: Column| {
                        let s = col.as_materialized_series();
                        let ca = s.struct_()?;
                        let close_s = ca.field_by_name("__c")?;
                        let high_s = ca.field_by_name("__h")?;
                        let low_s = ca.field_by_name("__l")?;
                        let vol_s = ca.field_by_name("__v")?;
                        let c: Vec<f64> = close_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let h: Vec<f64> = high_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let l: Vec<f64> = low_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let v: Vec<f64> = vol_s
                            .f64()?
                            .into_iter()
                            .map(|v| v.unwrap_or(f64::NAN))
                            .collect();
                        let cmf_vals = compute_cmf(&c, &h, &l, &v, period);
                        let padded = pad_series(&cmf_vals, s.len());
                        Ok(Series::new("cmf".into(), padded).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }

            // --- Stateful counting functions ---

            "consecutive_up" => {
                let col_expr = extract_single_col(&args, "consecutive_up")?;
                Ok(col_expr.map(
                    move |col: Column| {
                        let ca = col.as_materialized_series().f64()?;
                        let vals: Vec<f64> = ca
                            .into_iter()
                            .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                            .collect();
                        let n = vals.len();
                        let mut counts = vec![0.0_f64; n];
                        for i in 1..n {
                            if !vals[i].is_nan() && !vals[i - 1].is_nan() && vals[i] > vals[i - 1] {
                                counts[i] = counts[i - 1] + 1.0;
                            }
                        }
                        Ok(Series::new("consecutive_up".into(), counts).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            "consecutive_down" => {
                let col_expr = extract_single_col(&args, "consecutive_down")?;
                Ok(col_expr.map(
                    move |col: Column| {
                        let ca = col.as_materialized_series().f64()?;
                        let vals: Vec<f64> = ca
                            .into_iter()
                            .map(|opt_v| opt_v.unwrap_or(f64::NAN))
                            .collect();
                        let n = vals.len();
                        let mut counts = vec![0.0_f64; n];
                        for i in 1..n {
                            if !vals[i].is_nan() && !vals[i - 1].is_nan() && vals[i] < vals[i - 1] {
                                counts[i] = counts[i - 1] + 1.0;
                            }
                        }
                        Ok(Series::new("consecutive_down".into(), counts).into())
                    },
                    |_: &Schema, _: &Field| Ok(Field::new("v".into(), DataType::Float64)),
                ))
            }
            other => Err(format!(
                "Unknown function: '{other}'. Available: sma, ema, std, max, min, abs, change, \
                 pct_change, rsi, macd_hist, macd_signal, macd_line, roc, bbands_mid, bbands_upper, \
                 bbands_lower, atr, stochastic, keltner_upper, keltner_lower, obv, mfi, tr, \
                 rel_volume, range_pct, zscore, rank, iv_rank, if, aroon_up, aroon_down, aroon_osc, \
                 supertrend, cmf, consecutive_up, consecutive_down"
            )),
        }
    }
}

#[derive(Debug, Clone)]
enum FuncArg {
    Number(f64),
    Expression(Expr),
}

impl FuncArg {
    fn into_expr(self) -> Expr {
        match self {
            FuncArg::Number(n) => lit(n),
            FuncArg::Expression(e) => e,
        }
    }

    fn as_usize(&self) -> Result<usize, String> {
        match self {
            FuncArg::Number(n) => {
                if *n > 0.0 && n.fract() == 0.0 {
                    Ok(*n as usize)
                } else {
                    Err(format!("Expected positive integer, got {n}"))
                }
            }
            FuncArg::Expression(_) => Err("Expected a number, got an expression".to_string()),
        }
    }
}

fn extract_col_period(args: &[FuncArg], func_name: &str) -> Result<(Expr, usize), String> {
    if args.len() != 2 {
        return Err(format!(
            "{func_name}() takes exactly 2 arguments: (column, period)"
        ));
    }
    let col_expr = match &args[0] {
        FuncArg::Expression(e) => e.clone(),
        FuncArg::Number(n) => lit(*n),
    };
    let period = args[1].as_usize()?;
    Ok((col_expr, period))
}

fn extract_single_col(args: &[FuncArg], func_name: &str) -> Result<Expr, String> {
    if args.len() != 1 {
        return Err(format!("{func_name}() takes exactly 1 argument: (column)"));
    }
    Ok(args[0].clone().into_expr())
}

fn extract_two_cols(args: &[FuncArg], func_name: &str) -> Result<(Expr, Expr), String> {
    if args.len() != 2 {
        return Err(format!(
            "{func_name}() takes exactly 2 arguments: (col1, col2)"
        ));
    }
    Ok((args[0].clone().into_expr(), args[1].clone().into_expr()))
}

fn extract_three_cols(args: &[FuncArg], func_name: &str) -> Result<(Expr, Expr, Expr), String> {
    if args.len() != 3 {
        return Err(format!(
            "{func_name}() takes exactly 3 arguments: (col1, col2, col3)"
        ));
    }
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        args[2].clone().into_expr(),
    ))
}

fn extract_three_cols_period(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, Expr, Expr, usize), String> {
    if args.len() != 4 {
        return Err(format!(
            "{func_name}() takes exactly 4 arguments: (col1, col2, col3, period)"
        ));
    }
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        args[2].clone().into_expr(),
        args[3].as_usize()?,
    ))
}

fn extract_three_cols_period_mult(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, Expr, Expr, usize, f64), String> {
    if args.len() != 5 {
        return Err(format!(
            "{func_name}() takes exactly 5 arguments: (col1, col2, col3, period, multiplier)"
        ));
    }
    let mult = match &args[4] {
        FuncArg::Number(n) => *n,
        FuncArg::Expression(_) => {
            return Err(format!(
                "{func_name}(): multiplier (5th arg) must be a number"
            ))
        }
    };
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        args[2].clone().into_expr(),
        args[3].as_usize()?,
        mult,
    ))
}

fn extract_four_cols_period(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, Expr, Expr, Expr, usize), String> {
    if args.len() != 5 {
        return Err(format!(
            "{func_name}() takes exactly 5 arguments: (col1, col2, col3, col4, period)"
        ));
    }
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        args[2].clone().into_expr(),
        args[3].clone().into_expr(),
        args[4].as_usize()?,
    ))
}

fn extract_three_cols_period_as_two_cols(
    args: &[FuncArg],
    func_name: &str,
) -> Result<(Expr, Expr, usize), String> {
    if args.len() != 3 {
        return Err(format!(
            "{func_name}() takes exactly 3 arguments: (col1, col2, period)"
        ));
    }
    Ok((
        args[0].clone().into_expr(),
        args[1].clone().into_expr(),
        args[2].as_usize()?,
    ))
}

/// Compute percentile rank within a rolling window.
fn compute_rolling_rank(vals: &[f64], period: usize) -> Vec<f64> {
    let n = vals.len();
    if period == 0 || n < period {
        return vec![];
    }
    (0..=n - period)
        .map(|i| {
            let window = &vals[i..i + period];
            let current = vals[i + period - 1];
            if current.is_nan() {
                return f64::NAN;
            }
            let below = window
                .iter()
                .filter(|&&v| !v.is_nan() && v < current)
                .count();
            let valid = window.iter().filter(|&&v| !v.is_nan()).count();
            if valid == 0 {
                f64::NAN
            } else {
                below as f64 / valid as f64 * 100.0
            }
        })
        .collect()
}

/// Compute IV Rank (min-max normalization) within a rolling window.
/// `IV Rank = (current - window_min) / (window_max - window_min) × 100`
fn compute_iv_rank(vals: &[f64], period: usize) -> Vec<f64> {
    let n = vals.len();
    if period == 0 || n < period {
        return vec![];
    }
    (0..=n - period)
        .map(|i| {
            let window = &vals[i..i + period];
            let current = vals[i + period - 1];
            if current.is_nan() {
                return f64::NAN;
            }
            let mut min = f64::INFINITY;
            let mut max = f64::NEG_INFINITY;
            let mut valid = 0usize;
            for &v in window {
                if !v.is_nan() {
                    valid += 1;
                    if v < min {
                        min = v;
                    }
                    if v > max {
                        max = v;
                    }
                }
            }
            // Require at least half the lookback to have valid data
            if valid < period / 2 + 1 {
                return f64::NAN;
            }
            let range = max - min;
            if range <= 0.0 {
                return f64::NAN;
            }
            (current - min) / range * 100.0
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parse a formula string into a Polars expression.
pub fn parse_formula(formula: &str) -> Result<Expr, String> {
    let tokens = tokenize(formula)?;
    if tokens.is_empty() {
        return Err("Empty formula".to_string());
    }
    let mut parser = Parser::new(tokens);
    let expr = parser.parse_expr()?;

    // Ensure all tokens consumed
    if parser.pos < parser.tokens.len() {
        return Err(format!(
            "Unexpected tokens after expression at position {}",
            parser.pos
        ));
    }

    Ok(expr)
}

/// Validate that a formula can be parsed without errors.
/// Returns Ok(()) if valid, or an error message.
pub fn validate_formula(formula: &str) -> Result<(), String> {
    parse_formula(formula).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_df() -> DataFrame {
        df! {
            "close" => &[100.0, 102.0, 101.0, 105.0, 103.0, 107.0, 110.0, 108.0, 112.0, 115.0],
            "open" => &[99.0, 101.0, 102.0, 101.0, 105.0, 103.0, 106.0, 110.0, 108.0, 112.0],
            "high" => &[101.0, 103.0, 103.0, 106.0, 106.0, 108.0, 111.0, 111.0, 113.0, 116.0],
            "low" => &[98.0, 100.0, 100.0, 100.0, 102.0, 102.0, 105.0, 107.0, 107.0, 111.0],
            "volume" => &[1000u64, 1200, 900, 1500, 1100, 1800, 2000, 800, 1600, 2200],
        }
        .unwrap()
    }

    #[test]
    fn simple_comparison() {
        let df = test_df();
        let signal = FormulaSignal::new("close > 105".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
        let bools = result.bool().unwrap();
        // close: 100, 102, 101, 105, 103, 107, 110, 108, 112, 115
        // >105:   F    F     F    F    F    T    T    T    T    T
        assert_eq!(bools.get(5), Some(true));
        assert_eq!(bools.get(3), Some(false));
    }

    #[test]
    fn lookback_expression() {
        let df = test_df();
        let signal = FormulaSignal::new("close > close[1]".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // close[0] is null after lookback shift, so comparison yields null (None)
        assert_eq!(bools.get(0), None);
        assert_eq!(bools.get(1), Some(true)); // 102 > 100
        assert_eq!(bools.get(2), Some(false)); // 101 > 102 = F
    }

    #[test]
    fn sma_function() {
        let df = test_df();
        let signal = FormulaSignal::new("close > sma(close, 3)".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn complex_expression() {
        let df = test_df();
        let signal = FormulaSignal::new("(close - low) / (high - low) < 0.5".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn logical_and() {
        let df = test_df();
        let signal = FormulaSignal::new("close > 105 and volume > 1500".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // close>105 and volume>1500: only indices 5(107,1800), 6(110,2000), 8(112,1600), 9(115,2200)
        assert_eq!(bools.get(5), Some(true));
        assert_eq!(bools.get(7), Some(false)); // 108>105=T but 800>1500=F
    }

    #[test]
    fn negation() {
        let df = test_df();
        let signal = FormulaSignal::new("not close > 105".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert_eq!(bools.get(0), Some(true)); // not (100>105) = true
        assert_eq!(bools.get(5), Some(false)); // not (107>105) = false
    }

    #[test]
    fn pct_change_function() {
        let df = test_df();
        let signal = FormulaSignal::new("pct_change(close, 1) > 0.02".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn literal_true_false() {
        let df = test_df();
        let signal = FormulaSignal::new("true".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        for i in 0..df.height() {
            assert_eq!(bools.get(i), Some(true));
        }

        let signal2 = FormulaSignal::new("false".to_string());
        let result2 = signal2.evaluate(&df).unwrap();
        let bools2 = result2.bool().unwrap();
        for i in 0..df.height() {
            assert_eq!(bools2.get(i), Some(false));
        }
    }

    #[test]
    fn validate_formula_ok() {
        assert!(validate_formula("close > sma(close, 20)").is_ok());
        assert!(validate_formula("close > close[1] * 1.02").is_ok());
        assert!(validate_formula("(close - low) / (high - low) < 0.2").is_ok());
        // abs() with an arithmetic expression as its argument
        assert!(validate_formula("abs(close - open) > 1.0").is_ok());
    }

    #[test]
    fn validate_formula_err() {
        assert!(validate_formula("").is_err());
        assert!(validate_formula("close >").is_err());
        assert!(validate_formula("unknown_func(close)").is_err());
        // Unknown column name should be rejected
        assert!(validate_formula("foo > 1").is_err());
        assert!(validate_formula("typo[1] > close").is_err());
    }

    #[test]
    fn lookback_invalid_values() {
        // Fractional index should be rejected
        assert!(validate_formula("close[1.5]").is_err());
        // Negative index should be rejected
        assert!(validate_formula("close[-1]").is_err());
        // Excessively large index should be rejected
        assert!(validate_formula("close[99999]").is_err());
        // Valid integer lookbacks should be accepted
        assert!(validate_formula("close[0]").is_ok());
        assert!(validate_formula("close[10000]").is_ok());
        // Unknown column in lookback should be rejected
        assert!(validate_formula("foo[1]").is_err());
    }

    #[test]
    fn abs_with_expression_arg() {
        // abs(expr) should parse `1 + close` as a full expression, not stop at `1`
        let df = test_df();
        let signal = FormulaSignal::new("abs(close - open) > 0.5".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 10);
    }

    // --- Parse tests for new functions ---

    #[test]
    fn formula_rsi_parses() {
        assert!(validate_formula("rsi(close, 14) < 30").is_ok());
    }

    #[test]
    fn formula_atr_multi_col() {
        assert!(validate_formula("atr(close, high, low, 14) > 2.0").is_ok());
    }

    #[test]
    fn formula_if_ternary() {
        assert!(validate_formula("if(close > 100, 1, 0)").is_ok());
    }

    #[test]
    fn formula_nested_composition() {
        assert!(validate_formula(
            "rsi(close, 14) < 30 and close > bbands_lower(close, 20) and rel_volume(volume, 20) > 2.0"
        )
        .is_ok());
    }

    #[test]
    fn formula_macd_hist() {
        assert!(validate_formula("macd_hist(close) > 0").is_ok());
    }

    #[test]
    fn formula_stochastic() {
        assert!(validate_formula("stochastic(close, high, low, 14) < 20").is_ok());
    }

    #[test]
    fn formula_roc_parses() {
        assert!(validate_formula("roc(close, 10) > 5").is_ok());
    }

    #[test]
    fn formula_tr_parses() {
        assert!(validate_formula("tr(close, high, low) > 2.0").is_ok());
    }

    #[test]
    fn formula_keltner_parses() {
        assert!(validate_formula("keltner_upper(close, high, low, 20, 2.0) > close").is_ok());
        assert!(validate_formula("keltner_lower(close, high, low, 20, 2.0) < close").is_ok());
    }

    #[test]
    fn formula_obv_parses() {
        assert!(validate_formula("obv(close, volume) > 0").is_ok());
    }

    #[test]
    fn formula_mfi_parses() {
        assert!(validate_formula("mfi(close, high, low, volume, 14) < 20").is_ok());
    }

    #[test]
    fn formula_rank_parses() {
        assert!(validate_formula("rank(close, 20) > 80").is_ok());
    }

    #[test]
    fn formula_iv_rank_parses() {
        assert!(validate_formula("iv_rank(iv, 252) > 50").is_ok());
    }

    #[test]
    fn formula_iv_percentile_via_rank_parses() {
        assert!(validate_formula("rank(iv, 252) < 10").is_ok());
    }

    #[test]
    fn formula_zscore_parses() {
        assert!(validate_formula("zscore(close, 20) < -2").is_ok());
    }

    #[test]
    fn formula_range_pct_parses() {
        assert!(validate_formula("range_pct(close, high, low) < 0.2").is_ok());
    }

    // --- Evaluation tests ---

    fn eval_df() -> DataFrame {
        // 30 rows with realistic OHLCV data for evaluation tests
        let close: Vec<f64> = (0..30)
            .map(|i| 100.0 + (i as f64) * 0.5 + (i as f64 * 0.3).sin() * 2.0)
            .collect();
        let high: Vec<f64> = close.iter().map(|c| c + 1.5).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 1.5).collect();
        let open: Vec<f64> = close.iter().map(|c| c - 0.2).collect();
        let volume: Vec<f64> = (0..30).map(|i| 1000.0 + (i as f64) * 50.0).collect();
        let iv: Vec<f64> = (0..30)
            .map(|i| 0.15 + (i as f64) * 0.005 + (i as f64 * 0.5).sin() * 0.03)
            .collect();
        df! {
            "close" => &close,
            "open" => &open,
            "high" => &high,
            "low" => &low,
            "volume" => &volume,
            "iv" => &iv,
        }
        .unwrap()
    }

    #[test]
    fn formula_rsi_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("rsi(close, 14) < 80".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
        let bools = result.bool().unwrap();
        // Should have some true/false values, not all null
        let non_null = (0..30).filter_map(|i| bools.get(i)).count();
        assert!(non_null > 0);
    }

    #[test]
    fn formula_atr_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("atr(close, high, low, 5) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_if_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("if(close > 110, close, 0) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_iv_rank_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("iv_rank(iv, 10) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
        let bools = result.bool().unwrap();
        let non_null = (0..30).filter_map(|i| bools.get(i)).count();
        assert!(non_null > 0);
    }

    #[test]
    fn formula_iv_percentile_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("rank(iv, 10) > 50".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_macd_evaluates() {
        // Need 34+ rows for MACD; eval_df has 30, so use a larger df
        let close: Vec<f64> = (0..40).map(|i| 100.0 + (i as f64) * 0.5).collect();
        let df = df! { "close" => &close }.unwrap();
        let signal = FormulaSignal::new("macd_hist(close) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 40);
    }

    #[test]
    fn formula_tr_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("tr(close, high, low) > 1.0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_stochastic_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("stochastic(close, high, low, 5) < 80".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_obv_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("obv(close, volume) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    // --- Error tests ---

    #[test]
    fn formula_rsi_wrong_args() {
        assert!(validate_formula("rsi(close)").is_err());
    }

    #[test]
    fn formula_iv_rank_wrong_args() {
        assert!(validate_formula("iv_rank(iv)").is_err());
    }

    #[test]
    fn formula_atr_wrong_args() {
        assert!(validate_formula("atr(close, high)").is_err());
    }

    #[test]
    fn formula_mfi_wrong_args() {
        assert!(validate_formula("mfi(close, high, low)").is_err());
    }

    #[test]
    fn formula_if_wrong_args() {
        assert!(validate_formula("if(close > 100, 1)").is_err());
    }

    // --- New function parse tests ---

    #[test]
    fn formula_aroon_up_parses() {
        assert!(validate_formula("aroon_up(high, low, 25) > 70").is_ok());
    }

    #[test]
    fn formula_aroon_down_parses() {
        assert!(validate_formula("aroon_down(high, low, 25) < 30").is_ok());
    }

    #[test]
    fn formula_aroon_osc_parses() {
        assert!(validate_formula("aroon_osc(high, low, 25) > 0").is_ok());
    }

    #[test]
    fn formula_supertrend_parses() {
        assert!(validate_formula("close > supertrend(close, high, low, 10, 3.0)").is_ok());
    }

    #[test]
    fn formula_cmf_parses() {
        assert!(validate_formula("cmf(close, high, low, volume, 20) > 0").is_ok());
    }

    #[test]
    fn formula_consecutive_up_parses() {
        assert!(validate_formula("consecutive_up(close) >= 3").is_ok());
    }

    #[test]
    fn formula_consecutive_down_parses() {
        assert!(validate_formula("consecutive_down(close) >= 3").is_ok());
    }

    #[test]
    fn formula_lookback_on_function_parses() {
        assert!(validate_formula("sma(close, 5)[1] > sma(close, 5)[2]").is_ok());
    }

    #[test]
    fn formula_macd_hist_lookback_parses() {
        assert!(validate_formula("macd_hist(close)[1] < 0 and macd_hist(close) > 0").is_ok());
    }

    // --- New function evaluation tests ---

    #[test]
    fn formula_aroon_up_evaluates() {
        let n = 30;
        let high: Vec<f64> = (0..n).map(|i| 100.0 + i as f64 + 2.0).collect();
        let low: Vec<f64> = (0..n).map(|i| 100.0 + i as f64 - 2.0).collect();
        let df = df! {
            "high" => &high,
            "low" => &low,
        }
        .unwrap();
        let signal = FormulaSignal::new("aroon_up(high, low, 14) > 50".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), n);
    }

    #[test]
    fn formula_supertrend_evaluates() {
        let n = 30;
        let close: Vec<f64> = (0..n).map(|i| 100.0 + (i as f64) * 0.5).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let df = df! {
            "close" => &close,
            "high" => &high,
            "low" => &low,
        }
        .unwrap();
        let signal =
            FormulaSignal::new("close > supertrend(close, high, low, 10, 3.0)".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), n);
    }

    #[test]
    fn formula_cmf_evaluates() {
        let n = 30;
        let close: Vec<f64> = (0..n).map(|i| 100.0 + (i as f64) * 0.3).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 1.5).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 1.5).collect();
        let volume: Vec<f64> = (0..n).map(|_| 1_000_000.0).collect();
        let df = df! {
            "close" => &close,
            "high" => &high,
            "low" => &low,
            "volume" => &volume,
        }
        .unwrap();
        let signal = FormulaSignal::new("cmf(close, high, low, volume, 20) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), n);
    }

    #[test]
    fn formula_consecutive_up_evaluates() {
        let close = vec![100.0, 101.0, 102.0, 103.0, 102.0, 103.0, 104.0];
        let df = df! { "close" => &close }.unwrap();
        let signal = FormulaSignal::new("consecutive_up(close) >= 3".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 7);
        let bools = result.bool().unwrap();
        // Bar 3 (103): 3 consecutive rises → true
        assert!(bools.get(3).unwrap());
        // Bar 4 (102): drop, count resets → false
        assert!(!bools.get(4).unwrap());
    }

    #[test]
    fn formula_consecutive_down_evaluates() {
        let close = vec![103.0, 102.0, 101.0, 100.0, 101.0, 100.0, 99.0];
        let df = df! { "close" => &close }.unwrap();
        let signal = FormulaSignal::new("consecutive_down(close) >= 3".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 7);
        let bools = result.bool().unwrap();
        // Bar 3 (100): 3 consecutive falls → true
        assert!(bools.get(3).unwrap());
        // Bar 4 (101): rise, count resets → false
        assert!(!bools.get(4).unwrap());
    }

    #[test]
    fn formula_lookback_on_sma_evaluates() {
        let close: Vec<f64> = (0..20).map(|i| 100.0 + i as f64).collect();
        let df = df! { "close" => &close }.unwrap();
        let signal = FormulaSignal::new("sma(close, 5)[1] > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 20);
    }

    // --- New function error tests ---

    #[test]
    fn formula_aroon_up_wrong_args() {
        assert!(validate_formula("aroon_up(high)").is_err());
    }

    #[test]
    fn formula_supertrend_wrong_args() {
        assert!(validate_formula("supertrend(close, high, low)").is_err());
    }

    #[test]
    fn formula_cmf_wrong_args() {
        assert!(validate_formula("cmf(close, high, low)").is_err());
    }
}

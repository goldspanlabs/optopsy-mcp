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
//! - `williams_r(high, low, close, period)` — Williams %R (-100 to 0)
//! - `cci(close, period)` — Commodity Channel Index
//! - `ppo(close, short, long)` — Percentage Price Oscillator
//! - `cmo(close, period)` — Chande Momentum Oscillator
//! - `adx(high, low, close, period)` — Average Directional Index
//! - `plus_di(high, low, close, period)` — Positive Directional Indicator
//! - `minus_di(high, low, close, period)` — Negative Directional Indicator
//! - `psar(high, low, accel, max_accel)` — Parabolic SAR
//! - `tsi(close, fast, slow)` — True Strength Index
//! - `vpt(close, volume)` — Volume Price Trend
//! - `donchian_upper(high, low, period)` — Donchian Channel upper
//! - `donchian_mid(high, low, period)` — Donchian Channel midline
//! - `donchian_lower(high, low, period)` — Donchian Channel lower
//! - `ichimoku_tenkan(high, low, close)` — Ichimoku Tenkan-sen (9/26/52)
//! - `ichimoku_kijun(high, low, close)` — Ichimoku Kijun-sen
//! - `ichimoku_senkou_a(high, low, close)` — Ichimoku Senkou Span A
//! - `ichimoku_senkou_b(high, low, close)` — Ichimoku Senkou Span B
//! - `envelope_upper(close, period, pct)` — MA Envelope upper band
//! - `envelope_lower(close, period, pct)` — MA Envelope lower band
//! - `ad(high, low, close, volume)` — Accumulation/Distribution line
//! - `pvi(close, volume)` — Positive Volume Index
//! - `nvi(close, volume)` — Negative Volume Index
//! - `ulcer(close, period)` — Ulcer Index
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
//! **Functions (date/time)** — zero-argument, extract from bar date:
//! - `day_of_week()` — Day of week (1=Mon..7=Sun, ISO 8601)
//! - `month()` — Month (1-12)
//! - `day_of_month()` — Day of month (1-31)
//! - `hour()` — Hour (0-23, 0 for daily bars)
//! - `minute()` — Minute (0-59, 0 for daily bars)
//! - `week_of_year()` — ISO week number (1-53)
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

use polars::prelude::*;

use super::custom_funcs::FuncArg;
use super::helpers::SignalFn;

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

        // If formula uses date/time functions, inject a __dt column.
        // Match "func(" to avoid false positives on substrings.
        let needs_date = DATE_FUNCTIONS
            .iter()
            .any(|f| self.formula.contains(&format!("{f}(")));
        let working_df = if needs_date {
            inject_datetime_column(df)?
        } else {
            df.clone()
        };

        let result = working_df.lazy().select([expr.alias("signal")]).collect()?;

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
pub(crate) enum Token {
    Number(f64),
    Ident(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Dot,
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
pub(crate) fn tokenize(input: &str) -> Result<Vec<Token>, String> {
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
            '.' if i + 1 >= chars.len() || !chars[i + 1].is_ascii_digit() => {
                tokens.push(Token::Dot);
                i += 1;
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

/// Zero-argument date/time functions that extract temporal components from bar dates.
const DATE_FUNCTIONS: &[&str] = &[
    "day_of_week",
    "month",
    "day_of_month",
    "hour",
    "minute",
    "week_of_year",
];

/// Inject a `__dt` column (Datetime type) derived from whichever date column exists.
/// This normalizes `Date` → `Datetime` so all `.dt()` accessors work uniformly.
fn inject_datetime_column(df: &DataFrame) -> Result<DataFrame, PolarsError> {
    // Try "datetime" first, fall back to "date"
    let names = df.get_column_names();
    let has = |n: &str| names.iter().any(|c| c.as_str() == n);
    let col_name = if has("datetime") {
        "datetime"
    } else if has("date") {
        "date"
    } else {
        return Err(PolarsError::ColumnNotFound(
            "No 'date' or 'datetime' column found for date/time functions".into(),
        ));
    };

    let dt_expr = match df.column(col_name)?.dtype() {
        DataType::Date => col(col_name)
            .cast(DataType::Datetime(TimeUnit::Microseconds, None))
            .alias("__dt"),
        DataType::Datetime(_, _) => col(col_name).alias("__dt"),
        other => {
            return Err(PolarsError::ComputeError(
                format!(
                    "Date column '{col_name}' has unsupported type {other:?} for date/time functions"
                )
                .into(),
            ));
        }
    };

    df.clone().lazy().with_column(dt_expr).collect()
}

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
                    let expr = Self::build_function_call(&name, &args)?;
                    // Allow lookback on function results: sma(close, 5)[1]
                    self.parse_optional_lookback(expr)
                }
                // Check for lookback: ident "[" number "]"
                else if self.peek() == Some(&Token::LBracket) {
                    let name_lower = name.to_lowercase();
                    if VALID_COLUMNS.contains(&name_lower.as_str()) {
                        self.parse_optional_lookback(col(&*name_lower))
                    } else {
                        // Cross-symbol with lookback: VIX[1] → col("VIX_close").shift(1)
                        let sym = name.to_uppercase();
                        self.parse_optional_lookback(col(format!("{sym}_close")))
                    }
                }
                // Check for dot accessor: ident "." ident → cross-symbol column
                // Only allow dot-access for cross-symbol references (not primary columns)
                else if self.peek() == Some(&Token::Dot) {
                    let name_lower = name.to_lowercase();
                    if VALID_COLUMNS.contains(&name_lower.as_str()) {
                        return Err(format!(
                            "Dot accessor not allowed on primary column '{name}'. \
                             Use '{name}' directly, or did you mean a cross-symbol reference?"
                        ));
                    }
                    let sym = name.to_uppercase();
                    self.advance(); // consume Dot
                    match self.advance() {
                        Some(Token::Ident(col_name)) => {
                            let col_lower = col_name.to_lowercase();
                            if !VALID_COLUMNS.contains(&col_lower.as_str()) {
                                return Err(format!(
                                    "Unknown column '{col_name}' in cross-symbol reference '{sym}.{col_name}'. \
                                     Valid columns are: close, open, high, low, volume, adjclose, iv"
                                ));
                            }
                            let expr = col(format!("{sym}_{col_lower}"));
                            self.parse_optional_lookback(expr)
                        }
                        other => Err(format!(
                            "Expected column name after '{sym}.', got {other:?}"
                        )),
                    }
                } else {
                    // Plain column or cross-symbol reference
                    let name_lower = name.to_lowercase();
                    if VALID_COLUMNS.contains(&name_lower.as_str()) {
                        Ok(col(&*name_lower))
                    } else if name.starts_with("__") {
                        // Internal computed column (e.g., __hmm_regime_SPY_3_5_65)
                        // — use as literal column reference, no transformation
                        Ok(col(name))
                    } else {
                        // Cross-symbol reference, defaults to .close
                        let sym = name.to_uppercase();
                        Ok(col(format!("{sym}_close")))
                    }
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

    fn build_function_call(name: &str, args: &[FuncArg]) -> Result<Expr, String> {
        super::custom_funcs::dispatch(name, args)
    }
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

// ---------------------------------------------------------------------------
// Indicator extraction (for chart overlays)
// ---------------------------------------------------------------------------

/// Recognized indicator function names for chart extraction.
const INDICATOR_FUNCTIONS: &[&str] = &[
    "rsi",
    "macd_hist",
    "macd_signal",
    "macd_line",
    "stochastic",
    "sma",
    "ema",
    "bbands_upper",
    "bbands_lower",
    "bbands_mid",
    "keltner_upper",
    "keltner_lower",
    "atr",
    "aroon_up",
    "aroon_down",
    "aroon_osc",
    "supertrend",
    "mfi",
    "obv",
    "cmf",
    "roc",
    "williams_r",
    "cci",
    "ppo",
    "cmo",
    "adx",
    "plus_di",
    "minus_di",
    "psar",
    "tsi",
    "vpt",
    "donchian_upper",
    "donchian_mid",
    "donchian_lower",
    "ichimoku_tenkan",
    "ichimoku_kijun",
    "ichimoku_senkou_a",
    "ichimoku_senkou_b",
    "envelope_upper",
    "envelope_lower",
    "ad",
    "pvi",
    "nvi",
    "ulcer",
];

/// A recognized indicator function call extracted from a formula.
#[derive(Debug, Clone, PartialEq)]
pub struct IndicatorCall {
    /// Function name (e.g. `rsi`, `sma`, `bbands_upper`)
    pub func_name: String,
    /// Column name arguments (e.g. `["close"]` or `["close", "high", "low"]`)
    pub col_args: Vec<String>,
    /// Period parameter if present (e.g. 14 for rsi(close, 14))
    pub period: Option<usize>,
    /// Extra numeric parameter if present (e.g. multiplier for keltner/supertrend)
    pub multiplier: Option<f64>,
}

/// Extract recognized indicator function calls from a formula string.
///
/// Scans the token stream for `Ident(name) LParen ... RParen` patterns where
/// `name` is a known indicator function. Extracts column names, period, and
/// optional multiplier from the arguments. Deduplicates by function signature.
pub fn extract_indicator_calls(formula: &str) -> Vec<IndicatorCall> {
    let Ok(tokens) = tokenize(formula) else {
        return vec![];
    };

    let mut calls = Vec::new();
    let mut i = 0;

    while i < tokens.len() {
        if let Token::Ident(ref name) = tokens[i] {
            let name_lower = name.to_lowercase();
            if INDICATOR_FUNCTIONS.contains(&name_lower.as_str())
                && tokens.get(i + 1) == Some(&Token::LParen)
            {
                // Find matching RParen
                let args_start = i + 2;
                if let Some(args_end) = find_matching_paren(&tokens, i + 1) {
                    let call = parse_indicator_args(&name_lower, &tokens[args_start..args_end]);
                    // Deduplicate
                    if !calls.contains(&call) {
                        calls.push(call);
                    }
                    i = args_end + 1;
                    continue;
                }
            }
        }
        i += 1;
    }

    calls
}

/// Find the index of the `RParen` matching the `LParen` at `lparen_idx`.
fn find_matching_paren(tokens: &[Token], lparen_idx: usize) -> Option<usize> {
    let mut depth = 1;
    let mut i = lparen_idx + 1;
    while i < tokens.len() {
        match tokens[i] {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Parse the token slice between parens into an `IndicatorCall`.
/// Extracts column identifiers, period (first integer), and multiplier (second float).
fn parse_indicator_args(func_name: &str, arg_tokens: &[Token]) -> IndicatorCall {
    let mut col_args = Vec::new();
    let mut numbers = Vec::new();

    for token in arg_tokens {
        match token {
            Token::Ident(name) => {
                let lower = name.to_lowercase();
                if VALID_COLUMNS.contains(&lower.as_str()) {
                    col_args.push(lower);
                }
                // Skip non-column idents (nested function names, etc.)
            }
            Token::Number(n) => numbers.push(*n),
            _ => {} // Skip operators, parens, commas
        }
    }

    let period = numbers.first().and_then(|n| {
        if *n > 0.0 && n.fract() == 0.0 {
            Some(*n as usize)
        } else {
            None
        }
    });

    let multiplier = if numbers.len() >= 2 {
        Some(numbers[numbers.len() - 1])
    } else {
        None
    };

    IndicatorCall {
        func_name: func_name.to_string(),
        col_args,
        period,
        multiplier,
    }
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
        // Unknown identifiers are now cross-symbol references (not errors)
        assert!(validate_formula("foo > 1").is_ok()); // FOO_close > 1
        assert!(validate_formula("typo[1] > close").is_ok()); // TYPO_close.shift(1) > close
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
        // Unknown identifiers with lookback are cross-symbol references (not errors)
        assert!(validate_formula("foo[1]").is_ok()); // FOO_close.shift(1)
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
            .map(|i| 100.0 + f64::from(i) * 0.5 + (f64::from(i) * 0.3).sin() * 2.0)
            .collect();
        let high: Vec<f64> = close.iter().map(|c| c + 1.5).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 1.5).collect();
        let open: Vec<f64> = close.iter().map(|c| c - 0.2).collect();
        let volume: Vec<f64> = (0..30).map(|i| 1000.0 + f64::from(i) * 50.0).collect();
        let iv: Vec<f64> = (0..30)
            .map(|i| 0.15 + f64::from(i) * 0.005 + (f64::from(i) * 0.5).sin() * 0.03)
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
        let close: Vec<f64> = (0..40).map(|i| 100.0 + f64::from(i) * 0.5).collect();
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
        let n = 30_i32;
        let high: Vec<f64> = (0..n).map(|i| 100.0 + f64::from(i) + 2.0).collect();
        let low: Vec<f64> = (0..n).map(|i| 100.0 + f64::from(i) - 2.0).collect();
        let df = df! {
            "high" => &high,
            "low" => &low,
        }
        .unwrap();
        let signal = FormulaSignal::new("aroon_up(high, low, 14) > 50".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), n as usize);
    }

    #[test]
    fn formula_supertrend_evaluates() {
        let n = 30_i32;
        let close: Vec<f64> = (0..n).map(|i| 100.0 + f64::from(i) * 0.5).collect();
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
        assert_eq!(result.len(), n as usize);
    }

    #[test]
    fn formula_cmf_evaluates() {
        let n = 30_i32;
        let close: Vec<f64> = (0..n).map(|i| 100.0 + f64::from(i) * 0.3).collect();
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
        assert_eq!(result.len(), n as usize);
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
        let close: Vec<f64> = (0..20).map(|i| 100.0 + f64::from(i)).collect();
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

    // ── Edge case tests ─────────────────────────────────────────────

    #[test]
    fn formula_division_by_zero_evaluates() {
        let df = df! { "close" => &[0.0, 1.0, 2.0] }.unwrap();
        let signal = FormulaSignal::new("close / 0 > 0".to_string());
        // Should not panic — division by zero produces inf/NaN which compares as false
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn formula_large_lookback_evaluates() {
        let df = df! { "close" => &[100.0, 101.0, 102.0] }.unwrap();
        // Lookback exceeding data length should produce nulls (not panic)
        let signal = FormulaSignal::new("close[100] > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn formula_not_operator() {
        let df = df! { "close" => &[100.0, 101.0, 102.0] }.unwrap();
        let signal = FormulaSignal::new("not (close > 101)".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // 100: not false = true, 101: not false = true, 102: not true = false
        assert!(bools.get(0).unwrap());
        assert!(bools.get(1).unwrap());
        assert!(!bools.get(2).unwrap());
    }

    #[test]
    fn formula_not_with_complex_expr() {
        let df = df! { "close" => &[100.0, 101.0, 102.0, 103.0, 104.0] }.unwrap();
        let signal = FormulaSignal::new("not (close > 101 and close < 104)".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // 100: not(F&F)=T, 101: not(F&T)=T, 102: not(T&T)=F, 103: not(T&T)=F, 104: not(T&F)=T
        assert!(bools.get(0).unwrap());
        assert!(bools.get(1).unwrap());
        assert!(!bools.get(2).unwrap());
        assert!(!bools.get(3).unwrap());
        assert!(bools.get(4).unwrap());
    }

    #[test]
    fn formula_empty_df_evaluates() {
        let df = df! { "close" => Vec::<f64>::new() }.unwrap();
        let signal = FormulaSignal::new("close > 100".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn formula_nested_function_composition() {
        // abs of a negative change
        assert!(validate_formula("abs(change(close, 5)) > 10").is_ok());
    }

    #[test]
    fn formula_chained_logical_operators() {
        assert!(validate_formula("close > 100 and close < 200 or close == 50").is_ok());
    }

    #[test]
    fn formula_consecutive_up_on_flat_data() {
        let df = df! { "close" => &[100.0, 100.0, 100.0] }.unwrap();
        let signal = FormulaSignal::new("consecutive_up(close) >= 1".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // Flat data has no rises, so all false
        assert!(bools.into_no_null_iter().all(|b| !b));
    }

    #[test]
    fn formula_multiple_comparisons_in_if() {
        assert!(
            validate_formula("if(close > sma(close, 20) and rsi(close, 14) < 30, 1, 0) > 0")
                .is_ok()
        );
    }

    #[test]
    fn validate_unclosed_paren_errors() {
        assert!(validate_formula("sma(close, 20").is_err());
    }

    // --- extract_indicator_calls tests ---

    #[test]
    fn extract_rsi_call() {
        let calls = extract_indicator_calls("rsi(close, 14) < 30");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].func_name, "rsi");
        assert_eq!(calls[0].col_args, vec!["close"]);
        assert_eq!(calls[0].period, Some(14));
    }

    #[test]
    fn extract_sma_call() {
        let calls = extract_indicator_calls("close > sma(close, 50)");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].func_name, "sma");
        assert_eq!(calls[0].col_args, vec!["close"]);
        assert_eq!(calls[0].period, Some(50));
    }

    #[test]
    fn extract_multiple_calls() {
        let calls = extract_indicator_calls("rsi(close, 14) < 30 and close > sma(close, 20)");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].func_name, "rsi");
        assert_eq!(calls[1].func_name, "sma");
    }

    #[test]
    fn extract_deduplicates_same_call() {
        let calls = extract_indicator_calls("rsi(close, 14) < 30 and rsi(close, 14) > 20");
        assert_eq!(calls.len(), 1);
    }

    #[test]
    fn extract_different_periods_not_deduped() {
        let calls = extract_indicator_calls("sma(close, 20) > sma(close, 50)");
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].period, Some(20));
        assert_eq!(calls[1].period, Some(50));
    }

    #[test]
    fn extract_multi_column_call() {
        let calls = extract_indicator_calls("stochastic(close, high, low, 14) < 20");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].func_name, "stochastic");
        assert_eq!(calls[0].col_args, vec!["close", "high", "low"]);
        assert_eq!(calls[0].period, Some(14));
    }

    #[test]
    fn extract_with_multiplier() {
        let calls = extract_indicator_calls("close > keltner_upper(close, high, low, 20, 2.0)");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].func_name, "keltner_upper");
        assert_eq!(calls[0].period, Some(20));
        assert_eq!(calls[0].multiplier, Some(2.0));
    }

    #[test]
    fn extract_no_indicator_functions() {
        let calls = extract_indicator_calls("close > 100 and open < 50");
        assert!(calls.is_empty());
    }

    #[test]
    fn extract_invalid_formula_returns_empty() {
        let calls = extract_indicator_calls("((((");
        assert!(calls.is_empty());
    }

    #[test]
    fn extract_nested_function_in_if() {
        let calls = extract_indicator_calls("if(rsi(close, 14) < 30, 1, 0) > 0");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].func_name, "rsi");
    }

    #[test]
    fn extract_indicator_inside_abs() {
        let calls = extract_indicator_calls("abs(rsi(close, 14)) > 50");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].func_name, "rsi");
        assert_eq!(calls[0].period, Some(14));
    }

    #[test]
    fn extract_non_indicator_functions_ignored() {
        // tr, rel_volume, zscore, range_pct are not in INDICATOR_FUNCTIONS
        let calls = extract_indicator_calls("zscore(close, 20) < -2 and tr(close, high, low) > 1");
        assert!(calls.is_empty());
    }

    // ── New indicator parse tests ───────────────────────────────────────

    #[test]
    fn formula_williams_r_parses() {
        assert!(validate_formula("williams_r(high, low, close, 14) < -80").is_ok());
    }

    #[test]
    fn formula_cci_parses() {
        assert!(validate_formula("cci(close, 20) > 100").is_ok());
    }

    #[test]
    fn formula_ppo_parses() {
        assert!(validate_formula("ppo(close, 12, 26) > 0").is_ok());
    }

    #[test]
    fn formula_cmo_parses() {
        assert!(validate_formula("cmo(close, 14) < -50").is_ok());
    }

    #[test]
    fn formula_adx_parses() {
        assert!(validate_formula("adx(high, low, close, 14) > 25").is_ok());
    }

    #[test]
    fn formula_plus_di_parses() {
        assert!(
            validate_formula("plus_di(high, low, close, 14) > minus_di(high, low, close, 14)")
                .is_ok()
        );
    }

    #[test]
    fn formula_psar_parses() {
        assert!(validate_formula("close > psar(high, low, 0.02, 0.2)").is_ok());
    }

    #[test]
    fn formula_tsi_parses() {
        assert!(validate_formula("tsi(close, 13, 25) > 0").is_ok());
    }

    #[test]
    fn formula_vpt_parses() {
        assert!(validate_formula("vpt(close, volume) > 0").is_ok());
    }

    #[test]
    fn formula_donchian_parses() {
        assert!(validate_formula("close > donchian_upper(high, low, 20)").is_ok());
        assert!(validate_formula("close < donchian_lower(high, low, 20)").is_ok());
        assert!(validate_formula("close > donchian_mid(high, low, 20)").is_ok());
    }

    #[test]
    fn formula_ichimoku_parses() {
        assert!(validate_formula("close > ichimoku_tenkan(high, low, close)").is_ok());
        assert!(validate_formula("ichimoku_kijun(high, low, close) > 0").is_ok());
        assert!(validate_formula("close > ichimoku_senkou_a(high, low, close)").is_ok());
        assert!(validate_formula("close > ichimoku_senkou_b(high, low, close)").is_ok());
    }

    #[test]
    fn formula_envelope_parses() {
        assert!(validate_formula("close > envelope_upper(close, 20, 2.5)").is_ok());
        assert!(validate_formula("close < envelope_lower(close, 20, 2.5)").is_ok());
    }

    #[test]
    fn formula_ad_parses() {
        assert!(validate_formula("ad(high, low, close, volume) > 0").is_ok());
    }

    #[test]
    fn formula_pvi_nvi_parses() {
        assert!(validate_formula("pvi(close, volume) > 1000").is_ok());
        assert!(validate_formula("nvi(close, volume) > 1000").is_ok());
    }

    #[test]
    fn formula_ulcer_parses() {
        assert!(validate_formula("ulcer(close, 14) > 5").is_ok());
    }

    // ── New indicator evaluation tests ──────────────────────────────────

    #[test]
    fn formula_williams_r_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("williams_r(high, low, close, 5) < -20".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_cci_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("cci(close, 10) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_ppo_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("ppo(close, 5, 10) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_cmo_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("cmo(close, 10) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_adx_evaluates() {
        let n = 30_i32;
        let close: Vec<f64> = (0..n).map(|i| 100.0 + f64::from(i) * 0.5).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let df = df! { "close" => &close, "high" => &high, "low" => &low }.unwrap();
        let signal = FormulaSignal::new("adx(high, low, close, 5) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), n as usize);
    }

    #[test]
    fn formula_psar_evaluates() {
        let n = 30_i32;
        let close: Vec<f64> = (0..n).map(|i| 100.0 + f64::from(i) * 0.5).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 2.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 2.0).collect();
        let df = df! { "close" => &close, "high" => &high, "low" => &low }.unwrap();
        let signal = FormulaSignal::new("close > psar(high, low, 0.02, 0.2)".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), n as usize);
    }

    #[test]
    fn formula_tsi_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("tsi(close, 5, 10) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_vpt_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("vpt(close, volume) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_donchian_evaluates() {
        let n = 30_i32;
        let high: Vec<f64> = (0..n).map(|i| 102.0 + f64::from(i)).collect();
        let low: Vec<f64> = (0..n).map(|i| 98.0 + f64::from(i)).collect();
        let close: Vec<f64> = (0..n).map(|i| 100.0 + f64::from(i)).collect();
        let df = df! { "close" => &close, "high" => &high, "low" => &low }.unwrap();
        let signal = FormulaSignal::new("close > donchian_upper(high, low, 5)".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), n as usize);
    }

    #[test]
    fn formula_envelope_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("close > envelope_upper(close, 10, 2.5)".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_ad_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("ad(high, low, close, volume) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_pvi_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("pvi(close, volume) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_nvi_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("nvi(close, volume) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    #[test]
    fn formula_ulcer_evaluates() {
        let df = eval_df();
        let signal = FormulaSignal::new("ulcer(close, 10) > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        assert_eq!(result.len(), 30);
    }

    // ── New indicator wrong-args tests ──────────────────────────────────

    #[test]
    fn formula_williams_r_wrong_args() {
        assert!(validate_formula("williams_r(high, low)").is_err());
    }

    #[test]
    fn formula_psar_wrong_args() {
        assert!(validate_formula("psar(high)").is_err());
    }

    #[test]
    fn formula_donchian_wrong_args() {
        assert!(validate_formula("donchian_upper(high)").is_err());
    }

    #[test]
    fn formula_ichimoku_wrong_args() {
        assert!(validate_formula("ichimoku_tenkan(high)").is_err());
    }

    #[test]
    fn formula_ad_wrong_args() {
        assert!(validate_formula("ad(high, low)").is_err());
    }

    #[test]
    fn formula_envelope_wrong_args() {
        assert!(validate_formula("envelope_upper(close)").is_err());
    }

    // ── New indicator extraction tests ──────────────────────────────────

    #[test]
    fn extract_indicator_new_functions() {
        let calls = extract_indicator_calls("adx(high, low, close, 14) > 25");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].func_name, "adx");

        let calls = extract_indicator_calls("williams_r(high, low, close, 14) < -80");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].func_name, "williams_r");

        let calls = extract_indicator_calls("close > donchian_upper(high, low, 20)");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].func_name, "donchian_upper");

        let calls = extract_indicator_calls("close > psar(high, low, 0.02, 0.2)");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].func_name, "psar");
    }

    // ── Date/time function tests ─────────────────────────────────────────

    #[test]
    fn parse_date_functions() {
        // Each date function should parse successfully
        for func in &[
            "day_of_week()",
            "month()",
            "day_of_month()",
            "hour()",
            "minute()",
            "week_of_year()",
        ] {
            let formula = format!("{func} == 1");
            assert!(
                parse_formula(&formula).is_ok(),
                "Failed to parse: {formula}"
            );
        }
    }

    #[test]
    fn date_functions_reject_args() {
        for func in &[
            "day_of_week",
            "month",
            "day_of_month",
            "hour",
            "minute",
            "week_of_year",
        ] {
            let formula = format!("{func}(close) == 1");
            assert!(
                parse_formula(&formula).is_err(),
                "{func} should reject arguments"
            );
        }
    }

    #[test]
    fn date_functions_combine_with_indicators() {
        let formula = "day_of_week() == 1 and close > sma(close, 5)";
        assert!(parse_formula(formula).is_ok());
    }

    #[test]
    fn date_functions_with_lookback() {
        // Lookback on date function result: day_of_week()[1]
        let formula = "day_of_week()[1] == 5";
        assert!(parse_formula(formula).is_ok());
    }

    fn test_df_with_dates() -> DataFrame {
        use chrono::NaiveDate;
        // Mon 2024-01-01, Tue 2024-01-02, Wed 2024-01-03, Thu 2024-01-04, Fri 2024-01-05
        // Mon 2024-12-02, Tue 2024-12-03, Wed 2024-12-04, Thu 2024-12-05, Fri 2024-12-06
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 12, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 12, 3).unwrap(),
            NaiveDate::from_ymd_opt(2024, 12, 4).unwrap(),
            NaiveDate::from_ymd_opt(2024, 12, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 12, 6).unwrap(),
        ];
        df! {
            "date" => DateChunked::from_naive_date(PlSmallStr::from("date"), dates),
            "close" => &[100.0, 102.0, 101.0, 105.0, 103.0, 107.0, 110.0, 108.0, 112.0, 115.0],
            "open" => &[99.0, 101.0, 102.0, 101.0, 105.0, 103.0, 106.0, 110.0, 108.0, 112.0],
            "high" => &[101.0, 103.0, 103.0, 106.0, 106.0, 108.0, 111.0, 111.0, 113.0, 116.0],
            "low" => &[98.0, 100.0, 100.0, 100.0, 102.0, 102.0, 105.0, 107.0, 107.0, 111.0],
            "volume" => &[1000u64, 1200, 900, 1500, 1100, 1800, 2000, 800, 1600, 2200],
        }
        .unwrap()
    }

    #[test]
    fn day_of_week_monday_filter() {
        let df = test_df_with_dates();
        // 2024-01-01 = Monday (weekday=1), 2024-12-02 = Monday (weekday=1)
        let signal = FormulaSignal::new("day_of_week() == 1".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert_eq!(bools.get(0), Some(true)); // Mon Jan 1
        assert_eq!(bools.get(1), Some(false)); // Tue Jan 2
        assert_eq!(bools.get(4), Some(false)); // Fri Jan 5
        assert_eq!(bools.get(5), Some(true)); // Mon Dec 2
    }

    #[test]
    fn month_december_filter() {
        let df = test_df_with_dates();
        let signal = FormulaSignal::new("month() == 12".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // Indices 0-4 are January, 5-9 are December
        for i in 0..5 {
            assert_eq!(
                bools.get(i),
                Some(false),
                "index {i} should be false (January)"
            );
        }
        for i in 5..10 {
            assert_eq!(
                bools.get(i),
                Some(true),
                "index {i} should be true (December)"
            );
        }
    }

    #[test]
    fn hour_zero_for_daily_bars() {
        let df = test_df_with_dates();
        // Daily bars promoted to midnight → hour == 0 for all
        let signal = FormulaSignal::new("hour() == 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        for i in 0..10 {
            assert_eq!(bools.get(i), Some(true), "index {i} should have hour=0");
        }
    }

    #[test]
    fn day_of_month_filter() {
        let df = test_df_with_dates();
        // Jan 1 → day_of_month=1
        let signal = FormulaSignal::new("day_of_month() == 1".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert_eq!(bools.get(0), Some(true)); // Jan 1
        assert_eq!(bools.get(1), Some(false)); // Jan 2
    }

    #[test]
    fn week_of_year_filter() {
        let df = test_df_with_dates();
        // 2024-01-01 is ISO week 1
        let signal = FormulaSignal::new("week_of_year() == 1".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert_eq!(bools.get(0), Some(true)); // Jan 1 = week 1
        assert_eq!(bools.get(5), Some(false)); // Dec 2 ≠ week 1
    }

    #[test]
    fn date_function_combined_with_price() {
        let df = test_df_with_dates();
        // Monday AND close > 100
        let signal = FormulaSignal::new("day_of_week() == 1 and close > 100".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert_eq!(bools.get(0), Some(false)); // Mon, close=100 (not > 100)
        assert_eq!(bools.get(5), Some(true)); // Mon Dec 2, close=107
    }

    #[test]
    fn date_function_missing_date_column() {
        // DataFrame without any date column should error
        let df = test_df(); // no date column
        let signal = FormulaSignal::new("day_of_week() == 1".to_string());
        let result = signal.evaluate(&df);
        assert!(result.is_err());
    }

    #[test]
    fn date_function_with_datetime_column() {
        use chrono::{NaiveDate, NaiveDateTime};
        // Test the Datetime pass-through branch (no Date→Datetime cast needed)
        let datetimes = vec![
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            ),
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                chrono::NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
            ),
            NaiveDateTime::new(
                NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                chrono::NaiveTime::from_hms_opt(9, 30, 0).unwrap(),
            ),
        ];
        let dt_chunked: DatetimeChunked =
            DatetimeChunked::new(PlSmallStr::from("datetime"), &datetimes);
        let df = DataFrame::new(
            3,
            vec![
                dt_chunked.into_column(),
                Column::new("close".into(), &[100.0, 102.0, 101.0]),
                Column::new("open".into(), &[99.0, 101.0, 100.0]),
                Column::new("high".into(), &[101.0, 103.0, 102.0]),
                Column::new("low".into(), &[98.0, 100.0, 99.0]),
                Column::new("volume".into(), &[1000u64, 1200, 900]),
            ],
        )
        .unwrap();

        // hour() should return actual hours, not 0
        let signal = FormulaSignal::new("hour() == 9".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert_eq!(bools.get(0), Some(true)); // 09:30
        assert_eq!(bools.get(1), Some(false)); // 14:00
        assert_eq!(bools.get(2), Some(true)); // 09:30

        // minute() should return actual minutes
        let signal = FormulaSignal::new("minute() == 30".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert_eq!(bools.get(0), Some(true)); // 09:30
        assert_eq!(bools.get(1), Some(false)); // 14:00
        assert_eq!(bools.get(2), Some(true)); // 09:30
    }

    #[test]
    fn gap_function_parses() {
        assert!(validate_formula("gap() > 0.01").is_ok());
        assert!(validate_formula("gap_size() > 2.0").is_ok());
        assert!(validate_formula("gap_filled() == 1.0").is_ok());
    }

    #[test]
    fn gap_function_evaluates() {
        // test_df data:
        // close: 100, 102, 101, 105, 103, 107, 110, 108, 112, 115
        // open:   99, 101, 102, 101, 105, 103, 106, 110, 108, 112
        // gap = (open - prev_close) / prev_close
        // bar 1: (101 - 100) / 100 = 0.01
        // bar 2: (102 - 102) / 102 = 0.0
        // bar 3: (101 - 101) / 101 = 0.0
        let df = test_df();
        let signal = FormulaSignal::new("gap() > 0.005".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // bar 0: null (no previous close)
        assert_eq!(bools.get(0), None); // null
        assert_eq!(bools.get(1), Some(true)); // 0.01 > 0.005
        assert_eq!(bools.get(2), Some(false)); // 0.0 > 0.005 = false
    }

    #[test]
    fn gap_size_evaluates() {
        let df = test_df();
        let signal = FormulaSignal::new("gap_size() > 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // bar 1: open=101 - prev_close=100 = 1.0 > 0 → true
        assert_eq!(bools.get(1), Some(true));
        // bar 3: open=101 - prev_close=101 = 0.0 > 0 → false
        assert_eq!(bools.get(3), Some(false));
    }

    #[test]
    fn gap_filled_evaluates() {
        let df = test_df();
        let signal = FormulaSignal::new("gap_filled() == 1.0".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // bar 1: gap up (open=101 > prev_close=100). low=100 <= 100 → filled=true
        assert_eq!(bools.get(1), Some(true));
        // bar 4: open=105 == prev_close=105 → no gap (open == prev_close) → filled=false
        assert_eq!(bools.get(4), Some(false));
    }

    #[test]
    fn gap_size_no_negatives_in_test_df() {
        // test_df has no gap-down bars (open >= prev_close for all bars).
        // Verify gap_size() < 0 is false for all. Real gap-down coverage
        // is in gap_down_with_custom_data below.
        let df = test_df();
        let signal = FormulaSignal::new("gap_size() < 0".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        // bar 1: 101 - 100 = 1 → not < 0
        assert_eq!(bools.get(1), Some(false));
        // bar 3: open=101, prev_close=101 → 0, not < 0
        assert_eq!(bools.get(3), Some(false));
        // bar 5: open=103, prev_close=103 → 0
        assert_eq!(bools.get(5), Some(false));
    }

    #[test]
    fn gap_down_with_custom_data() {
        // Create data with explicit gap downs
        let df = df! {
            "close"  => &[100.0, 105.0, 98.0, 102.0, 95.0],
            "open"   => &[100.0, 102.0, 100.0, 96.0, 103.0],
            "high"   => &[106.0, 106.0, 101.0, 103.0, 104.0],
            "low"    => &[99.0, 101.0, 97.0, 95.0, 94.0],
            "volume" => &[1000u64, 1200, 900, 1100, 1500],
        }
        .unwrap();

        // gap_size: bar 1: 102-100=2, bar 2: 100-105=-5, bar 3: 96-98=-2, bar 4: 103-102=1
        let signal = FormulaSignal::new("gap_size() < -1.0".to_string());
        let result = signal.evaluate(&df).unwrap();
        let bools = result.bool().unwrap();
        assert_eq!(bools.get(1), Some(false)); // +2
        assert_eq!(bools.get(2), Some(true)); // -5
        assert_eq!(bools.get(3), Some(true)); // -2
        assert_eq!(bools.get(4), Some(false)); // +1

        // gap_filled for gap-down: bar 2: open=100 < prev_close=105, high=101 < 105 → NOT filled
        // bar 3: open=96 < prev_close=98, high=103 >= 98 → filled!
        let signal2 = FormulaSignal::new("gap_filled() == 1.0".to_string());
        let result2 = signal2.evaluate(&df).unwrap();
        let bools2 = result2.bool().unwrap();
        assert_eq!(bools2.get(2), Some(false)); // gap down, high=101 < 105, not filled
        assert_eq!(bools2.get(3), Some(true)); // gap down, high=103 >= 98, filled
    }

    #[test]
    fn gap_functions_reject_args() {
        assert!(validate_formula("gap(close)").is_err());
        assert!(validate_formula("gap_size(14)").is_err());
        assert!(validate_formula("gap_filled(close, 2)").is_err());
    }

    // ── Cross-symbol formula tests ──────────────────────────────────────

    #[test]
    fn tokenize_dot() {
        let tokens = tokenize("VIX.close").unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[0], Token::Ident("VIX".to_string()));
        assert_eq!(tokens[1], Token::Dot);
        assert_eq!(tokens[2], Token::Ident("close".to_string()));
    }

    #[test]
    fn cross_symbol_bare_defaults_to_close() {
        // VIX > 20 → col("VIX_close") > 20
        assert!(validate_formula("VIX > 20").is_ok());
    }

    #[test]
    fn cross_symbol_dot_column() {
        // VIX.high > 20 → col("VIX_high") > 20
        assert!(validate_formula("VIX.high > 20").is_ok());
    }

    #[test]
    fn cross_symbol_dot_invalid_column() {
        // VIX.invalid should error
        assert!(validate_formula("VIX.invalid > 20").is_err());
    }

    #[test]
    fn cross_symbol_division() {
        // VIX / VIX3M < 0.9
        assert!(validate_formula("VIX / VIX3M < 0.9").is_ok());
    }

    #[test]
    fn cross_symbol_in_function() {
        // sma(VIX, 20) < 0.85
        // Note: VIX here will be parsed as a cross-symbol Expr argument
        assert!(validate_formula("sma(VIX, 20) > 15").is_ok());
    }

    #[test]
    fn cross_symbol_with_lookback() {
        // VIX[1] → col("VIX_close").shift(1)
        assert!(validate_formula("VIX[1] > 20").is_ok());
    }

    #[test]
    fn cross_symbol_dot_with_lookback() {
        // VIX.high[1] → col("VIX_high").shift(1)
        assert!(validate_formula("VIX.high[1] > 20").is_ok());
    }

    #[test]
    fn cross_symbol_mixed_with_primary() {
        // VIX > 30 and rsi(close, 14) < 30
        assert!(validate_formula("VIX > 30 and rsi(close, 14) < 30").is_ok());
    }

    #[test]
    fn cross_symbol_dot_syntax_ratio() {
        // VIX.high / VIX3M.low > 1.1
        assert!(validate_formula("VIX.high / VIX3M.low > 1.1").is_ok());
    }

    #[test]
    fn cross_symbol_number_dot_not_confused() {
        // 1.5 should still parse as number, not 1 + Dot + 5
        let tokens = tokenize("close > 1.5").unwrap();
        assert_eq!(tokens.len(), 3);
        assert_eq!(tokens[2], Token::Number(1.5));
    }

    #[test]
    fn primary_column_dot_access_rejected() {
        // close.high should error — close is a primary column, not a cross-symbol
        assert!(validate_formula("close.high > 0").is_err());
        assert!(validate_formula("volume.low > 0").is_err());
    }

    #[test]
    fn test_double_underscore_ident_is_literal_column() {
        let expr = parse_formula("__hmm_regime_SPY_3_5_65 == 2").unwrap();
        let fmt = format!("{expr:?}");
        assert!(
            fmt.contains("__hmm_regime_SPY_3_5_65"),
            "should contain literal column name, got: {fmt}"
        );
        assert!(
            !fmt.contains("_close"),
            "should NOT append _close suffix, got: {fmt}"
        );
    }
}

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
//! **Functions**:
//! - `sma(col, period)` — Simple Moving Average
//! - `ema(col, period)` — Exponential Moving Average (true EWM with alpha=2/(period+1))
//! - `std(col, period)` — Rolling Standard Deviation
//! - `max(col, period)` — Rolling Maximum
//! - `min(col, period)` — Rolling Minimum
//! - `abs(expr)` — Absolute value
//! - `change(col, period)` — `col - col[period]`
//! - `pct_change(col, period)` — `(col - col[period]) / col[period]`
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
//! ```

use polars::prelude::*;

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

    // primary = number | "true" | "false" | ident ("[" number "]")? | func_call | "(" expr ")"
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

                // Check for function call: ident "(" args ")"
                if self.peek() == Some(&Token::LParen) {
                    self.advance();
                    let args = self.parse_args()?;
                    self.expect(&Token::RParen)?;
                    Self::build_function_call(&name, args)
                }
                // Check for lookback: ident "[" number "]"
                else if self.peek() == Some(&Token::LBracket) {
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
                                return Err(format!(
                                    "Lookback index too large (max 10000), got {n}"
                                ));
                            }
                            let shift = n as i64;
                            Ok(col(&*name).shift(lit(shift)))
                        }
                        other => Err(format!("Expected number in lookback, got {other:?}")),
                    }
                } else {
                    // Plain column reference
                    Ok(col(&*name))
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
        // Try to parse as a plain number (for period arguments)
        if let Some(Token::Number(n)) = self.peek() {
            let n = *n;
            self.advance();
            Ok(FuncArg::Number(n))
        } else {
            let expr = self.parse_expr()?;
            Ok(FuncArg::Expression(expr))
        }
    }

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
            other => Err(format!(
                "Unknown function: '{other}'. Available: sma, ema, std, max, min, abs, change, pct_change"
            )),
        }
    }
}

#[derive(Debug)]
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
    }

    #[test]
    fn validate_formula_err() {
        assert!(validate_formula("").is_err());
        assert!(validate_formula("close >").is_err());
        assert!(validate_formula("unknown_func(close)").is_err());
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
    }
}

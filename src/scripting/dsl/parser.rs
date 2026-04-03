//! DSL parser: converts indent-based natural-language trading scripts into an IR.
//!
//! The parser is line-oriented with Python-style indentation for block structure.
//! Each line is classified by its leading keyword, and indented lines belong to
//! the nearest preceding block header at a lower indent level.

use super::error::DslError;

// ---------------------------------------------------------------------------
// Raw line representation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Line {
    indent: usize,
    content: String,
    num: usize,
}

// ---------------------------------------------------------------------------
// AST / IR types
// ---------------------------------------------------------------------------

/// A complete parsed DSL program.
#[derive(Debug)]
pub struct DslProgram {
    pub strategy: Option<StrategyBlock>,
    pub params: Vec<ParamDecl>,
    pub states: Vec<StateDecl>,
    pub on_bar: Option<Vec<Stmt>>,
    pub on_exit_check: Option<Vec<Stmt>>,
    pub on_position_opened: Option<Vec<Stmt>>,
    pub on_position_closed: Option<Vec<Stmt>>,
    pub on_end: Option<Vec<Stmt>>,
    /// Procedural mode: bare statements that become the on_bar body.
    pub body: Vec<Stmt>,
}

/// The `strategy` configuration block.
#[derive(Debug)]
pub struct StrategyBlock {
    pub name: String,
    pub symbol: String,
    pub capital: String,
    pub interval: String,
    pub data_ohlcv: bool,
    pub data_options: bool,
    pub indicators: Vec<String>,
    pub slippage: Option<String>,
    pub expiration_filter: Option<String>,
    pub max_positions: Option<i64>,
    pub cross_symbols: Vec<String>,
    pub procedural: bool,
    pub category: Option<String>,
    pub description: Option<String>,
    pub hypothesis: Option<String>,
    pub tags: Vec<String>,
    pub regime: Vec<String>,
}

/// A `param` declaration with default value and description.
#[derive(Debug)]
pub struct ParamDecl {
    pub name: String,
    pub default: String,
    pub description: String,
    pub choices: Vec<String>,
}

/// A `state` variable declaration with initial value.
#[derive(Debug)]
pub struct StateDecl {
    pub name: String,
    pub default: String,
}

/// Order type modifier for buy/sell statements.
#[derive(Debug, Clone)]
pub enum OrderModifier {
    /// Market order (default) — fills at next bar's open.
    Market,
    /// Limit order — fills if price reaches the limit.
    Limit { price: String },
    /// Stop order — fills if price reaches the stop.
    Stop { price: String },
}

/// Exit modifiers attached to a Buy/Sell order.
#[derive(Debug, Default)]
pub struct ExitModifiers {
    pub stop_loss: Option<OrderExitSpec>,
    pub profit_target: Option<OrderExitSpec>,
    pub trailing_stop: Option<OrderExitSpec>,
}

/// A single exit specification on an order.
#[derive(Debug, Clone)]
pub enum OrderExitSpec {
    Percent(f64), // 5% stored as 0.05
    Dollar(f64),  // $500
}

/// Quantifier type for `when any/all` statements.
#[derive(Debug, Clone, Copy)]
pub enum Quantifier {
    Any,
    All,
}

/// A statement inside an event block.
#[derive(Debug)]
pub enum Stmt {
    Require {
        indicators: Vec<String>,
        line: usize,
    },
    SkipWhen {
        condition: String,
        line: usize,
    },
    Set {
        name: String,
        expr: String,
        line: usize,
    },
    When {
        condition: String,
        then_body: Vec<Stmt>,
        else_body: Option<Vec<Stmt>>,
        line: usize,
    },
    Buy {
        qty_expr: String,
        order_type: OrderModifier,
        exit_modifiers: ExitModifiers,
        line: usize,
    },
    Sell {
        qty_expr: String,
        order_type: OrderModifier,
        exit_modifiers: ExitModifiers,
        line: usize,
    },
    CancelOrders {
        signal: Option<String>,
        line: usize,
    },
    HoldPosition {
        line: usize,
    },
    ClosePosition {
        reason: String,
        line: usize,
    },
    ClosePositionById {
        id_expr: String,
        reason: String,
        line: usize,
    },
    StopBacktest {
        reason: String,
        line: usize,
    },
    OpenStrategy {
        call: String,
        line: usize,
    },
    Plot {
        name: String,
        expr: String,
        display: Option<String>,
        line: usize,
    },
    AddTo {
        expr: String,
        name: String,
        line: usize,
    },
    SubtractFrom {
        expr: String,
        name: String,
        line: usize,
    },
    MultiplyBy {
        name: String,
        expr: String,
        line: usize,
    },
    DivideBy {
        name: String,
        expr: String,
        line: usize,
    },
    ForEach {
        var: String,
        iterable: String,
        body: Vec<Stmt>,
        line: usize,
    },
    Return {
        expr: String,
        line: usize,
    },
    Raw {
        code: String,
        line: usize,
    },
    TryOpen {
        call: String,
        var_name: String,
        body: Vec<Stmt>,
        line: usize,
    },
    WhenAnyAll {
        quantifier: Quantifier,
        binding_var: String,
        iterable: String,
        condition: String,
        capture_as: Option<String>,
        then_body: Vec<Stmt>,
        else_body: Option<Vec<Stmt>>,
        line: usize,
    },
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Parse a DSL source string into a `DslProgram`.
pub fn parse(source: &str) -> Result<DslProgram, DslError> {
    let lines = preprocess(source);

    let mut program = DslProgram {
        strategy: None,
        params: vec![],
        states: vec![],
        on_bar: None,
        on_exit_check: None,
        on_position_opened: None,
        on_position_closed: None,
        on_end: None,
        body: vec![],
    };

    let mut i = 0;
    while i < lines.len() {
        let line = &lines[i];
        if line.indent > 0 {
            return Err(DslError::new(
                line.num,
                "unexpected indentation at top level",
            ));
        }

        let content = &line.content;

        if content.starts_with("strategy ") {
            let (block, next) = parse_strategy_block(&lines, i)?;
            if program.strategy.is_some() {
                return Err(DslError::new(line.num, "duplicate strategy block"));
            }
            program.strategy = Some(block);
            i = next;
        } else if content.starts_with("extern ") {
            program.params.push(parse_extern(line)?);
            i += 1;
        } else if content.starts_with("state ") {
            program.states.push(parse_state(line)?);
            i += 1;
        } else if content == "on each bar" {
            if program.on_bar.is_some() {
                return Err(DslError::new(line.num, "duplicate 'on each bar' block"));
            }
            let (body, next) = parse_indented_body(&lines, i)?;
            program.on_bar = Some(parse_statements(&body)?);
            i = next;
        } else if content == "on exit check" {
            if program.on_exit_check.is_some() {
                return Err(DslError::new(line.num, "duplicate 'on exit check' block"));
            }
            let (body, next) = parse_indented_body(&lines, i)?;
            program.on_exit_check = Some(parse_statements(&body)?);
            i = next;
        } else if content == "on position opened" {
            if program.on_position_opened.is_some() {
                return Err(DslError::new(
                    line.num,
                    "duplicate 'on position opened' block",
                ));
            }
            let (body, next) = parse_indented_body(&lines, i)?;
            program.on_position_opened = Some(parse_statements(&body)?);
            i = next;
        } else if content == "on position closed" {
            if program.on_position_closed.is_some() {
                return Err(DslError::new(
                    line.num,
                    "duplicate 'on position closed' block",
                ));
            }
            let (body, next) = parse_indented_body(&lines, i)?;
            program.on_position_closed = Some(parse_statements(&body)?);
            i = next;
        } else if content == "on end" {
            if program.on_end.is_some() {
                return Err(DslError::new(line.num, "duplicate 'on end' block"));
            }
            let (body, next) = parse_indented_body(&lines, i)?;
            program.on_end = Some(parse_statements(&body)?);
            i = next;
        } else {
            // In procedural mode, remaining top-level lines become the body
            if program.strategy.as_ref().is_some_and(|s| s.procedural) {
                let remaining: Vec<Line> = lines[i..].to_vec();
                program.body = parse_statements(&remaining)?;
                break;
            }
            return Err(DslError::new(
                line.num,
                format!("unrecognized top-level declaration: {content}"),
            ));
        }
    }

    if program.strategy.is_none() {
        return Err(DslError::general("missing required 'strategy' block"));
    }

    let is_procedural = program.strategy.as_ref().is_some_and(|s| s.procedural);

    if is_procedural
        && (program.on_bar.is_some()
            || program.on_exit_check.is_some()
            || program.on_position_opened.is_some()
            || program.on_position_closed.is_some()
            || program.on_end.is_some())
    {
        return Err(DslError::general(
            "procedural mode cannot have event blocks (on each bar, on exit check, etc.)",
        ));
    }

    if is_procedural && program.body.is_empty() {
        return Err(DslError::general(
            "procedural strategy has no body statements",
        ));
    }

    Ok(program)
}

// ---------------------------------------------------------------------------
// Preprocessing
// ---------------------------------------------------------------------------

fn preprocess(source: &str) -> Vec<Line> {
    source
        .lines()
        .enumerate()
        .filter_map(|(i, raw)| {
            let trimmed = raw.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                None
            } else {
                let indent = raw.len() - raw.trim_start().len();
                Some(Line {
                    indent,
                    content: trimmed.to_string(),
                    num: i + 1,
                })
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Strategy block parsing
// ---------------------------------------------------------------------------

fn parse_strategy_block(lines: &[Line], start: usize) -> Result<(StrategyBlock, usize), DslError> {
    let header = &lines[start];
    let name = extract_quoted_string(&header.content, "strategy ", header.num)?;
    let after_name_pos = header.content.find(&format!("\"{name}\"")).unwrap() + name.len() + 2;
    let after_name = header.content[after_name_pos..].trim();
    let procedural = after_name == "procedural";

    let mut block = StrategyBlock {
        name,
        symbol: "params.SYMBOL".to_string(),
        capital: "params.CAPITAL".to_string(),
        interval: "daily".to_string(),
        data_ohlcv: true,
        data_options: false,
        indicators: vec![],
        slippage: None,
        expiration_filter: None,
        max_positions: None,
        cross_symbols: vec![],
        procedural,
        category: None,
        description: None,
        hypothesis: None,
        tags: vec![],
        regime: vec![],
    };

    let mut i = start + 1;
    while i < lines.len() && lines[i].indent > header.indent {
        let line = &lines[i];
        let content = &line.content;

        if let Some(rest) = content.strip_prefix("symbol ") {
            block.symbol = rest.trim().to_string();
        } else if let Some(rest) = content.strip_prefix("capital ") {
            block.capital = rest.trim().to_string();
        } else if let Some(rest) = content.strip_prefix("interval ") {
            block.interval = rest.trim().to_string();
        } else if let Some(rest) = content.strip_prefix("data ") {
            let parts: Vec<&str> = rest.split(',').map(|s| s.trim()).collect();
            block.data_ohlcv = parts.contains(&"ohlcv");
            block.data_options = parts.contains(&"options");
        } else if let Some(rest) = content.strip_prefix("indicators ") {
            block.indicators = rest.split(',').map(|s| s.trim().to_string()).collect();
        } else if let Some(rest) = content.strip_prefix("slippage ") {
            block.slippage = Some(rest.trim().to_string());
        } else if let Some(rest) = content.strip_prefix("expiration_filter ") {
            block.expiration_filter = Some(rest.trim().to_string());
        } else if let Some(rest) = content.strip_prefix("max_positions ") {
            block.max_positions = Some(
                rest.trim()
                    .parse::<i64>()
                    .map_err(|_| DslError::new(line.num, "max_positions must be an integer"))?,
            );
        } else if let Some(rest) = content.strip_prefix("cross_symbols ") {
            block.cross_symbols = rest.split(',').map(|s| s.trim().to_string()).collect();
        } else if let Some(rest) = content.strip_prefix("category ") {
            block.category = Some(rest.trim().to_string());
        } else if let Some(rest) = content.strip_prefix("description ") {
            block.description = Some(extract_quoted_value(rest.trim(), line.num)?);
        } else if let Some(rest) = content.strip_prefix("hypothesis ") {
            block.hypothesis = Some(extract_quoted_value(rest.trim(), line.num)?);
        } else if let Some(rest) = content.strip_prefix("tags ") {
            block.tags = rest.split(',').map(|s| s.trim().to_string()).collect();
        } else if let Some(rest) = content.strip_prefix("regime ") {
            block.regime = rest.split(',').map(|s| s.trim().to_string()).collect();
        } else {
            return Err(DslError::new(
                line.num,
                format!("unknown strategy property: {content}"),
            ));
        }

        i += 1;
    }

    Ok((block, i))
}

pub(crate) fn parse_exit_threshold_dsl(
    s: &str,
    line_num: usize,
) -> Result<(String, f64), DslError> {
    if let Some(pct_str) = s.strip_suffix('%') {
        let value = pct_str
            .trim()
            .parse::<f64>()
            .map_err(|_| DslError::new(line_num, format!("invalid percentage: {s}")))?;
        if value <= 0.0 {
            return Err(DslError::new(
                line_num,
                "exit threshold percentage must be positive",
            ));
        }
        Ok(("percent".to_string(), value / 100.0))
    } else if let Some(dollar_str) = s.strip_prefix('$') {
        let value = dollar_str
            .trim()
            .parse::<f64>()
            .map_err(|_| DslError::new(line_num, format!("invalid dollar amount: {s}")))?;
        if value <= 0.0 {
            return Err(DslError::new(
                line_num,
                "exit threshold dollar amount must be positive",
            ));
        }
        Ok(("dollar".to_string(), value))
    } else {
        Err(DslError::new(
            line_num,
            format!("exit threshold must be N% or $N, got: {s}"),
        ))
    }
}

/// Parse optional indented exit modifiers after a Buy/Sell line.
/// Returns (modifiers, count of consumed lines).
fn parse_exit_modifiers(
    lines: &[Line],
    buy_idx: usize,
) -> Result<(ExitModifiers, usize), DslError> {
    let buy_indent = lines[buy_idx].indent;
    let mut mods = ExitModifiers::default();
    let mut consumed = 0;
    let mut j = buy_idx + 1;
    while j < lines.len() && lines[j].indent > buy_indent {
        let content = &lines[j].content;
        if let Some(rest) = content.strip_prefix("stop_loss ") {
            let (mode, value) = parse_exit_threshold_dsl(rest.trim(), lines[j].num)?;
            mods.stop_loss = Some(match mode.as_str() {
                "percent" => OrderExitSpec::Percent(value),
                _ => OrderExitSpec::Dollar(value),
            });
            consumed += 1;
        } else if let Some(rest) = content.strip_prefix("profit_target ") {
            let (mode, value) = parse_exit_threshold_dsl(rest.trim(), lines[j].num)?;
            mods.profit_target = Some(match mode.as_str() {
                "percent" => OrderExitSpec::Percent(value),
                _ => OrderExitSpec::Dollar(value),
            });
            consumed += 1;
        } else if let Some(rest) = content.strip_prefix("trailing_stop ") {
            let (mode, value) = parse_exit_threshold_dsl(rest.trim(), lines[j].num)?;
            mods.trailing_stop = Some(match mode.as_str() {
                "percent" => OrderExitSpec::Percent(value),
                _ => OrderExitSpec::Dollar(value),
            });
            consumed += 1;
        } else {
            break;
        }
        j += 1;
    }
    Ok((mods, consumed))
}

// ---------------------------------------------------------------------------
// Extern and state declarations
// ---------------------------------------------------------------------------

fn parse_extern(line: &Line) -> Result<ParamDecl, DslError> {
    // extern NAME = DEFAULT "description"
    // extern NAME = DEFAULT "description" choices VAL1, VAL2
    let rest = line.content.strip_prefix("extern ").unwrap();

    let eq_pos = rest.find('=').ok_or_else(|| {
        DslError::new(
            line.num,
            "extern requires '=' (e.g., extern NAME = 42 \"desc\")",
        )
    })?;

    let name = rest[..eq_pos].trim().to_string();
    let after_eq = rest[eq_pos + 1..].trim();

    // Parse default value, which may itself be a quoted string
    let (default, desc_start) = if let Some(after_open_quote) = after_eq.strip_prefix('"') {
        // Quoted default: find closing quote
        let close = after_open_quote
            .find('"')
            .ok_or_else(|| DslError::new(line.num, "unterminated default string"))?;
        let val = after_eq[..close + 2].to_string(); // include quotes
        (val, close + 2)
    } else {
        // Unquoted default: everything up to the first quote (description)
        let quote_pos = after_eq
            .find('"')
            .ok_or_else(|| DslError::new(line.num, "extern requires a quoted description"))?;
        let val = after_eq[..quote_pos].trim().to_string();
        (val, quote_pos)
    };

    // Parse the description string
    let desc_region = after_eq[desc_start..].trim();
    if !desc_region.starts_with('"') {
        return Err(DslError::new(
            line.num,
            "extern requires a quoted description after the default value",
        ));
    }
    let desc_inner = &desc_region[1..];
    let desc_end = desc_inner
        .find('"')
        .ok_or_else(|| DslError::new(line.num, "unterminated description string"))?;
    let description = desc_inner[..desc_end].to_string();

    // Check for choices after closing quote
    let remainder = desc_inner[desc_end + 1..].trim();
    let choices = if let Some(choices_str) = remainder.strip_prefix("choices ") {
        choices_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    } else {
        vec![]
    };

    if name.is_empty() || default.is_empty() {
        return Err(DslError::new(
            line.num,
            "extern requires NAME = DEFAULT \"description\"",
        ));
    }

    Ok(ParamDecl {
        name,
        default,
        description,
        choices,
    })
}

fn parse_state(line: &Line) -> Result<StateDecl, DslError> {
    // state NAME = DEFAULT
    let rest = line.content.strip_prefix("state ").unwrap();

    let eq_pos = rest
        .find('=')
        .ok_or_else(|| DslError::new(line.num, "state requires '=' (e.g., state wins = 0)"))?;

    let name = rest[..eq_pos].trim().to_string();
    let default = rest[eq_pos + 1..].trim().to_string();

    if name.is_empty() || default.is_empty() {
        return Err(DslError::new(line.num, "state requires NAME = DEFAULT"));
    }

    Ok(StateDecl { name, default })
}

// ---------------------------------------------------------------------------
// Indented body extraction
// ---------------------------------------------------------------------------

/// Extract all lines indented deeper than `lines[start]`, returning them and
/// the index of the next line at the same or lower indent level.
fn parse_indented_body(lines: &[Line], start: usize) -> Result<(Vec<Line>, usize), DslError> {
    let base_indent = lines[start].indent;
    let mut body = vec![];
    let mut i = start + 1;

    while i < lines.len() && lines[i].indent > base_indent {
        body.push(lines[i].clone());
        i += 1;
    }

    if body.is_empty() {
        return Err(DslError::new(
            lines[start].num,
            "block has no indented body",
        ));
    }

    Ok((body, i))
}

// ---------------------------------------------------------------------------
// Statement parsing (recursive for when/otherwise nesting)
// ---------------------------------------------------------------------------

fn parse_statements(lines: &[Line]) -> Result<Vec<Stmt>, DslError> {
    let mut stmts = vec![];
    let mut i = 0;

    while i < lines.len() {
        let line = &lines[i];
        let content = &line.content;

        if let Some(rest) = content.strip_prefix("require ") {
            stmts.push(Stmt::Require {
                indicators: rest.split(',').map(|s| s.trim().to_string()).collect(),
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("skip when ") {
            stmts.push(Stmt::SkipWhen {
                condition: rest.to_string(),
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("set ") {
            let (name, expr) = parse_set_statement(rest, line.num)?;
            stmts.push(Stmt::Set {
                name,
                expr,
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("when any ") {
            let (stmt, next) = parse_when_any_all(lines, i, Quantifier::Any, rest)?;
            stmts.push(stmt);
            i = next;
        } else if let Some(rest) = content.strip_prefix("when all ") {
            let (stmt, next) = parse_when_any_all(lines, i, Quantifier::All, rest)?;
            stmts.push(stmt);
            i = next;
        } else if let Some(rest) = content.strip_prefix("when ") {
            let (stmt, next) = parse_when_chain(lines, i, rest)?;
            stmts.push(stmt);
            i = next;
        } else if content == "otherwise" {
            // Stray otherwise without a preceding when — error
            return Err(DslError::new(
                line.num,
                "'otherwise' without a preceding 'when'",
            ));
        } else if let Some(rest) = strip_prefix_ci(content, "Buy ") {
            let (qty_expr, order_type) = parse_order_statement(rest, line.num)?;
            let (exit_modifiers, consumed) = parse_exit_modifiers(lines, i)?;
            stmts.push(Stmt::Buy {
                qty_expr,
                order_type,
                exit_modifiers,
                line: line.num,
            });
            i += 1 + consumed;
        } else if let Some(rest) = strip_prefix_ci(content, "Sell ") {
            let (qty_expr, order_type) = parse_order_statement(rest, line.num)?;
            let (exit_modifiers, consumed) = parse_exit_modifiers(lines, i)?;
            stmts.push(Stmt::Sell {
                qty_expr,
                order_type,
                exit_modifiers,
                line: line.num,
            });
            i += 1 + consumed;
        } else if content == "cancel all orders" || content == "Cancel all orders" {
            stmts.push(Stmt::CancelOrders {
                signal: None,
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("cancel orders ") {
            let signal = extract_quoted_value(rest, line.num)?;
            stmts.push(Stmt::CancelOrders {
                signal: Some(signal),
                line: line.num,
            });
            i += 1;
        } else if content == "hold position" {
            stmts.push(Stmt::HoldPosition { line: line.num });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("close position ") {
            // close position ID "reason" or close position "reason"
            if rest.starts_with('"') {
                let reason = extract_quoted_value(rest, line.num)?;
                stmts.push(Stmt::ClosePosition {
                    reason,
                    line: line.num,
                });
            } else {
                // close position EXPR "reason"
                let quote_pos = rest.find('"').ok_or_else(|| {
                    DslError::new(line.num, "close position requires a quoted reason")
                })?;
                let id_expr = rest[..quote_pos].trim().to_string();
                let reason = extract_quoted_value(&rest[quote_pos..], line.num)?;
                stmts.push(Stmt::ClosePositionById {
                    id_expr,
                    reason,
                    line: line.num,
                });
            }
            i += 1;
        } else if let Some(rest) = content.strip_prefix("stop backtest ") {
            let reason = extract_quoted_value(rest, line.num)?;
            stmts.push(Stmt::StopBacktest {
                reason,
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("try open ") {
            let (call, var_name) = parse_try_open_header(rest, line.num)?;
            let (body_lines, next) = parse_indented_body(lines, i)?;
            let body = parse_statements(&body_lines)?;
            stmts.push(Stmt::TryOpen {
                call,
                var_name,
                body,
                line: line.num,
            });
            i = next;
        } else if let Some(rest) = content.strip_prefix("open ") {
            stmts.push(Stmt::OpenStrategy {
                call: rest.to_string(),
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("plot ") {
            let (name, expr, display) = parse_plot(rest, line.num)?;
            stmts.push(Stmt::Plot {
                name,
                expr,
                display,
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("add ") {
            let (expr, name) = parse_add_to(rest, line.num)?;
            stmts.push(Stmt::AddTo {
                expr,
                name,
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("subtract ") {
            let (expr, name) = parse_subtract_from(rest, line.num)?;
            stmts.push(Stmt::SubtractFrom {
                expr,
                name,
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("multiply ") {
            let (name, expr) = parse_multiply_by(rest, line.num)?;
            stmts.push(Stmt::MultiplyBy {
                name,
                expr,
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("divide ") {
            let (name, expr) = parse_divide_by(rest, line.num)?;
            stmts.push(Stmt::DivideBy {
                name,
                expr,
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("for each ") {
            let (var, iterable) = parse_for_each_header(rest, line.num)?;
            let (body_lines, next) = parse_indented_body(lines, i)?;
            let body = parse_statements(&body_lines)?;
            stmts.push(Stmt::ForEach {
                var,
                iterable,
                body,
                line: line.num,
            });
            i = next;
        } else if let Some(rest) = content.strip_prefix("return ") {
            stmts.push(Stmt::Return {
                expr: rest.to_string(),
                line: line.num,
            });
            i += 1;
        } else if let Some(rest) = content.strip_prefix("raw ") {
            stmts.push(Stmt::Raw {
                code: rest.to_string(),
                line: line.num,
            });
            i += 1;
        } else {
            return Err(DslError::new(
                line.num,
                format!("unrecognized statement: {content}"),
            ));
        }
    }

    Ok(stmts)
}

// ---------------------------------------------------------------------------
// When / otherwise chain parsing
// ---------------------------------------------------------------------------

/// Check if an `otherwise` block exists among sibling lines at `base_indent`,
/// skipping over `when ... then` blocks and their indented bodies.
fn has_otherwise_in_siblings(lines: &[Line], start: usize, base_indent: usize) -> bool {
    let mut j = start;
    while j < lines.len() {
        if lines[j].indent == base_indent {
            if lines[j].content == "otherwise" {
                return true;
            }
            if !(lines[j].content.starts_with("when ") && lines[j].content.ends_with(" then")) {
                // Non-when/otherwise sibling — stop scanning
                return false;
            }
        }
        j += 1;
    }
    false
}

/// Parse `when any/all VAR in ITERABLE has/have CONDITION [as CAPTURE] then` block.
fn parse_when_any_all(
    lines: &[Line],
    start: usize,
    quantifier: Quantifier,
    rest: &str,
) -> Result<(Stmt, usize), DslError> {
    let line_num = lines[start].num;
    let base_indent = lines[start].indent;

    // rest must end with " then"
    let rest = rest
        .strip_suffix(" then")
        .ok_or_else(|| DslError::new(line_num, "when any/all clause must end with 'then'"))?;

    // Split on " in "
    let in_pos = rest.find(" in ").ok_or_else(|| {
        DslError::new(
            line_num,
            "expected 'in' after variable name: when any/all VAR in ITERABLE has ...",
        )
    })?;
    let binding_var = rest[..in_pos].trim().to_string();
    let after_in = rest[in_pos + 4..].trim();

    // Split on " has " or " have "
    let (iterable, after_has) = if let Some(pos) = after_in.find(" has ") {
        (&after_in[..pos], after_in[pos + 5..].trim())
    } else if let Some(pos) = after_in.find(" have ") {
        (&after_in[..pos], after_in[pos + 6..].trim())
    } else {
        return Err(DslError::new(
            line_num,
            "expected 'has' or 'have' after iterable: when any/all VAR in ITERABLE has CONDITION then",
        ));
    };
    let iterable = iterable.trim().to_string();

    // Check for " as CAPTURE" at the end
    let (condition, capture_as) = if let Some(as_pos) = after_has.rfind(" as ") {
        let capture = after_has[as_pos + 4..].trim().to_string();
        let cond = after_has[..as_pos].trim().to_string();
        (cond, Some(capture))
    } else {
        (after_has.to_string(), None)
    };

    // Collect the then-body (indented deeper)
    let mut then_body_lines = vec![];
    let mut i = start + 1;
    while i < lines.len() && lines[i].indent > base_indent {
        then_body_lines.push(lines[i].clone());
        i += 1;
    }

    if then_body_lines.is_empty() {
        return Err(DslError::new(
            line_num,
            "when any/all block has no indented body",
        ));
    }

    let then_body = parse_statements(&then_body_lines)?;

    // Check for otherwise
    let else_body =
        if i < lines.len() && lines[i].indent == base_indent && lines[i].content == "otherwise" {
            let mut else_body_lines = vec![];
            i += 1;
            while i < lines.len() && lines[i].indent > base_indent {
                else_body_lines.push(lines[i].clone());
                i += 1;
            }
            if else_body_lines.is_empty() {
                return Err(DslError::new(
                    lines[i - 1].num,
                    "otherwise block has no indented body",
                ));
            }
            Some(parse_statements(&else_body_lines)?)
        } else {
            None
        };

    Ok((
        Stmt::WhenAnyAll {
            quantifier,
            binding_var,
            iterable,
            condition,
            capture_as,
            then_body,
            else_body,
            line: line_num,
        },
        i,
    ))
}

/// Parse a `when CONDITION then` block, consuming any subsequent `when`/`otherwise`
/// siblings at the same indent level to form an if/else-if/else chain.
fn parse_when_chain(
    lines: &[Line],
    start: usize,
    first_cond: &str,
) -> Result<(Stmt, usize), DslError> {
    let base_indent = lines[start].indent;
    let first_line_num = lines[start].num;

    // The condition must end with " then"
    let condition = first_cond
        .strip_suffix(" then")
        .ok_or_else(|| DslError::new(first_line_num, "when clause must end with 'then'"))?
        .to_string();

    // Collect the then-body (indented deeper)
    let mut then_body_lines = vec![];
    let mut i = start + 1;
    while i < lines.len() && lines[i].indent > base_indent {
        then_body_lines.push(lines[i].clone());
        i += 1;
    }

    if then_body_lines.is_empty() {
        return Err(DslError::new(
            first_line_num,
            "when block has no indented body",
        ));
    }

    let then_body = parse_statements(&then_body_lines)?;

    // Check for chained when/otherwise at same indent level.
    // Only chain consecutive `when` blocks into else-if if an `otherwise`
    // eventually appears in the sibling sequence. Without `otherwise`, each
    // `when` block is independent (separate `if` statements).
    let else_body = if i < lines.len() && lines[i].indent == base_indent {
        let next_content = &lines[i].content;

        if next_content == "otherwise" {
            // Collect otherwise body
            let mut otherwise_lines = vec![];
            let j_start = i;
            i += 1;
            while i < lines.len() && lines[i].indent > base_indent {
                otherwise_lines.push(lines[i].clone());
                i += 1;
            }
            if otherwise_lines.is_empty() {
                return Err(DslError::new(
                    lines[j_start].num,
                    "otherwise block has no indented body",
                ));
            }
            Some(parse_statements(&otherwise_lines)?)
        } else if next_content.starts_with("when ") && next_content.ends_with(" then") {
            // Only chain as else-if if an `otherwise` eventually follows
            if has_otherwise_in_siblings(lines, i, base_indent) {
                let rest = next_content.strip_prefix("when ").unwrap();
                let (chained, next_i) = parse_when_chain(lines, i, rest)?;
                i = next_i;
                Some(vec![chained])
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    Ok((
        Stmt::When {
            condition,
            then_body,
            else_body,
            line: first_line_num,
        },
        i,
    ))
}

// ---------------------------------------------------------------------------
// Helpers for specific statement forms
// ---------------------------------------------------------------------------

/// Parse `set NAME to EXPR`
fn parse_set_statement(rest: &str, line_num: usize) -> Result<(String, String), DslError> {
    let to_pos = rest.find(" to ").ok_or_else(|| {
        DslError::new(line_num, "set statement requires 'to' (e.g., set x to 42)")
    })?;

    let name = rest[..to_pos].trim().to_string();
    let expr = rest[to_pos + 4..].trim().to_string();

    if name.is_empty() || expr.is_empty() {
        return Err(DslError::new(
            line_num,
            "set statement requires NAME to EXPR",
        ));
    }

    Ok((name, expr))
}

/// Parse `"name" at EXPR` or `"name" at EXPR as subchart`
fn parse_plot(rest: &str, line_num: usize) -> Result<(String, String, Option<String>), DslError> {
    let name = extract_quoted_value(rest, line_num)?;
    let after_name = rest[name.len() + 2..].trim(); // skip past "name"

    let after_at = after_name.strip_prefix("at ").ok_or_else(|| {
        DslError::new(
            line_num,
            "plot requires 'at' (e.g., plot \"SMA\" at sma(200))",
        )
    })?;

    let (expr, display) = if let Some(as_pos) = after_at.rfind(" as ") {
        let expr = after_at[..as_pos].trim().to_string();
        let display = after_at[as_pos + 4..].trim().to_string();
        (expr, Some(display))
    } else {
        (after_at.trim().to_string(), None)
    };

    Ok((name, expr, display))
}

/// Parse `EXPR to NAME`
fn parse_add_to(rest: &str, line_num: usize) -> Result<(String, String), DslError> {
    let to_pos = rest.rfind(" to ").ok_or_else(|| {
        DslError::new(
            line_num,
            "add statement requires 'to' (e.g., add 1 to counter)",
        )
    })?;

    let expr = rest[..to_pos].trim().to_string();
    let name = rest[to_pos + 4..].trim().to_string();

    if expr.is_empty() || name.is_empty() {
        return Err(DslError::new(
            line_num,
            "add statement requires EXPR to NAME",
        ));
    }

    Ok((expr, name))
}

/// Parse `EXPR from NAME`
fn parse_subtract_from(rest: &str, line_num: usize) -> Result<(String, String), DslError> {
    let from_pos = rest.rfind(" from ").ok_or_else(|| {
        DslError::new(
            line_num,
            "subtract statement requires 'from' (e.g., subtract 1 from counter)",
        )
    })?;

    let expr = rest[..from_pos].trim().to_string();
    let name = rest[from_pos + 6..].trim().to_string();

    if expr.is_empty() || name.is_empty() {
        return Err(DslError::new(
            line_num,
            "subtract statement requires EXPR from NAME",
        ));
    }

    Ok((expr, name))
}

/// Parse `NAME by EXPR`
fn parse_multiply_by(rest: &str, line_num: usize) -> Result<(String, String), DslError> {
    let by_pos = rest.find(" by ").ok_or_else(|| {
        DslError::new(
            line_num,
            "multiply statement requires 'by' (e.g., multiply counter by 2)",
        )
    })?;

    let name = rest[..by_pos].trim().to_string();
    let expr = rest[by_pos + 4..].trim().to_string();

    if name.is_empty() || expr.is_empty() {
        return Err(DslError::new(
            line_num,
            "multiply statement requires NAME by EXPR",
        ));
    }

    Ok((name, expr))
}

/// Parse `NAME by EXPR`
fn parse_divide_by(rest: &str, line_num: usize) -> Result<(String, String), DslError> {
    let by_pos = rest.find(" by ").ok_or_else(|| {
        DslError::new(
            line_num,
            "divide statement requires 'by' (e.g., divide counter by 2)",
        )
    })?;

    let name = rest[..by_pos].trim().to_string();
    let expr = rest[by_pos + 4..].trim().to_string();

    if name.is_empty() || expr.is_empty() {
        return Err(DslError::new(
            line_num,
            "divide statement requires NAME by EXPR",
        ));
    }

    Ok((name, expr))
}

/// Parse `VAR in EXPR` (for `for each VAR in EXPR`)
fn parse_for_each_header(rest: &str, line_num: usize) -> Result<(String, String), DslError> {
    let in_pos = rest.find(" in ").ok_or_else(|| {
        DslError::new(
            line_num,
            "for each requires 'in' (e.g., for each pos in positions())",
        )
    })?;

    let var = rest[..in_pos].trim().to_string();
    let iterable = rest[in_pos + 4..].trim().to_string();

    if var.is_empty() || iterable.is_empty() {
        return Err(DslError::new(line_num, "for each requires VAR in EXPR"));
    }

    Ok((var, iterable))
}

/// Parse `STRATEGY_CALL as VARNAME` for `try open` header.
fn parse_try_open_header(rest: &str, line_num: usize) -> Result<(String, String), DslError> {
    let as_pos = rest.rfind(" as ").ok_or_else(|| {
        DslError::new(
            line_num,
            "try open requires 'as VARNAME' (e.g., try open short_put(0.30, 45) as spread)",
        )
    })?;
    let call = rest[..as_pos].trim().to_string();
    let var_name = rest[as_pos + 4..].trim().to_string();
    if call.is_empty() || var_name.is_empty() {
        return Err(DslError::new(
            line_num,
            "try open requires STRATEGY_CALL as VARNAME",
        ));
    }
    Ok((call, var_name))
}

/// Case-insensitive prefix strip (checks both "Buy " and "buy ").
fn strip_prefix_ci<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if let Some(rest) = s.strip_prefix(prefix) {
        Some(rest)
    } else {
        let lower = prefix.to_lowercase();
        s.strip_prefix(lower.as_str())
    }
}

/// Parse a buy/sell statement body, extracting qty and optional order modifier.
///
/// Accepted forms (all fill at next bar):
/// - `100 shares next bar at market`           → Market order (canonical)
/// - `100 shares next bar at 150.00 limit`     → Limit order (canonical)
/// - `100 shares next bar at 155.00 stop`      → Stop order (canonical)
/// - `100 shares at market`                    → Market order (shorthand)
/// - `100 shares at 150.00 limit`              → Limit order (shorthand)
/// - `100 shares`                              → Market order (implicit)
fn parse_order_statement(rest: &str, line_num: usize) -> Result<(String, OrderModifier), DslError> {
    // Canonical form: "N shares next bar at ..."
    if let Some(shares_pos) = rest.find(" shares next bar at ") {
        let qty_expr = rest[..shares_pos].trim().to_string();
        let after_at = rest[shares_pos + " shares next bar at ".len()..].trim();
        let order_type = parse_order_type(after_at, line_num)?;
        if qty_expr.is_empty() {
            return Err(DslError::new(line_num, "Buy/Sell requires a quantity"));
        }
        return Ok((qty_expr, order_type));
    }

    // Shorthand: "N shares at ..."
    if let Some(shares_pos) = rest.find(" shares at ") {
        let qty_expr = rest[..shares_pos].trim().to_string();
        let after_at = rest[shares_pos + " shares at ".len()..].trim();
        let order_type = parse_order_type(after_at, line_num)?;
        if qty_expr.is_empty() {
            return Err(DslError::new(line_num, "Buy/Sell requires a quantity"));
        }
        return Ok((qty_expr, order_type));
    }

    // Implicit market: "N shares" or bare expression
    let qty_expr = rest
        .strip_suffix(" shares")
        .unwrap_or(rest)
        .trim()
        .to_string();

    if qty_expr.is_empty() {
        return Err(DslError::new(line_num, "Buy/Sell requires a quantity"));
    }

    Ok((qty_expr, OrderModifier::Market))
}

/// Parse the order type after "at": "market", "PRICE limit", or "PRICE stop".
fn parse_order_type(after_at: &str, line_num: usize) -> Result<OrderModifier, DslError> {
    if after_at == "market" {
        Ok(OrderModifier::Market)
    } else if let Some(price_str) = after_at.strip_suffix(" limit") {
        Ok(OrderModifier::Limit {
            price: price_str.trim().to_string(),
        })
    } else if let Some(price_str) = after_at.strip_suffix(" stop") {
        Ok(OrderModifier::Stop {
            price: price_str.trim().to_string(),
        })
    } else {
        Err(DslError::new(
            line_num,
            "order type must be 'market', 'PRICE limit', or 'PRICE stop'",
        ))
    }
}

/// Extract a `"quoted string"` value from the start of a string.
fn extract_quoted_value(s: &str, line_num: usize) -> Result<String, DslError> {
    let s = s.trim();
    if !s.starts_with('"') {
        return Err(DslError::new(line_num, "expected a quoted string"));
    }

    let end = s[1..]
        .find('"')
        .ok_or_else(|| DslError::new(line_num, "unterminated quoted string"))?;

    Ok(s[1..=end].to_string())
}

/// Extract the quoted string after a prefix: `prefix "value"` → `value`
fn extract_quoted_string(content: &str, prefix: &str, line_num: usize) -> Result<String, DslError> {
    let rest = content
        .strip_prefix(prefix)
        .ok_or_else(|| DslError::new(line_num, format!("expected '{prefix}'")))?
        .trim();

    extract_quoted_value(rest, line_num)
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_preprocess_strips_comments_and_blanks() {
        let source = "# comment\nstrategy \"Test\"\n\n  symbol AAPL\n  # another comment\n  interval daily\n";
        let lines = preprocess(source);
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].content, "strategy \"Test\"");
        assert_eq!(lines[0].indent, 0);
        assert_eq!(lines[1].content, "symbol AAPL");
        assert!(lines[1].indent > 0);
    }

    #[test]
    fn test_parse_extern() {
        let line = Line {
            indent: 0,
            content: "extern THRESHOLD = 0.04 \"Entry threshold\"".to_string(),
            num: 1,
        };
        let p = parse_extern(&line).unwrap();
        assert_eq!(p.name, "THRESHOLD");
        assert_eq!(p.default, "0.04");
        assert_eq!(p.description, "Entry threshold");
        assert!(p.choices.is_empty());
    }

    #[test]
    fn test_parse_extern_with_choices() {
        let line = Line {
            indent: 0,
            content: "extern MODE = \"fast\" \"Execution mode\" choices fast, slow, balanced"
                .to_string(),
            num: 1,
        };
        let p = parse_extern(&line).unwrap();
        assert_eq!(p.name, "MODE");
        assert_eq!(p.default, "\"fast\"");
        assert_eq!(p.choices.len(), 3);
    }

    #[test]
    fn test_parse_state() {
        let line = Line {
            indent: 0,
            content: "state wins = 0".to_string(),
            num: 1,
        };
        let s = parse_state(&line).unwrap();
        assert_eq!(s.name, "wins");
        assert_eq!(s.default, "0");
    }

    #[test]
    fn test_parse_minimal_program() {
        let source = r#"
strategy "Test"
  symbol AAPL
  interval daily

on each bar
  skip when has positions
  buy 100 shares
"#;
        let program = parse(source).unwrap();
        assert!(program.strategy.is_some());
        assert_eq!(program.strategy.as_ref().unwrap().name, "Test");
        assert!(program.on_bar.is_some());
        assert_eq!(program.on_bar.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_parse_when_otherwise_chain() {
        let source = r#"
strategy "Test"
  symbol SPY
  interval daily

on exit check
  when pos.pnl_pct > 0.50 then
    close position "take_profit"
  when pos.days_held > 30 then
    close position "max_hold"
  otherwise
    hold position
"#;
        let program = parse(source).unwrap();
        let stmts = program.on_exit_check.as_ref().unwrap();
        assert_eq!(stmts.len(), 1); // single chained When

        if let Stmt::When { else_body, .. } = &stmts[0] {
            // The else_body should be a chained When
            assert!(else_body.is_some());
            let inner = else_body.as_ref().unwrap();
            assert_eq!(inner.len(), 1);
            if let Stmt::When {
                else_body: inner_else,
                ..
            } = &inner[0]
            {
                // The inner else_body should be the otherwise
                assert!(inner_else.is_some());
            } else {
                panic!("expected chained When");
            }
        } else {
            panic!("expected When statement");
        }
    }

    #[test]
    fn test_rejects_missing_strategy() {
        let source = "on each bar\n  buy 100 shares\n";
        let err = parse(source).unwrap_err();
        assert!(err.message.contains("missing required 'strategy' block"));
    }

    #[test]
    fn test_rejects_duplicate_blocks() {
        let source = r#"
strategy "Test"
  symbol SPY
  interval daily

on each bar
  buy 100 shares

on each bar
  sell 50 shares
"#;
        let err = parse(source).unwrap_err();
        assert!(err.message.contains("duplicate"));
    }
}

# Unused rust_ti Indicators

Inventory of `rust_ti` 2.2.0 bulk functions not yet wired into the formula DSL. All accept `&[f64]` slices and return `Vec<f64>` or `Vec<(f64, ...)>`.

## Currently Used (10 functions)

- `standard_indicators::bulk::rsi` — fixed period-14 RSI (chart overlay)
- `standard_indicators::bulk::macd` — fixed 12/26/9 MACD (chart overlay)
- `momentum_indicators::bulk::relative_strength_index` — variable-period RSI (formula DSL)
- `momentum_indicators::bulk::money_flow_index` — MFI
- `momentum_indicators::bulk::on_balance_volume` — OBV
- `candle_indicators::bulk::moving_constant_bands` — Bollinger Bands
- `candle_indicators::bulk::keltner_channel` — Keltner Channel
- `candle_indicators::bulk::supertrend` — Supertrend
- `trend_indicators::single::aroon_indicator` — Aroon (up/down/osc)
- `other_indicators::single::average_true_range` — ATR

## Momentum

| Function | Params | Returns | Notes |
|----------|--------|---------|-------|
| `williams_percent_r` | `(high, low, close, period)` | `Vec<f64>` | Inverted stochastic (-100 to 0) |
| `commodity_channel_index` | `(prices, period, model)` | `Vec<f64>` | CCI — mean reversion |
| `chaikin_oscillator` | `(high, low, close, volume, fast, slow)` | `Vec<f64>` | Volume-weighted momentum |
| `percentage_price_oscillator` | `(prices, short, long, model)` | `Vec<f64>` | PPO — normalized MACD |
| `chande_momentum_oscillator` | `(prices, period)` | `Vec<f64>` | CMO — symmetric momentum |
| `slow_stochastic` | `(stochastics, period, model)` | `Vec<f64>` | Smoothed %K (chains on stochastic output) |
| `slowest_stochastic` | `(slow_stochastics, period, model)` | `Vec<f64>` | Double-smoothed stochastic |
| `rate_of_change` | `(prices)` | `Vec<f64>` | Fixed-period ROC (we have custom `roc(col, period)`) |

## Trend

| Function | Params | Returns | Notes |
|----------|--------|---------|-------|
| `directional_movement_system` | `(high, low, period)` | `Vec<(+DI, -DI, ADX)>` | ADX trend strength — high value |
| `parabolic_time_price_system` | `(high, low, accel, max_accel)` | `Vec<(SAR, trend, AF)>` | Parabolic SAR — trailing stops |
| `true_strength_index` | `(prices, fast, slow, model)` | `Vec<f64>` | TSI — double-smoothed momentum |
| `volume_price_trend` | `(close, volume)` | `Vec<f64>` | VPT — volume-confirmed trends |

## Channels

| Function | Params | Returns | Notes |
|----------|--------|---------|-------|
| `donchian_channels` | `(high, low, period)` | `Vec<(upper, mid, lower)>` | Breakout system — high value |
| `ichimoku_cloud` | `(highs, lows, short, long, signal)` | `Vec<(tenkan, kijun, senkou_a, senkou_b, chikou)>` | Full Ichimoku |
| `moving_constant_envelopes` | `(prices, model, difference)` | `Vec<(upper, mid, lower)>` | MA envelopes |

## Strength / Volume

| Function | Params | Returns | Notes |
|----------|--------|---------|-------|
| `accumulation_distribution` | `(high, low, close, volume)` | `Vec<f64>` | A/D line |
| `relative_vigor_index` | `(open, high, low, close, period)` | `Vec<f64>` | RVI |
| `positive_volume_index` | `(prices, volume)` | `Vec<f64>` | PVI |
| `negative_volume_index` | `(prices, volume)` | `Vec<f64>` | NVI |

## Volatility

| Function | Params | Returns | Notes |
|----------|--------|---------|-------|
| `ulcer_index` | `(prices, period)` | `Vec<f64>` | Downside volatility measure |
| `volatility_system` | `(prices, short, long)` | `Vec<(f64, f64)>` | Volatility bands |

## Priority Recommendations

1. **ADX** (`directional_movement_system`) — trend strength filtering, widely used
2. **Parabolic SAR** — dynamic stop-loss / trailing stop signal
3. **Donchian Channels** — breakout entry/exit signals
4. **Williams %R** — quick add, similar to stochastic
5. **Ichimoku Cloud** — comprehensive trend/support/resistance system

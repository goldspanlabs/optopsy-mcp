# Signal Presets — Design Doc

## Problem

Entry and exit signals are inherently paired but stored/managed separately. Users must track two `Saved` references (e.g. `ibs_mean_reversion_entry` + `ibs_mean_reversion_exit`) and pass them to separate backtest params. This is error-prone and adds friction.

## Proposal

Add a **SignalPreset** — a thin storage wrapper that bundles an entry and exit signal under one name. It decomposes into two `SignalSpec` values before reaching the engine.

### Storage Format

Stored as `~/.optopsy/signals/presets/{name}.json`:

```json
{
  "name": "ibs_mean_reversion",
  "entry": "close < (max(high, 10) - (sma(high, 25) - sma(low, 25)) * 2.5) and ((close - low) / (high - low)) < 0.3",
  "exit": "close > close[1]"
}
```

- `entry` and `exit` are `SignalSpec` values (string shorthand or tagged objects)
- `exit` is optional — omitting it means the backtest uses its default exit logic (DTE, stop-loss, etc.)
- Presets live in a `presets/` subdirectory to avoid collision with standalone signals

### Usage

Reference in backtest params:

```json
{
  "entry_signal": { "type": "Preset", "name": "ibs_mean_reversion" },
  "strategy": "short_put",
  ...
}
```

The tool layer resolves the preset before calling the engine:

```rust
// In tool handler, before calling run_backtest / run_stock_backtest:
if let SignalSpec::Preset { name } = &params.entry_signal {
    let preset = storage::load_preset(name)?;
    params.entry_signal = Some(preset.entry);
    params.exit_signal = preset.exit; // None if not specified
}
```

### `build_signal` Tool Changes

New actions:

| Action | Purpose |
|--------|---------|
| `create_preset` | Bundle entry + exit into a named preset |
| `list_presets` | List all saved presets |
| `get_preset` | Load a preset by name |
| `delete_preset` | Remove a preset |

### What Doesn't Change

- `SignalSpec` enum stays as-is (Formula, Saved, And, Or)
- Engine still receives separate `entry_signal` / `exit_signal` — no engine changes
- Standalone signals still work — presets are additive
- Mix-and-match still possible (use a preset's entry with a different exit by using standalone signals)

### Data Model

```rust
#[derive(Serialize, Deserialize)]
pub struct SignalPreset {
    pub name: String,
    pub entry: SignalSpec,
    pub exit: Option<SignalSpec>,
}
```

### Migration

Existing paired signals (e.g. `ibs_mean_reversion_entry` + `ibs_mean_reversion_exit`) can be migrated to presets and the individual files removed. A one-time migration could be offered through `build_signal` with `action: "migrate_to_preset"`.

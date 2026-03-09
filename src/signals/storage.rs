//! Persistent storage for custom signals.
//!
//! Signals are stored as JSON files in `~/.optopsy/signals/{name}.json`.
//! Each file contains a serialized `SignalSpec`.

use anyhow::{bail, Context, Result};
use std::fs;
use std::path::{Component, PathBuf};

use super::registry::SignalSpec;

/// Test-only override for signals directory, allowing isolation via temp dirs.
#[cfg(test)]
static TEST_SIGNALS_DIR: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);

/// Returns `true` if `path` is a filesystem root (normalised via `components()`).
///
/// Using `components()` ensures that non-canonical representations such as `///`
/// are treated identically to `/`, unlike direct `Path` equality which compares
/// raw byte strings and would consider them different.
///
/// On Windows, absolute drive roots such as `C:\` are represented as
/// `Prefix("C:") + RootDir` and are also treated as roots.
fn is_root_path(path: &std::path::Path) -> bool {
    let mut comps = path.components();
    match comps.next() {
        Some(Component::RootDir) => comps.next().is_none(),
        // Windows: drive prefix (e.g. "C:") followed by RootDir ("\") with nothing after
        Some(Component::Prefix(_)) => {
            matches!(comps.next(), Some(Component::RootDir)) && comps.next().is_none()
        }
        _ => false,
    }
}

/// Returns `true` if `path` has no meaningful components (empty string) or is a root path.
fn is_empty_or_root(path: &std::path::Path) -> bool {
    let mut comps = path.components();
    match comps.next() {
        None => true,
        Some(Component::RootDir) => comps.next().is_none(),
        // Windows: drive prefix + RootDir with nothing after
        Some(Component::Prefix(_)) => {
            matches!(comps.next(), Some(Component::RootDir)) && comps.next().is_none()
        }
        _ => false,
    }
}

/// Returns `true` if `path` resolves to exactly `/signals` (i.e. `RootDir` + `Normal("signals")`).
///
/// On Windows also matches `C:\signals` (`Prefix + RootDir + Normal("signals")`).
fn is_root_signals_path(path: &std::path::Path) -> bool {
    let mut comps = path.components();
    match comps.next() {
        Some(Component::RootDir) => {
            matches!(comps.next(), Some(Component::Normal(n)) if n == "signals")
                && comps.next().is_none()
        }
        // Windows: drive prefix + RootDir + Normal("signals")
        Some(Component::Prefix(_)) => {
            matches!(comps.next(), Some(Component::RootDir))
                && matches!(comps.next(), Some(Component::Normal(n)) if n == "signals")
                && comps.next().is_none()
        }
        _ => false,
    }
}

/// Compute the signals directory path from a `DATA_ROOT` value.
///
/// Uses `components()`-based helpers so that non-normalised representations such as `///`
/// are handled correctly — string/path equality against `"/"` is not reliable because
/// it compares raw byte strings and does not account for repeated or trailing separators.
///
/// - `DATA_ROOT="/"` (or any path whose components reduce to a single `RootDir`) is rejected.
/// - For multi-component roots like `/data/cache`, returns the sibling `/data/signals`.
/// - For single-component roots like `/data` (parent is `/`), falls back to
///   `data_root.join("signals")` → `/data/signals`.
/// - The computed path `/signals` is also rejected as unsafe.
fn compute_signals_dir_from_data_root(data_root: &std::path::Path, val: &str) -> Result<PathBuf> {
    if is_root_path(data_root) {
        anyhow::bail!("DATA_ROOT '{val}' is not a safe directory for signals storage");
    }

    let candidate = match data_root.parent() {
        Some(p) if !is_empty_or_root(p) => p.join("signals"),
        _ => data_root.join("signals"),
    };

    if is_root_signals_path(&candidate) {
        anyhow::bail!("DATA_ROOT '{val}' would place signals in an unsafe directory ('/signals')");
    }

    Ok(candidate)
}

/// Get the signals storage directory, creating it if needed.
///
/// When `DATA_ROOT` is set (e.g. `/data/cache`), signals are stored as a sibling
/// directory: `/data/signals`. Otherwise falls back to `~/.optopsy/signals`.
fn signals_dir() -> Result<PathBuf> {
    #[cfg(test)]
    {
        if let Ok(guard) = TEST_SIGNALS_DIR.lock() {
            if let Some(ref dir) = *guard {
                if !dir.exists() {
                    fs::create_dir_all(dir).context("Failed to create test signals directory")?;
                }
                return Ok(dir.clone());
            }
        }
    }

    let dir = if let Ok(val) = std::env::var("DATA_ROOT") {
        let data_root = PathBuf::from(&val);
        compute_signals_dir_from_data_root(&data_root, &val)?
    } else {
        let expanded = shellexpand::tilde("~/.optopsy/signals");
        if expanded.as_ref() == "~/.optopsy/signals" {
            std::env::temp_dir().join("optopsy").join("signals")
        } else {
            PathBuf::from(expanded.as_ref())
        }
    };
    if !dir.exists() {
        fs::create_dir_all(&dir).context("Failed to create signals directory")?;
    }
    Ok(dir)
}

/// Validate a signal name for safe filesystem use.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("Signal name cannot be empty");
    }
    if name.len() > 64 {
        bail!("Signal name too long (max 64 characters)");
    }
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        bail!("Signal name contains invalid characters");
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        bail!("Signal name must contain only alphanumeric characters, underscores, or hyphens");
    }
    Ok(())
}

/// Normalize a formula for duplicate comparison by stripping all whitespace and lowercasing.
/// This catches `close > sma(close, 20)` == `close>sma(close,20)` == `Close > SMA(close, 20)`.
fn normalize_formula(formula: &str) -> String {
    formula
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect::<String>()
        .to_lowercase()
}

/// Check if a formula already exists among saved signals (under a different name).
/// Returns the name of the existing signal if found.
pub fn find_duplicate_formula(formula: &str, exclude_name: &str) -> Result<Option<String>> {
    let normalized = normalize_formula(formula);
    let signals = list_saved_signals()?;
    for s in signals {
        if s.name == exclude_name {
            continue;
        }
        if let Some(existing_formula) = &s.formula {
            if normalize_formula(existing_formula) == normalized {
                return Ok(Some(s.name));
            }
        }
    }
    Ok(None)
}

/// Save a signal spec to disk.
///
/// Writes to a uniquely-named temporary file in the same directory first, then atomically
/// renames it into place. Both the temporary path and final target path are checked for
/// symlinks before writing to prevent symlink-based redirect attacks.
pub fn save_signal(name: &str, spec: &SignalSpec) -> Result<()> {
    validate_name(name)?;
    let dir = signals_dir()?;
    let path = dir.join(format!("{name}.json"));

    // Reject if the final target path is (or resolves through) a symlink.
    if let Ok(meta) = fs::symlink_metadata(&path) {
        if meta.file_type().is_symlink() {
            bail!(
                "Refusing to write signal '{name}': target path is a symlink ({})",
                path.display()
            );
        }
    }

    let json = serde_json::to_string_pretty(spec).context("Failed to serialize signal")?;

    // Use a PID-qualified temp filename to avoid collisions between concurrent writers.
    let tmp_path = dir.join(format!(".{name}.{}.tmp", std::process::id()));

    // Reject if the temp path is also a symlink (defence-in-depth).
    if let Ok(meta) = fs::symlink_metadata(&tmp_path) {
        if meta.file_type().is_symlink() {
            bail!(
                "Refusing to write signal '{name}': temp path is a symlink ({})",
                tmp_path.display()
            );
        }
    }

    fs::write(&tmp_path, &json)
        .with_context(|| format!("Failed to write temp signal file: {}", tmp_path.display()))?;
    fs::rename(&tmp_path, &path).with_context(|| {
        // Best-effort cleanup of the temp file on rename failure.
        let _ = fs::remove_file(&tmp_path);
        format!(
            "Failed to rename temp file to signal file: {}",
            path.display()
        )
    })?;
    Ok(())
}

/// Load a signal spec from disk by name.
pub fn load_signal(name: &str) -> Result<SignalSpec> {
    validate_name(name)?;
    let dir = signals_dir()?;
    let path = dir.join(format!("{name}.json"));
    if !path.exists() {
        bail!("Signal '{name}' not found. Use list_saved_signals to see available signals.");
    }
    let json = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read signal file: {}", path.display()))?;
    let spec: SignalSpec =
        serde_json::from_str(&json).with_context(|| format!("Failed to parse signal '{name}'"))?;
    Ok(spec)
}

/// Delete a saved signal by name.
pub fn delete_signal(name: &str) -> Result<()> {
    validate_name(name)?;
    let dir = signals_dir()?;
    let path = dir.join(format!("{name}.json"));
    if !path.exists() {
        bail!("Signal '{name}' not found");
    }
    fs::remove_file(&path)
        .with_context(|| format!("Failed to delete signal file: {}", path.display()))?;
    Ok(())
}

/// List all saved signal names.
pub fn list_saved_signals() -> Result<Vec<SavedSignalInfo>> {
    let dir = signals_dir()?;
    let mut signals = Vec::new();

    for entry in fs::read_dir(&dir).context("Failed to read signals directory")? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                let name = stem.to_string();
                // Skip files whose names are not accepted by the other APIs
                if validate_name(&name).is_err() {
                    continue;
                }
                // Try to load and extract info
                match fs::read_to_string(&path) {
                    Ok(json) => {
                        let spec: Result<SignalSpec, _> = serde_json::from_str(&json);
                        let (formula, description) = match &spec {
                            Ok(SignalSpec::Custom {
                                formula,
                                description,
                                ..
                            }) => (Some(formula.clone()), description.clone()),
                            _ => (None, None),
                        };
                        signals.push(SavedSignalInfo {
                            name,
                            formula,
                            description,
                        });
                    }
                    Err(_) => {
                        signals.push(SavedSignalInfo {
                            name,
                            formula: None,
                            description: None,
                        });
                    }
                }
            }
        }
    }

    signals.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(signals)
}

/// Info about a saved signal.
pub struct SavedSignalInfo {
    pub name: String,
    pub formula: Option<String>,
    pub description: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Serialize tests that touch the filesystem signals directory.
    // All filesystem tests MUST hold this lock AND set the temp dir override.
    static FS_LOCK: Mutex<()> = Mutex::new(());

    /// RAII guard that sets `TEST_SIGNALS_DIR` to a temp directory and restores it on drop.
    struct TempSignalsDir {
        _tmp: tempfile::TempDir,
    }

    impl TempSignalsDir {
        fn new() -> Self {
            let tmp = tempfile::TempDir::new().unwrap();
            *TEST_SIGNALS_DIR.lock().unwrap() = Some(tmp.path().to_path_buf());
            Self { _tmp: tmp }
        }
    }

    impl Drop for TempSignalsDir {
        fn drop(&mut self) {
            *TEST_SIGNALS_DIR.lock().unwrap() = None;
        }
    }

    #[test]
    fn signals_dir_deep_path_uses_sibling() {
        // DATA_ROOT=/data/cache → /data/signals (sibling of parent)
        let result =
            compute_signals_dir_from_data_root(std::path::Path::new("/data/cache"), "/data/cache");
        assert_eq!(result.unwrap(), std::path::Path::new("/data/signals"));
    }

    #[test]
    fn signals_dir_single_component_falls_back_to_subdir() {
        // DATA_ROOT=/data → parent is "/", falls back to /data/signals
        let result = compute_signals_dir_from_data_root(std::path::Path::new("/data"), "/data");
        assert_eq!(result.unwrap(), std::path::Path::new("/data/signals"));
    }

    #[test]
    fn signals_dir_root_returns_error() {
        // DATA_ROOT=/ is unsafe
        let result = compute_signals_dir_from_data_root(std::path::Path::new("/"), "/");
        assert!(result.is_err(), "DATA_ROOT='/' should return an error");
    }

    #[test]
    fn signals_dir_triple_slash_root_returns_error() {
        // "///" normalises to a single RootDir component and must be rejected like "/"
        let result = compute_signals_dir_from_data_root(std::path::Path::new("///"), "///");
        assert!(
            result.is_err(),
            "DATA_ROOT='///' should return an error (non-normalised root)"
        );
    }

    #[test]
    fn signals_dir_nested_three_levels_uses_sibling() {
        // DATA_ROOT=/a/b/c → sibling /a/b/signals
        let result = compute_signals_dir_from_data_root(std::path::Path::new("/a/b/c"), "/a/b/c");
        assert_eq!(result.unwrap(), std::path::Path::new("/a/b/signals"));
    }

    #[test]
    fn validate_name_ok() {
        assert!(validate_name("my_signal").is_ok());
        assert!(validate_name("rsi-oversold-30").is_ok());
        assert!(validate_name("test123").is_ok());
    }

    #[test]
    fn validate_name_bad() {
        assert!(validate_name("").is_err());
        assert!(validate_name("../evil").is_err());
        assert!(validate_name("path/traversal").is_err());
        assert!(validate_name("has spaces").is_err());
    }

    #[test]
    fn normalize_formula_strips_whitespace_and_lowercases() {
        assert_eq!(
            normalize_formula("close > sma(close, 20)"),
            "close>sma(close,20)"
        );
        assert_eq!(
            normalize_formula("close>sma(close,20)"),
            "close>sma(close,20)"
        );
        assert_eq!(
            normalize_formula("Close > SMA(close, 20)"),
            "close>sma(close,20)"
        );
        assert_eq!(
            normalize_formula("  close  >  sma( close , 20 )  "),
            "close>sma(close,20)"
        );
    }

    #[test]
    fn find_duplicate_formula_detects_match() {
        let _lock = FS_LOCK.lock().unwrap();
        let _dir = TempSignalsDir::new();

        let name_a = "dup-test-a";
        let name_b = "dup-test-b";
        let formula = "close > sma(close, 20)";

        let spec = SignalSpec::Custom {
            name: name_a.to_string(),
            formula: formula.to_string(),
            description: None,
        };
        save_signal(name_a, &spec).unwrap();

        // Same formula under different name should be detected
        let result = find_duplicate_formula(formula, name_b).unwrap();
        assert_eq!(result, Some(name_a.to_string()));

        // Same name should be excluded from duplicate check
        let result = find_duplicate_formula(formula, name_a).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn find_duplicate_formula_normalizes_spacing_and_case() {
        let _lock = FS_LOCK.lock().unwrap();
        let _dir = TempSignalsDir::new();

        let name = "dup-ws-test";
        let formula = "close > sma(close, 20)";

        let spec = SignalSpec::Custom {
            name: name.to_string(),
            formula: formula.to_string(),
            description: None,
        };
        save_signal(name, &spec).unwrap();

        // Extra whitespace should still match
        assert_eq!(
            find_duplicate_formula("close  >  sma(close,  20)", "other").unwrap(),
            Some(name.to_string())
        );

        // No spaces should still match
        assert_eq!(
            find_duplicate_formula("close>sma(close,20)", "other").unwrap(),
            Some(name.to_string())
        );

        // Case differences should still match
        assert_eq!(
            find_duplicate_formula("Close > SMA(close, 20)", "other").unwrap(),
            Some(name.to_string())
        );
    }

    #[test]
    fn find_duplicate_formula_no_match() {
        let _lock = FS_LOCK.lock().unwrap();
        let _dir = TempSignalsDir::new();

        let name = "dup-nomatch-test";
        let formula = "close > sma(close, 50)";

        let spec = SignalSpec::Custom {
            name: name.to_string(),
            formula: formula.to_string(),
            description: None,
        };
        save_signal(name, &spec).unwrap();

        // Different formula should not match
        let result = find_duplicate_formula("close < ema(close, 20)", "other").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn save_signal_overwrite_same_name() {
        let _lock = FS_LOCK.lock().unwrap();
        let _dir = TempSignalsDir::new();

        let name = "overwrite-test";

        let spec1 = SignalSpec::Custom {
            name: name.to_string(),
            formula: "close > sma(close, 10)".to_string(),
            description: Some("version 1".to_string()),
        };
        save_signal(name, &spec1).unwrap();

        let spec2 = SignalSpec::Custom {
            name: name.to_string(),
            formula: "close > sma(close, 20)".to_string(),
            description: Some("version 2".to_string()),
        };
        save_signal(name, &spec2).unwrap();

        // Should have the updated formula
        let loaded = load_signal(name).unwrap();
        if let SignalSpec::Custom { formula, .. } = loaded {
            assert_eq!(formula, "close > sma(close, 20)");
        } else {
            panic!("Expected Custom signal spec");
        }
    }
}

//! Persistent storage for custom signals.
//!
//! Signals are stored as JSON files in `~/.optopsy/signals/{name}.json`.
//! Each file contains a serialized `SignalSpec`.

use anyhow::{bail, Context, Result};
use std::fs;
use std::path::PathBuf;

use super::registry::SignalSpec;

/// Get the signals storage directory, creating it if needed.
fn signals_dir() -> Result<PathBuf> {
    const TEMPLATE: &str = "~/.optopsy/signals";
    let expanded = shellexpand::tilde(TEMPLATE);
    // If tilde was not expanded (no HOME set), fall back to a tmp-based path
    let dir = if expanded.as_ref() == TEMPLATE {
        std::env::temp_dir().join("optopsy").join("signals")
    } else {
        PathBuf::from(expanded.as_ref())
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

/// Check if a formula already exists among saved signals (under a different name).
/// Returns the name of the existing signal if found.
pub fn find_duplicate_formula(formula: &str, exclude_name: &str) -> Result<Option<String>> {
    let normalized = formula.split_whitespace().collect::<Vec<_>>().join(" ");
    let signals = list_saved_signals()?;
    for s in signals {
        if s.name == exclude_name {
            continue;
        }
        if let Some(existing_formula) = &s.formula {
            let existing_normalized = existing_formula
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            if existing_normalized == normalized {
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

    // Serialize tests that touch the filesystem signals directory
    static FS_LOCK: Mutex<()> = Mutex::new(());

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
    fn find_duplicate_formula_detects_match() {
        let _lock = FS_LOCK.lock().unwrap();
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

        // Cleanup
        let _ = delete_signal(name_a);
    }

    #[test]
    fn find_duplicate_formula_normalizes_whitespace() {
        let _lock = FS_LOCK.lock().unwrap();
        let name = "dup-ws-test";
        let formula = "close > sma(close, 20)";

        let spec = SignalSpec::Custom {
            name: name.to_string(),
            formula: formula.to_string(),
            description: None,
        };
        save_signal(name, &spec).unwrap();

        // Extra whitespace should still match
        let result = find_duplicate_formula("close  >  sma(close,  20)", "other").unwrap();
        assert_eq!(result, Some(name.to_string()));

        // Cleanup
        let _ = delete_signal(name);
    }

    #[test]
    fn find_duplicate_formula_no_match() {
        let _lock = FS_LOCK.lock().unwrap();
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

        // Cleanup
        let _ = delete_signal(name);
    }

    #[test]
    fn save_signal_overwrite_same_name() {
        let _lock = FS_LOCK.lock().unwrap();
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

        // Cleanup
        let _ = delete_signal(name);
    }
}

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
}

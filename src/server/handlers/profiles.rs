//! REST API handler for listing available parameter profiles.

use axum::Json;
use serde::Serialize;
use std::collections::HashMap;

use crate::scripting::stdlib::{list_scripts, load_profiles_registry};

/// A profile entry combining registry and script-level definitions.
#[derive(Debug, Serialize)]
pub struct ProfileInfo {
    pub name: String,
    pub registry_params: HashMap<String, serde_json::Value>,
    pub scripts_with_overrides: Vec<String>,
}

/// `GET /profiles` — List all available parameter profiles.
pub async fn list_profiles() -> Json<Vec<ProfileInfo>> {
    let registry = tokio::task::spawn_blocking(load_profiles_registry)
        .await
        .unwrap_or_default();

    let scripts = tokio::task::spawn_blocking(list_scripts)
        .await
        .unwrap_or_default();

    let mut profile_names: std::collections::BTreeSet<String> = registry.keys().cloned().collect();

    for script in &scripts {
        if let Some(profiles) = &script.profiles {
            for name in profiles.keys() {
                profile_names.insert(name.clone());
            }
        }
    }

    let profiles: Vec<ProfileInfo> = profile_names
        .into_iter()
        .map(|name| {
            let registry_params = registry.get(&name).cloned().unwrap_or_default();
            let scripts_with_overrides: Vec<String> = scripts
                .iter()
                .filter(|s| s.profiles.as_ref().is_some_and(|p| p.contains_key(&name)))
                .map(|s| s.id.clone())
                .collect();
            ProfileInfo {
                name,
                registry_params,
                scripts_with_overrides,
            }
        })
        .collect();

    Json(profiles)
}

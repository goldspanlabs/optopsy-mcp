//! Handler body for the `build_signal` tool.

use crate::tools;
use crate::tools::response_types::BuildSignalResponse;

use super::super::params::BuildSignalParams;

/// Execute the `build_signal` tool logic.
///
/// Parses the `action` field and dispatches to the appropriate signal operation.
pub fn execute(params: BuildSignalParams) -> Result<BuildSignalResponse, String> {
    let action = match params.action.as_str() {
        "create" => {
            let name = params
                .name
                .ok_or("'name' is required for action='create'")?;
            let formula = params
                .formula
                .ok_or("'formula' is required for action='create'")?;
            tools::build_signal::Action::Create {
                name,
                formula,
                description: params.description,
                save: params.save,
            }
        }
        "search" => {
            let prompt = params
                .prompt
                .ok_or("'prompt' is required for action='search'")?;
            tools::build_signal::Action::Search { prompt }
        }
        "list" => tools::build_signal::Action::List,
        "delete" => {
            let name = params
                .name
                .ok_or("'name' is required for action='delete'")?;
            tools::build_signal::Action::Delete { name }
        }
        "validate" => {
            let formula = params
                .formula
                .ok_or("'formula' is required for action='validate'")?;
            tools::build_signal::Action::Validate { formula }
        }
        "get" => {
            let name = params.name.ok_or("'name' is required for action='get'")?;
            tools::build_signal::Action::Get { name }
        }
        "update" => {
            let name = params
                .name
                .ok_or("'name' is required for action='update'")?;
            let new_name = params
                .new_name
                .ok_or("'new_name' is required for action='update'")?;
            tools::build_signal::Action::Update {
                name,
                new_name,
                display_name: params.display_name,
                formula: params.formula,
            }
        }
        "catalog" => tools::build_signal::Action::Catalog,
        other => {
            return Err(format!(
                "Invalid action: \"{other}\". Must be \"catalog\", \"search\", \"create\", \"list\", \"delete\", \"validate\", \"get\", or \"update\"."
            ));
        }
    };

    Ok(tools::build_signal::execute(action))
}

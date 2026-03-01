use anyhow::{bail, Result};
use std::sync::Arc;

use crate::data::eodhd::EodhdProvider;

use super::ai_format;
use super::response_types::DownloadResponse;

pub async fn execute(
    eodhd: Option<&Arc<EodhdProvider>>,
    symbol: &str,
) -> Result<DownloadResponse> {
    let Some(provider) = eodhd else {
        bail!(
            "EODHD_API_KEY not configured. \
             Set the EODHD_API_KEY environment variable to download options data."
        );
    };

    let summary = provider.download_options(symbol).await?;
    Ok(ai_format::format_download(summary))
}

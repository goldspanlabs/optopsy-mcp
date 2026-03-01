use anyhow::Result;
use rmcp::ServiceExt;
use tracing_subscriber::{self, EnvFilter};

mod data;
mod engine;
mod server;
mod signals;
mod strategies;
mod tools;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    tracing::info!("Starting optopsy-mcp MCP server");

    let server = server::OptopsyServer::new();
    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;

    Ok(())
}

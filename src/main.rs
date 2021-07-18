use crate::message::handle_messages;
use log::{error, info};
use std::env::{set_var, var};
use std::process::exit;

mod message;
mod search;
#[cfg(test)]
mod test;
mod ws;

#[tokio::main]
async fn main() {
    run().await.unwrap();
}

async fn init() -> anyhow::Result<()> {
    dotenv::dotenv()
        .map(|p| info!("load environment file: {}", p.to_str().unwrap_or("unknown")))
        .ok();

    if get_env("RUST_LOG").is_err() {
        set_var(
            "RUST_LOG",
            if cfg!(debug_assertions) {
                "debug"
            } else {
                "info"
            },
        );
    }
    env_logger::init();

    if !search::is_meilisearch_running().await {
        error!("error: meilisearch is not running");
        exit(1)
    }

    Ok(())
}

async fn run() -> anyhow::Result<()> {
    init().await?;

    let (tx, rx) = tokio::sync::mpsc::channel(10_000);

    let res = tokio::try_join!(tokio::spawn(ws::run(tx)), tokio::spawn(handle_messages(rx)))?;
    res.0?;
    res.1?;

    Ok(())
}

fn get_env(name: &'static str) -> anyhow::Result<String> {
    var(name).map_err(|_| anyhow::anyhow!("找不到环境变量: {}", name))
}

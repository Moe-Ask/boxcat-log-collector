use meilisearch_sdk::client::Client;
use meilisearch_sdk::document::Document;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

use crate::get_env;
use crate::message::{Level, Message};
use chrono::TimeZone;
use meilisearch_sdk::dumps::{DumpInfo, DumpStatus};
use meilisearch_sdk::indexes::IndexStats;
use meilisearch_sdk::progress::UpdateStatus;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::time::Duration;

static CLIENT: Lazy<Client> = Lazy::new(|| Client::new(get_host(), get_api_key()));

pub fn get_api_key() -> String {
    get_env("SEARCH_API_KEY").unwrap()
}

pub fn get_host() -> String {
    get_env("SEARCH_HOST").unwrap()
}

pub async fn is_meilisearch_running() -> bool {
    CLIENT.is_healthy().await
}

#[derive(Serialize, Deserialize, Debug)]
struct LogIndex {
    uid: String,

    level: String,
    module: String,
    source: Option<String>,
    timestamp: String,
    is_compressed: bool,
    payload: String,
}

impl Document for LogIndex {
    type UIDType = String;

    fn get_uid(&self) -> &Self::UIDType {
        &self.uid
    }
}
pub static A: AtomicU64 = AtomicU64::new(0);
pub async fn add_documents(msgs: Vec<Message>) -> anyhow::Result<()> {
    A.fetch_add(msgs.len() as u64, Ordering::SeqCst);
    static ATOMIC_ID: AtomicU64 = AtomicU64::new(0);

    if msgs.is_empty() {
        return Ok(());
    }

    let index = CLIENT.get_or_create("log").await?;
    index
        .add_documents(
            &{
                let mut v = Vec::with_capacity(msgs.len());
                for msg in msgs {
                    v.push(LogIndex {
                        uid: format!(
                            "{}-{}-{}",
                            chrono::Local::now().timestamp_nanos(),
                            &uuid::Uuid::new_v4().to_simple_ref().to_string(),
                            ATOMIC_ID.fetch_add(1, Ordering::Release)
                        ),
                        level: Level::from(msg.level).to_string(),
                        module: msg.module,
                        source: msg.source,
                        timestamp: chrono::Utc.timestamp(msg.timestamp as i64, 0).to_rfc3339(),
                        is_compressed: msg.is_compressed,
                        payload: { String::from_utf8(msg.payload).unwrap() },
                    });
                }
                v
            },
            Some("uid"),
        )
        .await?;

    Ok(())
}

pub async fn get_stats() -> anyhow::Result<IndexStats> {
    let index = CLIENT.get_or_create("log").await?;
    Ok(index.get_stats().await?)
}

pub async fn all_updated() -> anyhow::Result<bool> {
    let index = CLIENT.get_or_create("log").await?;
    Ok(index
        .get_all_updates()
        .await?
        .iter()
        .find(|status| {
            matches!(
                status,
                UpdateStatus::Processing { .. } | UpdateStatus::Enqueued { .. }
            )
        })
        .is_none())
}

pub async fn dump() -> anyhow::Result<DumpInfo> {
    Ok(CLIENT.create_dump().await?)
}

pub async fn wait_for_dump(info: &DumpInfo, interval: Option<Duration>) -> anyhow::Result<bool> {
    let mut interval =
        tokio::time::interval(interval.unwrap_or_else(|| Duration::from_millis(1000)));
    loop {
        match CLIENT.get_dump_status(&info.uid).await?.status {
            DumpStatus::Done => return Ok(true),
            DumpStatus::Failed => return Ok(false),
            DumpStatus::InProgress => {
                interval.tick().await;
            }
        }
    }
}

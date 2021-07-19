use std::io::{Cursor, Write};

use async_compression::tokio::write::{ZstdDecoder, ZstdEncoder};
use byteorder::{ReadBytesExt, WriteBytesExt};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::mem::swap;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::sync::mpsc::Receiver;
use tokio::sync::Mutex;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message {
    pub(crate) level: u8,
    pub(crate) module: String,
    pub(crate) source: Option<String>,
    pub(crate) timestamp: u64,
    pub(crate) is_compressed: bool,
    pub(crate) payload: Vec<u8>,
}

#[allow(dead_code)]
pub enum Level {
    Fatal = 1,
    Error = 3,
    Warn = 5,
    Info = 7,
    Debug = 9,
    Trace = 11,

    Unknown = 256,
}

impl From<u8> for Level {
    fn from(b: u8) -> Self {
        match b {
            1 => Level::Fatal,
            3 => Level::Error,
            5 => Level::Warn,
            7 => Level::Info,
            9 => Level::Debug,
            11 => Level::Trace,
            _ => Level::Unknown,
        }
    }
}

impl Display for Level {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Level::Fatal => "fatal",
                Level::Error => "error",
                Level::Warn => "warning",
                Level::Info => "info",
                Level::Debug => "debug",
                Level::Trace => "trace",
                Level::Unknown => "unknown",
            }
        )
    }
}

impl Message {
    pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(self.payload.len() + 10);
        buffer.write_u8(self.level)?;
        {
            let bytes = self.module.as_bytes();
            buffer.write_u8(bytes.len() as u8)?;
            buffer.write_all(bytes)?;
        }
        {
            if let Some(source) = &self.source {
                let bytes = source.as_bytes();
                buffer.write_u8(bytes.len() as u8)?;
                buffer.write_all(bytes)?;
            } else {
                buffer.write_u8(0)?;
            }
        }
        buffer.write_u64::<byteorder::NetworkEndian>(self.timestamp)?;
        buffer.write_u8(self.is_compressed as u8)?;
        buffer.write_u32::<byteorder::NetworkEndian>(self.payload.len() as u32)?;
        buffer.write_all(&self.payload)?;
        buffer.flush()?;

        Ok(buffer)
    }

    pub fn deserialize(data: &[u8]) -> anyhow::Result<Self> {
        let mut cur = Cursor::new(data);
        Ok(Message {
            level: cur.read_u8()?,
            module: {
                let mut data = Vec::with_capacity(cur.read_u8()? as usize);
                for _ in 0..data.capacity() {
                    data.push(cur.read_u8()?);
                }
                String::from_utf8(data)?
            },
            source: {
                let len = cur.read_u8()?;
                if len != 0 {
                    let mut data = Vec::with_capacity(len as usize);
                    for _ in 0..data.capacity() {
                        data.push(cur.read_u8()?);
                    }
                    Some(String::from_utf8(data)?)
                } else {
                    None
                }
            },
            timestamp: cur.read_u64::<byteorder::NetworkEndian>()?,
            is_compressed: cur.read_u8()? != 0,
            payload: {
                let mut data =
                    Vec::with_capacity(cur.read_u32::<byteorder::NetworkEndian>()? as usize);
                for _ in 0..data.capacity() {
                    data.push(cur.read_u8()?);
                }
                data
            },
        })
    }
}

pub(crate) async fn handle_message(mut msg: Message) -> anyhow::Result<Message> {
    use tokio::io::AsyncWriteExt;
    if msg.is_compressed {
        let mut decoder = ZstdDecoder::new(Vec::with_capacity(msg.payload.len()));
        decoder.write_all(&msg.payload).await?;
        decoder.shutdown().await?;
        msg.payload = decoder.into_inner();
        msg.is_compressed = false;
    }

    write_to_temp_file(&msg.serialize()?).await?;

    Ok(msg)
}

pub async fn handle_messages(mut rx: Receiver<Message>) -> anyhow::Result<()> {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(1_000));
    let msgs = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let m2 = Arc::clone(&msgs);

    tokio::spawn(async move {
        loop {
            interval.tick().await;

            let mut new_vec = Vec::new();
            swap(m2.clone().lock().await.as_mut(), &mut new_vec);
            crate::search::add_documents(new_vec).await.unwrap();
        }
    });

    loop {
        while let Some(msg) = rx.recv().await {
            if msg.level >= Level::Trace as u8 {
                continue;
            }
            match handle_message(msg).await {
                Err(err) => log::error!("handle message error: {}", err),
                Ok(msg) => msgs.lock().await.push(msg),
            }
        }
        return Ok(());
    }
}

static CURRENT_FILE: OnceCell<(String, Mutex<File>)> = OnceCell::new();

#[async_recursion::async_recursion]
async fn write_to_temp_file(data: &[u8]) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let today = get_today();

    let open_temp_file = || async {
        OpenOptions::default()
            .create(true)
            .append(true)
            .write(true)
            .open(format!("{}.temp", &today))
            .await
    };

    // init
    if CURRENT_FILE.get().is_none() {
        CURRENT_FILE
            .set((today.clone(), Mutex::new(open_temp_file().await?)))
            .unwrap();
    }

    let (file_day, file) = CURRENT_FILE.get().unwrap();
    let mut file = file.lock().await;

    if &today != file_day {
        compression_file(
            &mut file,
            OpenOptions::default()
                .create(true)
                .truncate(true)
                .write(true)
                .open(format!("{}.zstd", &today))
                .await?,
        )
        .await?;
        CURRENT_FILE
            .set((today.clone(), Mutex::new(open_temp_file().await?)))
            .unwrap();
        return write_to_temp_file(data).await;
    }

    // be
    file.write_u32(data.len() as u32).await?;
    file.write_all(data).await?;
    file.sync_data().await?;

    Ok(())
}

fn get_today() -> String {
    chrono::Local::today().format("%Y-%m-%d").to_string()
}

async fn compression_file(from: &mut File, to: File) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut encoder = ZstdEncoder::new(to);
    tokio::io::copy(from, &mut encoder).await?;
    encoder.shutdown().await?;
    encoder.into_inner().sync_data().await?;
    Ok(())
}

#[test]
fn test_message_ser_de() {
    let msg = Message {
        level: Level::Info as u8,
        module: "test".to_string(),
        source: Some("localhost:7070".to_owned()),
        timestamp: chrono::Local::now().timestamp() as u64,
        is_compressed: false,
        payload: b"this is a test message".to_vec(),
    };

    assert!(Message::deserialize(&msg.serialize().unwrap()).is_ok());
}

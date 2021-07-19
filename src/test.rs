use crate::message::{handle_message, handle_messages, Level, Message};
use crate::search::{all_updated, get_stats};
use crate::{init, run};
use std::env::set_var;
use std::sync::atomic::{AtomicU32, Ordering};

#[tokio::test]
async fn test_dump() {
    set_var("RUST_LOG", "INFO");
    init().await;

    let info = crate::search::dump().await.unwrap();
    assert!(crate::search::wait_for_dump(&info, None).await.unwrap());
}

#[tokio::test]
async fn test_message() {
    tokio::spawn(run());

    let msg = Message {
        level: Level::Info as u8,
        module: "test".to_string(),
        source: Some("localhost:7070".to_owned()),
        timestamp: chrono::Local::now().timestamp() as u64,
        is_compressed: false,
        payload: b"this is a test message".to_vec(),
    };

    handle_message(msg).await.unwrap();
}

#[tokio::test]
async fn test_message_throughput() {
    const TEST_TIME: u32 = 5;

    set_var("RUST_LOG", "INFO");
    init().await.unwrap();

    let (tx, rx) = tokio::sync::mpsc::channel(50_000);
    let h = tokio::spawn(handle_messages(rx));
    tokio::task::yield_now().await;

    let counter = AtomicU32::new(0);
    let now_docs = get_stats().await.unwrap().number_of_documents;

    tokio::time::timeout(tokio::time::Duration::from_secs(TEST_TIME as u64), async {
        loop {
            let msg = Message {
                level: Level::Info as u8,
                module: "test".to_string(),
                source: Some("localhost:7070".to_owned()),
                timestamp: chrono::Local::now().timestamp() as u64,
                is_compressed: false,
                payload: b"this is a test message".to_vec(),
            };
            tx.send(msg).await.unwrap();
            counter.fetch_add(1, Ordering::SeqCst);
        }
    })
    .await
    .ok();

    drop(tx);
    h.await.unwrap().unwrap();

    let count = counter.load(Ordering::SeqCst);

    while !all_updated().await.unwrap() {
        tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
    }

    let updated = get_stats().await.unwrap().number_of_documents - now_docs;

    println!("handle message speed: {}/s", count / TEST_TIME);
    dbg!(crate::search::A.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(count, updated as u32, "uid冲突");
}

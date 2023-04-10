use futures::StreamExt;
use redis_async::{client, resp::FromResp};
use serde::{Deserialize, Serialize};
use sqlx::mysql::MySqlPool;

/**
 * TODO: These structs are used to deserialize the JSON messages.
 * They are also defined in the ws-service codebase - these should be
 * extracted into a shared library.
 */
#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub channel: String,
    pub payload: Payload,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    pub fen: String,
    pub game_id: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Attempt to subscribe to games channel
    if let Err(error) = subscribe(String::from("games")).await {
        println!("Error: {}", error);
    } else {
        println!("Successfully spawned games queue listener");
    }

    Ok(())
}

async fn handle_message(message: String, pool: &sqlx::mysql::MySqlPool) {
    let message_obj: Message = serde_json::from_str::<Message>(&message).unwrap();

    let rows_affected = sqlx::query!(
        r#"
    UPDATE games
    SET fen = ?
    WHERE id = ?
            "#,
        message_obj.payload.fen,
        message_obj.payload.game_id.to_string()
    )
    .execute(pool)
    .await
    .expect("Could not update game")
    .rows_affected();
}

async fn subscribe(channel: String) -> Result<(), Box<dyn std::error::Error>> {
    let redis_host: String = std::env::var("REDIS_HOST").unwrap_or(String::from("127.0.0.1"));
    let redis_con = client::pubsub_connect(redis_host, 6379)
        .await
        .expect("Could not connect to Redis");

    let sql_pool = MySqlPool::connect(
        &std::env::var("DATABASE_URL")
            .unwrap_or(String::from("mysql://sail:password@localhost/backend")),
    )
    .await
    .unwrap();

    let mut msg_stream = redis_con
        .psubscribe(&channel)
        .await
        .expect("Could not subscribe to games topic");

    while let Some(msg) = msg_stream.next().await {
        match msg {
            Ok(msg) => handle_message(String::from_resp(msg).unwrap(), &sql_pool).await,
            Err(e) => {
                eprintln!("ERROR: {}", e);
                break;
            }
        }
    }

    Ok(())
}

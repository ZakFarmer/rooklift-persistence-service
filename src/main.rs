extern crate redis;

use redis::{ControlFlow, PubSubCommands};
use serde::{Serialize, Deserialize};

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
    if let Err(error) = subscribe(String::from("games")) {
        println!("Error: {}", error);
    } else {
        println!("Successfully spawned games queue listener");
    }

    Ok(())
}

fn handle_message(message: Message) {
    println!("Received message: {:?}", message);
}

fn subscribe(channel: String) -> Result<(), Box<dyn std::error::Error>> {
    let _ = tokio::spawn(async move {
        let client = redis::Client::open("redis://localhost").unwrap();

        let mut con = client.get_connection().unwrap();

        let _: () = con.subscribe(&[channel], |msg| {
            let payload: String = msg.get_payload().unwrap();
            let payload_obj = serde_json::from_str::<Message>(&payload).unwrap();

            handle_message(payload_obj);

            return ControlFlow::Continue;
        }).unwrap();
    });

    Ok(())
}
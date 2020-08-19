
use futures::channel::mpsc;
use async_std::{
    prelude::*,
    task,
    net::{
        ToSocketAddrs,
        TcpListener,
        TcpStream,
    },
    io::{BufReader},
};
use futures::sink::SinkExt;
use std::sync::Arc;
use futures::{StreamExt};
use std::collections::HashMap;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Sender<T> = mpsc::UnboundedSender<T>;
type Receiver<T> = mpsc::UnboundedReceiver<T>;

enum Message {
    UserMessage {
        contents: String,
    },
    NewConnection {
        id: i32,
        sender: Sender<Message>,
    },
    Closed {
        id: i32,
    },
}

fn main() -> Result<()> {
    println!("hello....");
    task::block_on(accept_task("127.0.0.1:8083"))
}

async fn accept_task(addr: impl ToSocketAddrs) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;

    let (mut sender_to_broker, receiver_broker) = mpsc::unbounded::<Message>();

    println!("before broker");
    // broker 
    task::spawn(connection_broker_task(receiver_broker));
    println!("after broker");

    let mut next_id : i32 = 1;

    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        println!("connected");
        let stream = Arc::new(stream?);
        (&*stream).write_all("hello\n".as_bytes()).await?;
        (&*stream).flush().await?;

        let (sender, receiver) = mpsc::unbounded::<Message>();

        // read from stream -> write to sender
        task::spawn(connection_recv_task(next_id, Arc::clone(&stream), sender_to_broker.clone()));

        let new_conn_message = Message::NewConnection{id: next_id, sender: sender.clone() };
        next_id += 1;

        sender_to_broker.send(new_conn_message).await?;

        // read from receiver2 -> write to stream
        task::spawn(connection_send_task(Arc::clone(&stream), receiver));
    }
    Ok(())
}

// stream -> sender
async fn connection_recv_task(id: i32, stream: Arc<TcpStream>, mut sender_to_broker: Sender<Message>) -> Result<()> {
    let reader = BufReader::new(&*stream);
    let mut lines = reader.lines();

    while let Some(message) = lines.next().await {
        if let Ok(message) = message {
            println!("received: {}", message);
            sender_to_broker.send(Message::UserMessage{contents: message}).await?;
        } else {
            println!("receive error");
            break;
        }
    };

    println!("exit connection_recv_task");
    sender_to_broker.send(Message::Closed{id}).await?;

    Ok(())
}

// receiver -> stream
async fn connection_send_task(stream: Arc<TcpStream>, mut receiver: Receiver<Message>) -> Result<()> {
    let mut stream = &*stream;
    while let Some(msg) = receiver.next().await {
        match msg {
            Message::UserMessage{contents} => {
                println!("send_task. {}", contents);
                stream.write_all(format!("{}\n", contents).as_bytes()).await?
            },
            _ => {}
        }
    }
    Ok(())
}

async fn connection_broker_task(mut receiver: Receiver<Message>) -> Result<()> {
    let mut connections: HashMap<i32, Sender<Message>> = HashMap::new();

    while let Some(message) = receiver.next().await {
        match message {
            Message::NewConnection{id,  sender} => {
                println!("connection added");
                connections.insert(id, sender);
            },
            Message::UserMessage{contents} => {
                println!("user message {}", contents);
                for (_, sender) in connections.iter_mut() {
                    println!("send to connection");
                    //(&*stream).write_all(contents.as_bytes()).await?;
                    sender.send(Message::UserMessage{contents: contents.clone()}).await?;
                }
            },
            Message::Closed{id} => {
                println!("session closed. {}", id);
                connections.remove(&id);
            }
        }
    }
    Ok(())
}
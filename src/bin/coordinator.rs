use anyhow::{Ok, Result};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use desync::net::{recv_json, send_json};

#[derive(Serialize, Deserialize)]
struct Register {
    id: String,
}

#[derive(Serialize, Deserialize)]
struct Job {
    id: String,
    cmd: String,
}

#[derive(Serialize, Deserialize)]
struct JobResult {
    job_id: String,
    success: bool,
    output: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6000").await?;
    println!("coord listeing");
    loop {
        let (mut socket, addr) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_worker(&mut socket).await {
                eprintln!("worker task error {e}");
            }
        });
        println!("worker disconnected");
    }
}

async fn handle_worker(socket: &mut TcpStream) -> Result<()> {
    let reg: Register = recv_json(socket).await?;
    println!("worker registered {}", reg.id);

    let job = Job {
        id: "dummy job".to_string(),
        cmd: "echo hello world && uname -a".to_string(),
    };
    send_json(socket, &job).await?;
    let res: JobResult = recv_json(socket).await?;
    println!("job result :\n {}", res.output);
    Ok(())
}

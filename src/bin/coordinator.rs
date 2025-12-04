use anyhow::Result;
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
        let reg: Register = recv_json(&mut socket).await?;
        println!("worker registered {}", reg.id);

        let job = Job {
            id: "dummy job".to_string(),
            cmd: "echo hello world && uname -a".to_string(),
        };
        send_json(&mut socket, &job).await?;
        let res: JobResult = recv_json(&mut socket).await?;
        println!("job result :\n {}", res.output);
    }
}

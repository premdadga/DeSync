use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;

use desync::net::{recv_json, send_json};

#[derive(Serialize, Deserialize, Debug)]
struct JobSubmission {
    cmd: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct JobConfirmation {
    job_id: String,
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut socket = TcpStream::connect("127.0.0.1:6001").await?;

    let submission: JobSubmission = JobSubmission {
        cmd: "echo Hello from client!".to_string(),
    };

    send_json(&mut socket, &submission).await?;
    println!("Job submitted");

    let confirmation: JobConfirmation = recv_json(&mut socket).await?;
    println!("Received: {:?}", confirmation);

    Ok(())
}

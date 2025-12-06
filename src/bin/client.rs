use anyhow::Result;
use desync::net::{recv_json, send_json};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;

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
    let args: Vec<String> = std::env::args().collect();

    let cmd = if args.len() > 1 {
        args[1..].join(" ")
    } else {
        "echo Hello from client!".to_string()
    };

    let mut socket = TcpStream::connect("127.0.0.1:6001").await?;

    let submission = JobSubmission { cmd: cmd.clone() };

    send_json(&mut socket, &submission).await?;
    println!("Submitted job: {}", cmd);

    let confirmation: JobConfirmation = recv_json(&mut socket).await?;
    println!("✓ {}", confirmation.message);
    println!("Job ID: {}", confirmation.job_id);

    Ok(())
}

use anyhow::{Ok, Result};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::Command,
};

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
    let mut socket = TcpStream::connect("127.0.0.1:6000").await?;
    println!("connected ");
    //initializing the connection
    let reg = Register {
        id: "worker1".to_string(),
    };
    let out = serde_json::to_string(&reg)?;
    socket.write_all(out.as_bytes()).await?; //write all does the job
                                             //reading job from server
    let mut buf = vec![0u8; 16384];
    let n = socket.read(&mut buf).await?;
    let recv = String::from_utf8_lossy(&buf[..n]);
    let job: Job = serde_json::from_str(&recv)?;
    println!("got the job {:?}", job.cmd);

    //executing the job
    let output = if cfg!(target_os = "windows") {
        Command::new("cmd").arg("/C").arg(&job.cmd).output().await?
    } else {
        Command::new("sh").arg("-c").arg(&job.cmd).output().await?
    };
    let success = output.status.success();
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result = JobResult {
        job_id: job.id,
        success,
        output: combined,
    };

    let msg = serde_json::to_string(&result)?;
    socket.write_all(msg.as_bytes()).await?;
    println!("resunnt sent");

    Ok(())
}

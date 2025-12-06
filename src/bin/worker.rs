use anyhow::Result;
use desync::net::{recv_json, send_json};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, process::Command};

#[derive(Serialize, Deserialize, Debug)]
struct Register {
    id: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Job {
    id: String,
    cmd: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct JobResult {
    job_id: String,
    success: bool,
    output: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut socket = TcpStream::connect("127.0.0.1:6000").await?;
    println!("Connected to coordinator");

    // Register with coordinator
    let reg = Register {
        id: "worker1".to_string(),
    };
    send_json(&mut socket, &reg).await?;
    println!("Registered as {}", reg.id);

    // Job loop - keep handling jobs!
    loop {
        // Wait for job from coordinator
        let job: Job = match recv_json(&mut socket).await {
            Ok(job) => job,
            Err(e) => {
                eprintln!("Failed to receive job: {}", e);
                break;
            }
        };

        println!("Received job {}: {}", job.id, job.cmd);

        // Execute the job
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
            job_id: job.id.clone(),
            success,
            output: combined,
        };

        // Send result back
        send_json(&mut socket, &result).await?;
        println!("Job {} completed, sent result", job.id);
    }

    println!("Worker shutting down");
    Ok(())
}

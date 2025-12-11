use anyhow::Result;
use desync::net::{recv_json, send_json};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    process::Command,
    time::{self, Duration},
};

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
enum WorkerMessage {
    Ready,
    Heartbeat,
    JobResult {
        job_id: String,
        success: bool,
        output: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut socket = TcpStream::connect("127.0.0.1:6000").await?;
    println!("Connected to coordinator");

    // Register
    let reg = Register {
        id: format!(
            "worker_{}",
            uuid::Uuid::new_v4().to_string()[..8].to_string()
        ),
    };
    send_json(&mut socket, &reg).await?;
    println!("Registered as {}", reg.id);

    // Spawn heartbeat task
    let socket_clone = socket.try_clone()?;
    tokio::spawn(async move { send_heartbeats(socket_clone).await });

    // Worker loop
    loop {
        // Signal: I'm ready for work!
        send_json(&mut socket, &WorkerMessage::Ready).await?;
        println!("Sent ready signal");

        // Wait for job
        let job: Job = match recv_json(&mut socket).await {
            Ok(job) => job,
            Err(e) => {
                eprintln!("Failed to receive job: {}", e);
                break;
            }
        };

        println!("Received job {}: {}", job.id, job.cmd);

        // Execute
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

        // Send result
        let result = WorkerMessage::JobResult {
            job_id: job.id.clone(),
            success,
            output: combined,
        };
        send_json(&mut socket, &result).await?;
        println!("Job {} completed", job.id);
    }

    Ok(())
}

async fn send_heartbeats(mut socket: TcpStream) {
    let mut interval = time::interval(Duration::from_secs(5));

    loop {
        interval.tick().await;

        if send_json(&mut socket, &WorkerMessage::Heartbeat)
            .await
            .is_err()
        {
            eprintln!("Failed to send heartbeat, connection lost");
            break;
        }

        println!("💓 Sent heartbeat");
    }
}

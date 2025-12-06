use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use uuid::Uuid;

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

#[derive(Serialize, Deserialize, Clone)]
struct Job {
    id: String,
    cmd: String,
}

#[derive(Serialize, Deserialize)]
struct Register {
    id: String,
}

#[derive(Serialize, Deserialize)]
struct JobResult {
    job_id: String,
    success: bool,
    output: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let (job_tx, job_rx) = mpsc::channel::<Job>(100);
    let job_tx_clone = job_tx.clone();
    let job_rx = Arc::new(Mutex::new(job_rx));

    // Job submission listener (port 6001)
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:6001").await?;
        println!("Job submission listening on port 6001");

        loop {
            let (mut socket, _) = listener.accept().await?;
            let tx = job_tx_clone.clone();

            tokio::spawn(async move {
                let result: Result<()> = async {
                    let submission: JobSubmission = recv_json(&mut socket).await?;
                    println!("Received job submission: {:?}", submission.cmd);

                    let job_id = Uuid::new_v4().to_string();
                    let job = Job {
                        id: job_id.clone(),
                        cmd: submission.cmd,
                    };

                    tx.send(job)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to queue job: {}", e))?; // Add ?

                    let confirmation = JobConfirmation {
                        job_id,
                        message: "Job queued successfully".to_string(),
                    };
                    send_json(&mut socket, &confirmation).await?;

                    println!("Job queued");
                    Ok(())
                }
                .await;

                if let Err(e) = result {
                    eprintln!("Job submission handler error: {}", e);
                }
            });
        }

        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    });

    let listener = TcpListener::bind("127.0.0.1:6000").await?;
    println!("Coordinator listening on port 6000");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let rx = job_rx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_worker(&mut socket, rx).await {
                eprintln!("Worker task error: {}", e);
            }
            println!("Worker disconnected");
        });
    }
}

async fn handle_worker(
    socket: &mut TcpStream,
    job_rx: Arc<Mutex<mpsc::Receiver<Job>>>,
) -> Result<()> {
    let reg: Register = recv_json(socket).await?;
    println!("Worker registered: {}", reg.id);

    let job = {
        let mut rx = job_rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("JOb channel closed"))?
    };

    println!(
        "sending job to worker job id: {}, job command : {}",
        job.id, job.cmd
    );

    send_json(socket, &job).await?;
    let res: JobResult = recv_json(socket).await?;
    println!("Job result:\n{}", res.output);

    Ok(())
}

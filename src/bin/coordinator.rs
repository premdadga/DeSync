use anyhow::Result;
use desync::net::{recv_json, send_json};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, Mutex},
};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
struct JobSubmission {
    cmd: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct JobConfirmation {
    job_id: String,
    message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Job {
    id: String,
    cmd: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Register {
    id: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct JobResult {
    job_id: String,
    success: bool,
    output: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let (job_tx, job_rx) = mpsc::channel::<Job>(100);
    let job_rx = Arc::new(Mutex::new(job_rx));

    // Run both listeners concurrently - no outer spawns!
    tokio::try_join!(
        run_job_submission_listener(job_tx),
        run_worker_listener(job_rx)
    )?;

    Ok(())
}

async fn run_job_submission_listener(job_tx: mpsc::Sender<Job>) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6001").await?;
    println!("Job submission listening on port 6001");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let tx = job_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_job_submission(&mut socket, tx).await {
                eprintln!("Job submission error: {}", e);
            }
        });
    }
}

async fn handle_job_submission(socket: &mut TcpStream, job_tx: mpsc::Sender<Job>) -> Result<()> {
    let submission: JobSubmission = recv_json(socket).await?;
    println!("Received job: {}", submission.cmd);

    let job_id = Uuid::new_v4().to_string();
    let job = Job {
        id: job_id.clone(),
        cmd: submission.cmd,
    };

    job_tx.send(job).await?;

    let confirmation = JobConfirmation {
        job_id,
        message: "Job queued successfully".to_string(),
    };
    send_json(socket, &confirmation).await?;

    Ok(())
}

async fn run_worker_listener(job_rx: Arc<Mutex<mpsc::Receiver<Job>>>) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6000").await?;
    println!("Worker listener on port 6000");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let rx = job_rx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_worker(&mut socket, rx).await {
                eprintln!("Worker error: {}", e);
            }
            println!("Worker disconnected");
        });
    }
}

async fn handle_worker(
    socket: &mut TcpStream,
    job_rx: Arc<Mutex<mpsc::Receiver<Job>>>,
) -> Result<()> {
    // Register worker
    let reg: Register = recv_json(socket).await?;
    println!("Worker registered: {}", reg.id);

    // Job loop - worker handles multiple jobs!
    loop {
        // Wait for a job from the queue
        let job = {
            let mut rx = job_rx.lock().await;
            match rx.recv().await {
                Some(job) => job,
                None => {
                    println!("Job channel closed, worker {} exiting", reg.id);
                    return Ok(());
                }
            }
        };

        println!("Sending job {} to worker {}", job.id, reg.id);
        send_json(socket, &job).await?;

        // Wait for result
        let result: JobResult = recv_json(socket).await?;
        println!(
            "Worker {} completed job {}: success={}",
            reg.id, result.job_id, result.success
        );
        if result.success {
            println!("Output:\n{}", result.output);
        } else {
            println!("Error:\n{}", result.output);
        }
    }
}

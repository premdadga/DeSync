use anyhow::Result;
use desync::net::{recv_json, send_json};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
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
enum WorkerMessage {
    Ready,
    JobResult {
        job_id: String,
        success: bool,
        output: String,
    },
}

// Internal message from worker handler to dispatcher
struct WorkerRequest {
    worker_id: String,
    sender: mpsc::Sender<Job>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Job submissions come in here
    let (job_tx, mut job_rx) = mpsc::channel::<Job>(100);

    // Worker ready requests come in here
    let (worker_tx, mut worker_rx) = mpsc::channel::<WorkerRequest>(100);

    // Spawn job dispatcher - the brain!
    tokio::spawn(async move {
        let mut job_queue: VecDeque<Job> = VecDeque::new();
        let mut idle_workers: VecDeque<WorkerRequest> = VecDeque::new();

        loop {
            tokio::select! {
                // New job submitted
                Some(job) = job_rx.recv() => {
                    println!("Job {} queued", job.id);

                    // Is there an idle worker?
                    if let Some(worker) = idle_workers.pop_front() {
                        println!("Dispatching job {} to worker {}", job.id, worker.worker_id);
                        if worker.sender.send(job).await.is_err() {
                            eprintln!("Worker {} disconnected", worker.worker_id);
                        }
                    } else {
                        // No idle workers, queue the job
                        job_queue.push_back(job);
                    }
                }

                // Worker became ready
                Some(worker) = worker_rx.recv() => {
                    // Is there a queued job?
                    if let Some(job) = job_queue.pop_front() {
                        println!("Dispatching job {} to worker {}", job.id, worker.worker_id);
                        if worker.sender.send(job).await.is_err() {
                            eprintln!("Worker {} disconnected", worker.worker_id);
                        }
                    } else {
                        // No jobs, worker goes into idle pool
                        idle_workers.push_back(worker);
                    }
                }
            }
        }
    });

    // Run both listeners
    tokio::try_join!(
        run_job_submission_listener(job_tx),
        run_worker_listener(worker_tx)
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

async fn run_worker_listener(worker_tx: mpsc::Sender<WorkerRequest>) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6000").await?;
    println!("Worker listener on port 6000");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let tx = worker_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_worker(&mut socket, tx).await {
                eprintln!("Worker error: {}", e);
            }
            println!("Worker disconnected");
        });
    }
}

async fn handle_worker(
    socket: &mut TcpStream,
    worker_tx: mpsc::Sender<WorkerRequest>,
) -> Result<()> {
    // Register
    let reg: Register = recv_json(socket).await?;
    println!("Worker registered: {}", reg.id);

    // Create a channel for this worker to receive jobs
    let (job_sender, mut job_receiver) = mpsc::channel::<Job>(10);

    // Worker loop
    loop {
        // Wait for worker to signal ready
        let msg: WorkerMessage = recv_json(socket).await?;

        match msg {
            WorkerMessage::Ready => {
                // Tell dispatcher this worker is ready
                let request = WorkerRequest {
                    worker_id: reg.id.clone(),
                    sender: job_sender.clone(),
                };

                if worker_tx.send(request).await.is_err() {
                    eprintln!("Dispatcher gone, worker {} exiting", reg.id);
                    return Ok(());
                }

                // Wait for dispatcher to send a job
                if let Some(job) = job_receiver.recv().await {
                    println!("Sending job {} to worker {}", job.id, reg.id);
                    send_json(socket, &job).await?;
                } else {
                    // Dispatcher dropped our sender
                    return Ok(());
                }
            }

            WorkerMessage::JobResult {
                job_id,
                success,
                output,
            } => {
                println!(
                    "Worker {} completed job {}: success={}",
                    reg.id, job_id, success
                );
                if success {
                    println!("Output:\n{}", output);
                } else {
                    println!("Error:\n{}", output);
                }
            }
        }
    }
}

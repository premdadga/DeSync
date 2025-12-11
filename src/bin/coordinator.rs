use anyhow::Result;
use desync::net::{recv_json, send_json};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time,
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
    Heartbeat,
    JobResult {
        job_id: String,
        success: bool,
        output: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
enum CoordinatorMessage {
    Job(Job),
    Shutdown,
}

// Worker states
#[derive(Debug, Clone, PartialEq)]
enum WorkerStatus {
    Idle,
    Busy { job_id: String },
    Dead,
}

// Track each worker
struct WorkerInfo {
    id: String,
    status: WorkerStatus,
    last_heartbeat: Instant,
    sender: mpsc::Sender<CoordinatorMessage>,
}

// Track each job
#[derive(Debug, Clone)]
struct JobInfo {
    job: Job,
    assigned_to: Option<String>,
    attempts: u32,
    max_retries: u32,
}

// Messages to dispatcher
enum DispatcherMessage {
    NewJob(Job),
    WorkerReady {
        worker_id: String,
    },
    WorkerHeartbeat {
        worker_id: String,
    },
    JobComplete {
        worker_id: String,
        job_id: String,
    },
    WorkerRegistered {
        worker_id: String,
        sender: mpsc::Sender<CoordinatorMessage>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let (dispatcher_tx, mut dispatcher_rx) = mpsc::channel::<DispatcherMessage>(100);

    // Spawn the dispatcher - the brain!
    tokio::spawn(async move {
        run_dispatcher(&mut dispatcher_rx).await;
    });

    // Run both listeners
    tokio::try_join!(
        run_job_submission_listener(dispatcher_tx.clone()),
        run_worker_listener(dispatcher_tx)
    )?;

    Ok(())
}

async fn run_dispatcher(dispatcher_rx: &mut mpsc::Receiver<DispatcherMessage>) {
    let mut job_queue: VecDeque<JobInfo> = VecDeque::new();
    let mut workers: HashMap<String, WorkerInfo> = HashMap::new();

    // Health check interval - check every 2 seconds
    let mut health_check_interval = time::interval(Duration::from_secs(2));

    const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

    loop {
        tokio::select! {
            // New message from workers or job submissions
            Some(msg) = dispatcher_rx.recv() => {
                match msg {
                    DispatcherMessage::NewJob(job) => {
                        println!("📥 Job {} queued", job.id);

                        let job_info = JobInfo {
                            job: job.clone(),
                            assigned_to: None,
                            attempts: 0,
                            max_retries: 3,
                        };

                        // Try to assign to idle worker
                        if let Some(worker_id) = find_idle_worker(&workers) {
                            assign_job_to_worker(&mut workers, &mut job_queue, worker_id, job_info).await;
                        } else {
                            job_queue.push_back(job_info);
                        }
                    }

                    DispatcherMessage::WorkerRegistered { worker_id, sender } => {
                        println!("✅ Worker {} registered", worker_id);
                        workers.insert(
                            worker_id.clone(),
                            WorkerInfo {
                                id: worker_id,
                                status: WorkerStatus::Idle,
                                last_heartbeat: Instant::now(),
                                sender,
                            },
                        );
                    }

                    DispatcherMessage::WorkerReady { worker_id } => {
                        if let Some(worker) = workers.get_mut(&worker_id) {
                            worker.status = WorkerStatus::Idle;
                            worker.last_heartbeat = Instant::now();

                            // Assign job if available
                            if let Some(job_info) = job_queue.pop_front() {
                                assign_job_to_worker(&mut workers, &mut job_queue, worker_id, job_info).await;
                            }
                        }
                    }

                    DispatcherMessage::WorkerHeartbeat { worker_id } => {
                        if let Some(worker) = workers.get_mut(&worker_id) {
                            worker.last_heartbeat = Instant::now();
                            println!("💓 Heartbeat from worker {}", worker_id);
                        }
                    }

                    DispatcherMessage::JobComplete { worker_id, job_id } => {
                        println!("✅ Job {} completed by worker {}", job_id, worker_id);
                        if let Some(worker) = workers.get_mut(&worker_id) {
                            worker.status = WorkerStatus::Idle;

                            // Try to assign next job
                            if let Some(job_info) = job_queue.pop_front() {
                                assign_job_to_worker(&mut workers, &mut job_queue, worker_id, job_info).await;
                            }
                        }
                    }
                }
            }

            // Health check timer fires
            _ = health_check_interval.tick() => {
                check_worker_health(&mut workers, &mut job_queue, HEARTBEAT_TIMEOUT).await;
            }
        }
    }
}

fn find_idle_worker(workers: &HashMap<String, WorkerInfo>) -> Option<String> {
    workers
        .iter()
        .find(|(_, info)| info.status == WorkerStatus::Idle)
        .map(|(id, _)| id.clone())
}

async fn assign_job_to_worker(
    workers: &mut HashMap<String, WorkerInfo>,
    job_queue: &mut VecDeque<JobInfo>,
    worker_id: String,
    mut job_info: JobInfo,
) {
    if let Some(worker) = workers.get_mut(&worker_id) {
        job_info.assigned_to = Some(worker_id.clone());
        job_info.attempts += 1;

        worker.status = WorkerStatus::Busy {
            job_id: job_info.job.id.clone(),
        };

        println!(
            "📤 Dispatching job {} to worker {} (attempt {}/{})",
            job_info.job.id, worker_id, job_info.attempts, job_info.max_retries
        );

        if worker
            .sender
            .send(CoordinatorMessage::Job(job_info.job.clone()))
            .await
            .is_err()
        {
            eprintln!("❌ Worker {} disconnected, requeueing job", worker_id);
            worker.status = WorkerStatus::Dead;
            job_info.assigned_to = None;
            job_queue.push_front(job_info);
        }
    }
}

async fn check_worker_health(
    workers: &mut HashMap<String, WorkerInfo>,
    job_queue: &mut VecDeque<JobInfo>,
    timeout: Duration,
) {
    let now = Instant::now();
    let mut dead_workers = Vec::new();

    for (worker_id, worker) in workers.iter_mut() {
        if worker.status == WorkerStatus::Dead {
            continue;
        }

        let elapsed = now.duration_since(worker.last_heartbeat);

        if elapsed > timeout {
            eprintln!(
                "💀 Worker {} timed out (last seen {:?} ago)",
                worker_id, elapsed
            );

            // If worker was busy, requeue the job
            if let WorkerStatus::Busy { job_id } = &worker.status {
                println!("🔄 Requeueing job {} from dead worker", job_id);

                // Find the job and requeue it
                let job_info = JobInfo {
                    job: Job {
                        id: job_id.clone(),
                        cmd: String::new(), // We'd need to store this properly
                    },
                    assigned_to: None,
                    attempts: 0,
                    max_retries: 3,
                };
                job_queue.push_front(job_info);
            }

            worker.status = WorkerStatus::Dead;
            dead_workers.push(worker_id.clone());
        }
    }

    // Remove dead workers
    for worker_id in dead_workers {
        workers.remove(&worker_id);
        println!("🗑️  Removed dead worker {}", worker_id);
    }
}

async fn run_job_submission_listener(dispatcher_tx: mpsc::Sender<DispatcherMessage>) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6001").await?;
    println!("Job submission listening on port 6001");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let tx = dispatcher_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_job_submission(&mut socket, tx).await {
                eprintln!("Job submission error: {}", e);
            }
        });
    }
}

async fn handle_job_submission(
    socket: &mut TcpStream,
    dispatcher_tx: mpsc::Sender<DispatcherMessage>,
) -> Result<()> {
    let submission: JobSubmission = recv_json(socket).await?;
    println!("Received job: {}", submission.cmd);

    let job_id = Uuid::new_v4().to_string();
    let job = Job {
        id: job_id.clone(),
        cmd: submission.cmd,
    };

    dispatcher_tx.send(DispatcherMessage::NewJob(job)).await?;

    let confirmation = JobConfirmation {
        job_id,
        message: "Job queued successfully".to_string(),
    };
    send_json(socket, &confirmation).await?;

    Ok(())
}

async fn run_worker_listener(dispatcher_tx: mpsc::Sender<DispatcherMessage>) -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6000").await?;
    println!("Worker listener on port 6000");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let tx = dispatcher_tx.clone();

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
    dispatcher_tx: mpsc::Sender<DispatcherMessage>,
) -> Result<()> {
    // Register
    let reg: Register = recv_json(socket).await?;
    println!("Worker registered: {}", reg.id);

    // Create channel for this worker
    let (job_sender, mut job_receiver) = mpsc::channel::<CoordinatorMessage>(10);

    // Tell dispatcher about this worker
    dispatcher_tx
        .send(DispatcherMessage::WorkerRegistered {
            worker_id: reg.id.clone(),
            sender: job_sender,
        })
        .await?;

    // Worker loop
    loop {
        let msg: WorkerMessage = recv_json(socket).await?;

        match msg {
            WorkerMessage::Ready => {
                dispatcher_tx
                    .send(DispatcherMessage::WorkerReady {
                        worker_id: reg.id.clone(),
                    })
                    .await?;

                // Wait for job from dispatcher
                if let Some(coord_msg) = job_receiver.recv().await {
                    match coord_msg {
                        CoordinatorMessage::Job(job) => {
                            send_json(socket, &job).await?;
                        }
                        CoordinatorMessage::Shutdown => {
                            println!("Sending shutdown to worker {}", reg.id);
                            return Ok(());
                        }
                    }
                }
            }

            WorkerMessage::Heartbeat => {
                dispatcher_tx
                    .send(DispatcherMessage::WorkerHeartbeat {
                        worker_id: reg.id.clone(),
                    })
                    .await?;
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

                dispatcher_tx
                    .send(DispatcherMessage::JobComplete {
                        worker_id: reg.id.clone(),
                        job_id,
                    })
                    .await?;
            }
        }
    }
}

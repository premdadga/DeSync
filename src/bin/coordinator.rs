use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
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
    let listener = TcpListener::bind("127.0.0.1:6000").await?;
    println!("coord listeing");
    loop {
        let (mut socket, addr) = listener.accept().await?;
        let mut buf = vec![0u8; 4096];
        let n = socket.read(&mut buf).await?;
        let recv = String::from_utf8_lossy(&buf[..n]);
        let reg: Register = serde_json::from_str(&recv)?;
        println!("worker registered {}", reg.id);

        let job = Job {
            id: "dummy job".to_string(),
            cmd: "echo hello world && uname -a".to_string(),
        };
        let msg = serde_json::to_string(&job)?;
        socket.write_all(msg.as_bytes()).await?;

        let mut buf2 = vec![0u8; 16384];
        let n2 = socket.read(&mut buf2).await?;
        let recv2 = String::from_utf8_lossy(&buf2[..n2]);
        let res: JobResult = serde_json::from_str(&recv2)?;
        println!("job result :\n {}", res.output);
    }
}

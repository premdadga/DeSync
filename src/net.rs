use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub async fn send_json<T: Serialize>(stream: &mut TcpStream, value: &T) -> Result<()> {
    let json = serde_json::to_vec(value)?;
    let len = json.len() as u32;
    let prefix = len.to_be_bytes();
    stream.write_all(&prefix).await?;
    stream.write_all(&json).await?;
    Ok(())
}

pub async fn recv_json<T: for<'de> Deserialize<'de>>(stream: &mut TcpStream) -> Result<T> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    let value = serde_json::from_slice::<T>(&buf)?;
    Ok(value)
}

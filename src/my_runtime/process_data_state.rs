use std::future::Future;
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::my_runtime::MyTcpListener;

pub struct AcceptConnection {
    pub my_tcp_listener: MyTcpListener,
}

impl Future for AcceptConnection {
    type Output = io::Result<mio::net::TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("--- Future: Attempting to accept socket connection... ---");

        match self.my_tcp_listener.accept() {
            Ok((stream, _addr)) => Poll::Ready(Ok(stream)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                println!("Future: No requests currently in queue. Sending waker to Reactor...");
                let waker: std::task::Waker = cx.waker().clone();
                let _ = self.my_tcp_listener.reactor_send(waker);
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

pub async fn process_data(my_tcp_listener: MyTcpListener) {
    println!("Step 1: HTTP Backend Active. Awaiting browser request...");

    let connection_future = AcceptConnection { my_tcp_listener };

    if let Ok(stream) = connection_future.await {
        give_res(stream);
    }
}

fn give_res(mut stream: mio::net::TcpStream) {
    println!("Step 3: Client connected! Processing HTTP payload...");

    let mut buffer: [u8; 1024] = [0; 1024];
    let _ = stream.read(&mut buffer);
    println!(
        "Frontend Request:\n{}",
        String::from_utf8_lossy(&buffer[..150])
    );

    let response = "HTTP/1.1 200 OK\r\nAccess-Control-Allow-Origin: *\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\": \"Success from DIY Async!\"}\n";
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
    println!("Step 4: Sent JSON packet to frontend successfully.");
}

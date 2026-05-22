// process_data_state.rs
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::sync::mpsc::Sender;
use std::io::{self, Read, Write};
use crate::reactor::Reactor;

pub struct AcceptConnection {
    // A raw pointer pointing directly back to our single Reactor instance
    pub reactor_ptr: *const Reactor,
    pub reactor_sender: Sender<std::task::Waker>,
}

impl Future for AcceptConnection {
    type Output = io::Result<mio::net::TcpStream>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("--- Future: Attempting to accept socket connection... ---");

        // Dereference the pointer safely to execute the accept action
        let reactor = unsafe { &*self.reactor_ptr };

        match reactor.accept_stream() {
            Ok((stream, _addr)) => Poll::Ready(Ok(stream)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                println!("Future: No requests currently in queue. Sending waker to Reactor...");
                let waker = cx.waker().clone();
                let _ = self.reactor_sender.send(waker);
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

pub async fn processData(reactor_ptr: *const Reactor, sender: Sender<std::task::Waker>) {
    println!("Step 1: HTTP Backend Active. Awaiting browser request...");
    
    let connection_future = AcceptConnection { reactor_ptr, reactor_sender: sender };
    
    if let Ok(mut stream) = connection_future.await {
        println!("Step 3: Client connected! Processing HTTP payload...");

        let mut buffer = [0; 1024];
        let _ = stream.read(&mut buffer);
        println!("Frontend Request:\n{}", String::from_utf8_lossy(&buffer[..150]));

        let response = "HTTP/1.1 200 OK\r\nAccess-Control-Allow-Origin: *\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\": \"Success from DIY Async!\"}\n";
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
        println!("Step 4: Sent JSON packet to frontend successfully.");
    }
}
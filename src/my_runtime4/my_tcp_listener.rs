use std::{
    io,
    sync::{Arc, Mutex, mpsc},
    task::{Context, Poll},
};

use mio::{
    Token,
    net::{TcpListener, TcpStream},
};

use crate::my_runtime4::my_reactor::Registration;

pub struct MyTcpListener {
    // pub listener: TcpListener,
    listener: Arc<Mutex<TcpListener>>,
    pub has_waker: bool,
    reactor_sender: mpsc::Sender<Registration>,
    token: usize,
}

impl MyTcpListener {
    pub fn new(addr: std::net::SocketAddr, reactor_sender: mpsc::Sender<Registration>) -> Self {
        // let addr = "127.0.0.1:8080".parse().unwrap();
        let raw_listener = TcpListener::bind(addr).unwrap();
        let listener: Arc<Mutex<TcpListener>> = Arc::new(Mutex::new(raw_listener));

        MyTcpListener {
            // listener: raw_listener,
            listener,
            has_waker: false,
            reactor_sender,
            token: 0, // In a real system, generate an incremental unique ID per socket
        }
    }
}

impl Future for MyTcpListener {
    type Output = io::Result<TcpStream>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let listener_clone = self.listener.clone();
        let listener_lock = listener_clone.lock().unwrap();

        match listener_lock.accept() {
            Ok((stream, _addr)) => Poll::Ready(Ok(stream)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                if !self.has_waker {
                    println!("Future: Socket blocked! Forwarding waker to the global Reactor...");

                    self.token += 1; // Increment token for the next listener

                    let registration = Registration {
                        listener: listener_clone.clone(),
                        token: Token(self.token),
                        waker: cx.waker().clone(),
                    };

                    // Send to the central reactor thread without blocking this task worker
                    let _ = self.reactor_sender.send(registration);
                    self.has_waker = true;
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

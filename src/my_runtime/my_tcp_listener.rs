use std::{
    io,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread,
    time::Duration,
};

use mio::net::{TcpListener, TcpStream};

pub struct MyTcpListener {
    pub listener: TcpListener,
    pub has_waker: bool,
}

impl MyTcpListener {
    pub fn new(addr: std::net::SocketAddr) -> Self {
        // let addr = "127.0.0.1:8080".parse().unwrap();
        let raw_listener = TcpListener::bind(addr).unwrap();
        MyTcpListener {
            listener: raw_listener,
            has_waker: false,
        }
    }
}

impl Future for MyTcpListener {
    type Output = io::Result<TcpStream>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.listener.accept() {
            Ok((stream, _addr)) => Poll::Ready(Ok(stream)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                println!("Future: No requests currently in queue. Sending waker to Reactor...");
                // let waker: std::task::Waker = cx.waker().clone();
                // let _ = self.my_tcp_listener.reactor_send(waker);
                println!("Executor: Task is still pending, will check again later.");
                if !self.has_waker {
                    let waker = Arc::new(Mutex::new(cx.waker().clone()));

                    self.has_waker = true;

                    thread::spawn(move || {
                        thread::sleep(Duration::from_secs(30));

                        let waker = waker.lock().unwrap();

                        waker.wake_by_ref();
                    });
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

use std::{
    io, result,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread,
    time::Duration,
};

use mio::net::{TcpListener, TcpStream};

use crate::my_runtime::my_reactor::Reactor;

pub struct MyTcpListener {
    // pub listener: TcpListener,
    listener: Arc<Mutex<TcpListener>>,
    pub has_waker: bool,
}

impl MyTcpListener {
    pub fn new(addr: std::net::SocketAddr) -> Self {
        // let addr = "127.0.0.1:8080".parse().unwrap();
        let raw_listener = TcpListener::bind(addr).unwrap();
        let listener: Arc<Mutex<TcpListener>> = Arc::new(Mutex::new(raw_listener));

        MyTcpListener {
            // listener: raw_listener,
            listener,
            has_waker: false,
        }
    }
}

impl Future for MyTcpListener {
    type Output = io::Result<TcpStream>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let listener_clone = self.listener.clone();
        let listener_lock = listener_clone.lock().unwrap();
        match listener_lock.accept() {
            Ok((stream, _addr)) => {
                Poll::Ready(Ok(stream))
            },
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                println!("Executor: Task is still pending, will check again later.");
                if !self.has_waker {
                    let waker = Arc::new(Mutex::new(cx.waker().clone()));

                    self.has_waker = true;

                    let mut listener_clone2 = listener_clone.clone();

                    thread::spawn(move || {
                        // thread::sleep(Duration::from_secs(30));

                        let mut reactor = Reactor::new();

                        let data_ready = reactor.check_tcp_resource(&mut listener_clone2);

                        if data_ready {
                            let waker = waker.lock().unwrap();

                            waker.wake_by_ref();
                        } 
                    });
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

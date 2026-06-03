use std::{
    io,
    sync::{Arc, Mutex, mpsc},
    task::{Context, Poll},
};

use mio::{
    Token,
    net::{TcpListener, TcpStream},
};

use mio::Waker as MioWaker;

use crate::my_runtime6::my_reactor::Registration;

use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_TOKEN: AtomicUsize = AtomicUsize::new(1);
pub struct MyTcpListener {
    listener: Arc<Mutex<TcpListener>>,
    registration_sent: bool, // Renamed for accurate intent
    reactor_sender: mpsc::Sender<Registration>,
    mio_waker: Arc<MioWaker>,
    token: Token,
}

impl MyTcpListener {
    pub fn new(
        addr: std::net::SocketAddr,
        reactor_sender: mpsc::Sender<Registration>,
        mio_waker: Arc<MioWaker>, // Accept it on creation
    ) -> Self {
        let raw_listener = TcpListener::bind(addr).unwrap();
        let listener = Arc::new(Mutex::new(raw_listener));

        let token = Token(NEXT_TOKEN.fetch_add(1, Ordering::Relaxed));

        MyTcpListener {
            listener,
            registration_sent: false,
            reactor_sender,
            mio_waker,
            token,
        }
    }
}

impl Future for MyTcpListener {
    type Output = io::Result<TcpStream>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let listener_clone = self.listener.clone();

        let accept_result = {
            let listener_lock = self.listener.lock().unwrap();
            listener_lock.accept()
        };

        match accept_result {
            Ok((stream, _addr)) => {
                self.registration_sent = false;
                println!(
                    "file: my_tcp_listener.rs - line 62 - Ok - stream : {:?} ",
                    stream
                );
                Poll::Ready(Ok(stream))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                if !self.registration_sent {
                    println!("Future: Socket blocked! Forwarding waker to the global Reactor...");

                    let registration = Registration {
                        listener: listener_clone.clone(),
                        token: self.token,
                        waker: cx.waker().clone(),
                    };

                    let _ = self.reactor_sender.send(registration);

                    self.mio_waker.wake().unwrap();

                    self.registration_sent = true;
                }
                Poll::Pending
            }
            Err(e) => {
                self.registration_sent = false;
                Poll::Ready(Err(e))
            }
        }
    }
}

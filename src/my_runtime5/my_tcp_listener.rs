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

use crate::my_runtime5::my_reactor::Registration;

pub struct MyTcpListener {
    listener: Arc<Mutex<TcpListener>>,
    pub has_waker: bool,
    reactor_sender: mpsc::Sender<Registration>,
    mio_waker: Arc<MioWaker>, // Listener holds a reference clone of the reactor's waker
    token: usize,
}

impl MyTcpListener {
    pub fn new(
        addr: std::net::SocketAddr,
        reactor_sender: mpsc::Sender<Registration>,
        mio_waker: Arc<MioWaker>, // Accept it on creation
    ) -> Self {
        let raw_listener = TcpListener::bind(addr).unwrap();
        let listener = Arc::new(Mutex::new(raw_listener));

        MyTcpListener {
            listener,
            has_waker: false,
            reactor_sender,
            mio_waker,
            token: 0, // Reserve token 0 for the Waker, start socket tokens from 1!
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

                    // Incremental unique ID (offset by 1 to skip WAKER_TOKEN at 0)
                    self.token += 1;

                    let registration = Registration {
                        listener: listener_clone.clone(),
                        token: Token(self.token),
                        waker: cx.waker().clone(),
                    };

                    // 1. Send the registration to the channel queue
                    let _ = self.reactor_sender.send(registration);

                    // 2. CRITICAL: Break the OS poll lock!
                    // This kicks `self.poll.poll(..., None)` out of its deep sleep immediately.
                    self.mio_waker.wake().unwrap();

                    self.has_waker = true;
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

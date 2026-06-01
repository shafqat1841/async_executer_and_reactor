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
    mio_waker: Arc<MioWaker>,
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
            token: 0,
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

                    self.token += 1;

                    let registration = Registration {
                        listener: listener_clone.clone(),
                        token: Token(self.token),
                        waker: cx.waker().clone(),
                    };

                    let _ = self.reactor_sender.send(registration);

                    self.mio_waker.wake().unwrap();

                    self.has_waker = true;
                }
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

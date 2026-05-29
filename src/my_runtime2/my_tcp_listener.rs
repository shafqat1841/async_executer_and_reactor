use std::{sync::{Arc, Mutex, mpsc::Sender}, task::Waker};

#[derive(Clone)] // Now we can clone this handle easily!
pub struct MyTcpListener {
    // Wrap the mio listener in an Arc so it can be shared cleanly
pub listener: Arc<Mutex<mio::net::TcpListener>>,
    reactor_sender: Sender<Waker>,
}

impl MyTcpListener {
    pub fn new(listener: Arc<Mutex<mio::net::TcpListener>>, reactor_sender: Sender<Waker>) -> Self {
        MyTcpListener {
            listener,
            reactor_sender,
        }
    }

    pub fn accept(&self) -> std::io::Result<(mio::net::TcpStream, std::net::SocketAddr)> {
        let lock = self.listener.lock().unwrap();
        lock.accept()
    }

    pub fn reactor_send(&self, waker: Waker) {
        let _ = self.reactor_sender.send(waker);
    }
}
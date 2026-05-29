use std::{
    sync::{Arc, Mutex, mpsc::Sender},
    task::Waker,
};

#[derive(Clone)] // Now we can clone this handle easily!
pub struct MyTcpListener {
    // Wrap the mio listener in an Arc so it can be shared cleanly
    pub listener: Arc<Mutex<mio::net::TcpListener>>,
    reactor_sender: Option<Sender<Waker>>,
}

impl MyTcpListener {
    pub fn new() -> Self {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let raw_listener = mio::net::TcpListener::bind(addr).unwrap();
        let listener = Arc::new(Mutex::new(raw_listener));
        MyTcpListener {
            listener,
            reactor_sender: None,
        }
    }

    pub fn accept(&self) -> std::io::Result<(mio::net::TcpStream, std::net::SocketAddr)> {
        let lock = self.listener.lock().unwrap();
        lock.accept()
    }

    pub fn reactor_send(&self, waker: Waker) {
        if let Some(sender) = self.reactor_sender.as_ref() {
            let _ = sender.send(waker);
        } else {
            panic!("Reactor sender not set in MyTcpListener");
        }
    }
}

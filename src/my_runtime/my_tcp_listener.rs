use std::{sync::mpsc::Sender, task::Waker};

pub struct MyTcpListener {
    pub listener: mio::net::TcpListener,
    reactor_sender: Sender<Waker>,
}

impl MyTcpListener {
    pub fn new(reactor_sender: Sender<Waker>) -> Self {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let listener = mio::net::TcpListener::bind(addr).unwrap();
        MyTcpListener {
            listener,
            reactor_sender,
        }
    }

    pub fn accept(&self) -> std::io::Result<(mio::net::TcpStream, std::net::SocketAddr)> {
        self.listener.accept()
    }

    pub fn reactor_send(&self, waker: Waker) {
        let _ = self.reactor_sender.send(waker);
    }
}

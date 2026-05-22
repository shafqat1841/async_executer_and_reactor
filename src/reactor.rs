// reactor.rs
use mio::{Events, Poll, Token, Interest};
use std::sync::mpsc::Receiver;
use std::task::Waker;
use std::time::Duration;
use std::io;

pub struct Reactor {
    pub poll: Poll,
    pub events: Events,
    pub listener: mio::net::TcpListener,
    pub waker_receiver: Receiver<Waker>,
}

impl Reactor {
    pub fn tick(&mut self) -> bool {
        println!("--- Reactor Tick: OS epoll blocking for real events ---");

        let mut has_waker = false;
        while let Ok(_waker) = self.waker_receiver.try_recv() {
            has_waker = true;
        }

        if has_waker {
            let _ = self.poll.registry().deregister(&mut self.listener);
            let _ = self.poll.registry().register(
                &mut self.listener,
                Token(0),
                Interest::READABLE,
            );
        }

        self.poll.poll(&mut self.events, Some(Duration::from_secs(30))).unwrap();

        let mut data_ready = false;
        for event in self.events.iter() {
            if event.token() == Token(0) && event.is_readable() {
                println!("Reactor: OS signaled data is ready on Port 8080!");
                data_ready = true;
            }
        }
        
        data_ready
    }

    // Add this helper method so the future can securely borrow the socket
    pub fn accept_stream(&self) -> io::Result<(mio::net::TcpStream, std::net::SocketAddr)> {
        self.listener.accept()
    }
}
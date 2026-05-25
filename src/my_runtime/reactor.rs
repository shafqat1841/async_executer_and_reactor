// my_runtime/reactor.rs
use mio::{Events, Interest, Poll, Token};
use std::sync::mpsc::Receiver;
use std::task::Waker;
use std::time::Duration;

use crate::my_runtime::my_tcp_listener::MyTcpListener;

pub struct Reactor {
    pub poll: Poll,
    pub events: Events,
    pub waker_receiver: Receiver<Waker>,
}

impl Reactor {
    pub fn new(waker_receiver: Receiver<Waker>) -> Self {
        let poll = Poll::new().unwrap();
        let events = Events::with_capacity(1024);

        Reactor {
            poll,
            events,
            waker_receiver,
        }
    }

    pub fn tick(&mut self, listener: &mut MyTcpListener) -> bool {
        println!("--- Reactor Tick: OS epoll blocking for real events ---");

        let mut has_waker = false;
        while let Ok(_waker) = self.waker_receiver.try_recv() {
            has_waker = true;
        }

        if has_waker {
            if let Ok(mut guard) = listener.listener.lock() {
                let _ = self.poll.registry().deregister(&mut *guard);
                let _ = self
                    .poll
                    .registry()
                    .register(&mut *guard, Token(0), Interest::READABLE);
            }
        }

        self.poll
            .poll(&mut self.events, Some(Duration::from_secs(30)))
            .unwrap();

        let mut data_ready = false;
        for event in self.events.iter() {
            if event.token() == Token(0) && event.is_readable() {
                println!("Reactor: OS signaled data is ready on Port 8080!");
                data_ready = true;
            }
        }

        data_ready
    }
}

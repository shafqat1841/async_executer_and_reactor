use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::sync::{Arc, Mutex};

pub struct Reactor {
    pub poll: Poll,
    pub events: Events,
}

impl Reactor {
    pub fn new() -> Self {
        let poll = Poll::new().unwrap();
        let events = Events::with_capacity(1024);

        Reactor { poll, events }
    }

    pub fn check_tcp_resource(&mut self, my_tcp_listener: &mut Arc<Mutex<TcpListener>>) -> bool {
        println!("--- Reactor Tick: OS epoll blocking for real events ---");

        if let Ok(mut listener) = my_tcp_listener.lock() {
            let _ = self.poll.registry().deregister(&mut *listener);
            let _ = self
                .poll
                .registry()
                .register(&mut *listener, Token(0), Interest::READABLE);
        }

        self.poll
            // .poll(&mut self.events, Some(Duration::from_secs(5)))
            .poll(&mut self.events, None)
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

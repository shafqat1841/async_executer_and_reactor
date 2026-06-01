use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::sync::{Arc, Mutex, mpsc};
use std::task::Waker;
use std::time::Duration;

pub struct Registration {
    pub listener: Arc<Mutex<TcpListener>>,
    pub token: Token,
    pub waker: Waker,
}

pub struct Reactor {
    pub poll: Poll,
    pub events: Events,
    registration_receiver: mpsc::Receiver<Registration>,
}

impl Reactor {
    pub fn new(registration_receiver: mpsc::Receiver<Registration>) -> Self {
        let poll = Poll::new().unwrap();
        let events = Events::with_capacity(1024);

        Reactor {
            poll,
            events,
            registration_receiver,
        }
    }

    pub fn run_loop(&mut self) {
        let mut wakers_map: std::collections::HashMap<Token, Waker> =
            std::collections::HashMap::new();

        loop {
            while let Ok(reg) = self.registration_receiver.try_recv() {
                if let Ok(mut guard) = reg.listener.lock() {
                    let _ = self.poll.registry().deregister(&mut *guard);
                    let _ = self
                        .poll
                        .registry()
                        .register(&mut *guard, reg.token, Interest::READABLE)
                        .unwrap();
                    wakers_map.insert(reg.token, reg.waker);
                }
            }

            self.poll
                .poll(&mut self.events, Some(Duration::from_millis(100)))
                .unwrap();

            for event in self.events.iter() {
                if event.is_readable() {
                    if let Some(waker) = wakers_map.remove(&event.token()) {
                        println!(
                            "Reactor: Resource ready for Token({:?})! Waking task...",
                            event.token()
                        );
                        waker.wake();
                    }
                }
            }
        }
    }
}

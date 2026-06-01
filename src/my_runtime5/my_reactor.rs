// my_runtime4/my_reactor.rs
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};
use std::task::Waker as StdWaker;

pub const WAKER_TOKEN: Token = Token(0);

#[derive(Debug)]
pub struct Registration {
    pub listener: Arc<Mutex<TcpListener>>,
    pub token: Token,
    pub waker: StdWaker,
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
        let mut wakers_map: HashMap<Token, StdWaker> = HashMap::new();

        loop {
            self.poll.poll(&mut self.events, None).unwrap();

            let mut check_registrations = false;

            for event in self.events.iter() {
                dbg!(event.token());
                if event.token() == WAKER_TOKEN {
                    check_registrations = true;
                } else if event.is_readable() {
                    if let Some(waker) = wakers_map.remove(&event.token()) {
                        waker.wake();
                    }
                }
            }

            if check_registrations {
                dbg!(check_registrations);
                while let Ok(reg) = self.registration_receiver.try_recv() {
                    dbg!(reg.token);
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
            }
        }
    }
}

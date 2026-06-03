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
    registered_tokens: HashMap<Token, Arc<Mutex<TcpListener>>>,
}

impl Reactor {
    pub fn new(registration_receiver: mpsc::Receiver<Registration>) -> Self {
        Reactor {
            poll: Poll::new().unwrap(),
            events: Events::with_capacity(1024),
            registration_receiver,
            registered_tokens: HashMap::new(),
        }
    }

    pub fn run_loop(&mut self) {
        let mut wakers_map: HashMap<Token, StdWaker> = HashMap::new();

        loop {
            self.poll.poll(&mut self.events, None).unwrap();

            for event in self.events.iter() {
                if event.token() != WAKER_TOKEN && event.is_readable() {
                    if let Some(waker) = wakers_map.remove(&event.token()) {
                        waker.wake();
                    }
                }
            }
            // Always check for incoming registrations to avoid missing wakeups
            while let Ok(reg) = self.registration_receiver.try_recv() {
                if let Ok(mut guard) = reg.listener.lock() {
                    // Check if this token was already registered with OS
                    if self.registered_tokens.contains_key(&reg.token) {
                        let _ = self.poll.registry().reregister(
                            &mut *guard,
                            reg.token,
                            Interest::READABLE,
                        );
                    } else {
                        let _ = self.poll.registry().register(
                            &mut *guard,
                            reg.token,
                            Interest::READABLE,
                        );
                        self.registered_tokens
                            .insert(reg.token, reg.listener.clone());
                    }
                    wakers_map.insert(reg.token, reg.waker);
                }
            }
        }
    }
}

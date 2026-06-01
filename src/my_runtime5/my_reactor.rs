// my_runtime4/my_reactor.rs
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token, Waker as MioWaker}; // Import mio's Waker
use std::collections::HashMap;
use std::sync::{Arc, Mutex, mpsc};
use std::task::Waker as StdWaker;

// A dedicated token for waking up the event loop when registrations arrive
pub const WAKER_TOKEN: Token = Token(0);

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

        // 1. Create a mio::Waker and register it inside the Poll instance
        // This links the waker explicitly to WAKER_TOKEN.
        let _ = MioWaker::new(self.poll.registry(), WAKER_TOKEN).unwrap();

        // Share this waker handle globally or store it safely. For your current runtime,
        // we can keep it inside the reactor thread scope if registrations are paired with it.
        // However, the thread sending the registration needs a way to call it!
        // Let's pass it out or bundle it so the runtime can give copies to the listeners.

        loop {
            // 2. BLOCK INDEFINITELY (0% CPU Burn)
            // Passing `None` instructs the OS to put this thread to sleep until an event fires.
            self.poll.poll(&mut self.events, None).unwrap();

            // 3. Check what woke us up
            let mut check_registrations = false;

            for event in self.events.iter() {
                if event.token() == WAKER_TOKEN {
                    // It was our cross-thread registration waker!
                    check_registrations = true;
                } else if event.is_readable() {
                    // It was a network resource socket that became ready
                    if let Some(waker) = wakers_map.remove(&event.token()) {
                        println!(
                            "Reactor: Resource ready for Token({:?})! Waking task...",
                            event.token()
                        );
                        waker.wake(); // Pushes the task back to the executor queue
                    }
                }
            }

            // 4. If we were awoken by a registration request, drain the channel
            if check_registrations {
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
            }
        }
    }
}

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

    pub fn check_tcp_resource(&mut self, my_tcp_listener: &mut Arc<Mutex<TcpListener>>) -> bool {
        println!("--- Reactor Tick: OS epoll blocking for real events ---");

        let mut wakers_map: std::collections::HashMap<Token, Waker> =
            std::collections::HashMap::new();

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

    pub fn run_loop(&mut self) {
        // Map to correlate incoming OS Tokens with their corresponding Task Wakers
        let mut wakers_map: std::collections::HashMap<Token, Waker> =
            std::collections::HashMap::new();

        loop {
            // 1. Check for any new futures registering their wakers
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

            // 2. Sleep via OS kernel until a hardware network event drops in
            self.poll
                .poll(&mut self.events, Some(Duration::from_millis(100)))
                .unwrap();

            // 3. Fire the precise wakers for the resources that are finally ready
            for event in self.events.iter() {
                if event.is_readable() {
                    if let Some(waker) = wakers_map.remove(&event.token()) {
                        println!(
                            "Reactor: Resource ready for Token({:?})! Waking task...",
                            event.token()
                        );
                        waker.wake(); // Pushes the task straight back to your Executor Queue!
                    }
                }
            }
        }
    }
}

use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self, Sender},
    },
    task::Waker,
};

use crate::my_runtime::executor::{Executor, Task};
pub use crate::my_runtime::{my_tcp_listener::MyTcpListener, reactor::Reactor};

pub struct MyRuntime {
    reactor: Reactor,
    executor: Executor,
    listener: MyTcpListener,
}

impl MyRuntime {
    pub fn new() -> Self {
        let (reactor_sender, reactor_receiver) = mpsc::channel::<Waker>();
        let reactor = Reactor::new(reactor_receiver);
        let executor = Executor::new();

        let listener = Self::make_shared_listener(reactor_sender.clone());

        MyRuntime {
            reactor,
            executor,
            listener,
        }
    }

    fn make_shared_listener(reactor_sender: Sender<Waker>) -> MyTcpListener {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let raw_listener = mio::net::TcpListener::bind(addr).unwrap();
        let shared_listener = Arc::new(Mutex::new(raw_listener));

        let listener: MyTcpListener = MyTcpListener::new(shared_listener, reactor_sender.clone());
        listener
    }

    pub fn get_tcp_listener_mut(&mut self) -> MyTcpListener {
        self.listener.clone()
    }

    pub fn spawn<F>(&mut self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task: Task = Box::pin(future);
        self.executor.spawn(task);

        self.executor.run(&mut self.reactor, &mut self.listener);
    }
}

mod executor;
mod my_tcp_listener;
// mod process_data_state;
mod reactor;

use std::{
    sync::mpsc::{self, Sender},
    task::{self, Waker},
};

pub use crate::my_runtime::{my_tcp_listener::MyTcpListener, reactor::Reactor};
use crate::{my_runtime::executor::Executor, types::Task};

pub struct MyRuntime {
    // pub tcp_listener: MyTcpListener,
    reactor_sender: Sender<Waker>,
    reactor: Reactor,
    executor: Executor,
}

impl MyRuntime {
    pub fn new() -> Self {
        let (reactor_sender, reactor_reciver) = mpsc::channel::<Waker>();

        let reactor = Reactor::new(reactor_reciver);

        let executor = Executor::new();

        MyRuntime {
            reactor,
            reactor_sender,
            executor,
        }
    }

    pub fn get_tcp_listener_mut(&mut self) -> MyTcpListener {
        let reactor_sender = self.reactor_sender.clone();
        let tcp_listener: MyTcpListener = MyTcpListener::new(reactor_sender);
        tcp_listener
    }

    pub fn spawn(&mut self, task: Task) {
        self.executor.spawn(task);
    }

    pub fn run(&mut self) {
        let mut tcp_listener: MyTcpListener = self.get_tcp_listener_mut();
        self.executor
            .run(&mut self.reactor, &mut tcp_listener);
    }
}

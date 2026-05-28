use std::{
    io, sync::{
        Arc, Mutex,
        mpsc::{self, Sender},
    }, task::Waker
};

use crate::my_runtime::{executor::{Executor, Task}, process_data_state::ProcessDataRes};
pub use crate::my_runtime::{my_tcp_listener::MyTcpListener, reactor::Reactor};

pub struct MyRuntime {
    executor: Executor,
    reactor_sender: mpsc::Sender<Arc<Task>>, // listener: MyTcpListener,
}

impl MyRuntime {
    pub fn new() -> Self {
        let (reactor_sender, reactor_receiver) = mpsc::channel::<Arc<Task>>();
        let reactor = Reactor::new(reactor_receiver);
        let executor = Executor::new(reactor);

        MyRuntime {
            executor,
            reactor_sender,
        }
    }

    pub fn spawn<F>(&mut self, future: F) -> ()
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let executor: mpsc::Sender<Arc<Task>> = self.reactor_sender.clone();
        let task = Task::new(future, executor);
        let arc_task = Arc::new(task);

        let _ = self.reactor_sender.send(arc_task);

        self.executor.run();
    }
}

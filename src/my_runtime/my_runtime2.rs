use std::future::Future;
use std::sync::{Arc, Mutex, mpsc};

use futures::future::BoxFuture;
use futures::task::{self, ArcWake};
use std::task::Context;

use crate::my_runtime::my_task::MyTask;

pub struct MyRuntime2 {
    scheduled: mpsc::Receiver<Arc<MyTask>>,

    sender: mpsc::Sender<Arc<MyTask>>,
}

impl MyRuntime2 {
    pub fn new() -> MyRuntime2 {
        let (sender, scheduled) = mpsc::channel();

        MyRuntime2 { scheduled, sender }
    }

    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(MyTask {
            future: Mutex::new(Box::pin(future)),
            executor: self.sender.clone(),
        });

        let _ = self.sender.send(task);

        self.run();
    }

    pub fn run(&self) {
        while let Ok(task) = self.scheduled.recv() {
            let waker = task::waker(task.clone());

            let mut cx = Context::from_waker(&waker);

            let mut future = task.future.try_lock().unwrap();

            let _ = future.as_mut().poll(&mut cx);
        }
    }
}

use std::future::Future;
use std::sync::{Arc, Mutex, mpsc};

use futures::future::BoxFuture;
use futures::task::{self, ArcWake};
use std::task::{Context, Poll};

use crate::my_runtime::my_reactor::Reactor;
use crate::my_runtime::my_task::MyTask;

pub struct MyRuntime2 {
    scheduled: mpsc::Receiver<Arc<MyTask>>,

    sender: mpsc::Sender<Arc<MyTask>>,

     reactor: Arc<Mutex<Reactor>>
}

impl MyRuntime2 {
    pub fn new() -> MyRuntime2 {
        let (sender, scheduled) = mpsc::channel();

        let reactor: Arc<Mutex<Reactor>> = Arc::new(Mutex::new(Reactor::new()));

        MyRuntime2 { scheduled, sender, reactor }
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

    pub fn run(&self) -> Option<()> {

        let mut result = None;

        while let Ok(task) = self.scheduled.recv() {
            let waker = task::waker(task.clone());

            let mut cx = Context::from_waker(&waker);

            let mut future = task.future.try_lock().unwrap();

            let res: Poll<()> = future.as_mut().poll(&mut cx);

            match res {
                Poll::Pending => {
                    println!("Task is still pending, will check again later.");
                }
                Poll::Ready(res) => {
                    // println!("file: my_runtime2.rs - line 57 - Poll::Ready - res : {:?} ", res);
                    // println!("Task completed!");
                    result = Some(res);
                    break;

                }
            }
        }

        result
    }
}

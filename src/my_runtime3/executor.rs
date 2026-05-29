use std::future::Future;
use std::sync::{Arc, Mutex, mpsc};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::{Duration};
use std::{thread};

use futures::future::BoxFuture;
// use futures::lock::Mutex;
use futures::task::{self, ArcWake};

use crate::my_runtime::reactor::Reactor;

// pub type Task = Pin<Box<dyn Future<Output = ()>>>;

pub struct Task {
    pub future: Mutex<BoxFuture<'static, ()>>,

    pub executor: mpsc::Sender<Arc<Task>>,
}

impl Task {
    pub fn new<F>(future: F, executor: mpsc::Sender<Arc<Task>>) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Task {
            future: Mutex::new(Box::pin(future)),
            executor, // Placeholder, will be set properly in MyRuntime
        }
    }
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let _ = arc_self.executor.send(arc_self.clone());
    }
}

pub struct Executor {
    reactor: Reactor,
    has_waker: bool,
}

impl Executor {
    pub fn new(reactor: Reactor) -> Self {
        Executor {
            reactor,
            has_waker: false,
        }
    }

    pub fn run(&mut self) -> () {
        while let Ok(task) = self.reactor.waker_receiver.recv() {
            let waker = task::waker(task.clone());

            let mut cx = Context::from_waker(&waker);

            let mut future = task.future.try_lock().unwrap();

            match future.as_mut().poll(&mut cx) {
                Poll::Pending => {
                    println!("Executor: Task is still pending, will check again later.");
                    if !self.has_waker {
                        let waker = Arc::new(Mutex::new(cx.waker().clone()));

                        self.has_waker = true;

                        thread::spawn(move || {
                            thread::sleep(Duration::from_secs(30));

                            let waker = waker.lock().unwrap();

                            waker.wake_by_ref();
                        });
                    }
                }
                Poll::Ready(_) => {
                    println!("Executor: Task completed, removing from queue.");
                }
            }
        }
    }
}

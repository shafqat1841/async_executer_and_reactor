use std::future::Future;
use std::sync::{Arc, Mutex, mpsc};

use futures::future::BoxFuture;
use futures::task::{self, ArcWake};
use std::task::Context;

pub struct Task {
    pub future: Mutex<BoxFuture<'static, ()>>,

    pub executor: mpsc::Sender<Arc<Task>>,
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let _ = arc_self.executor.send(arc_self.clone());
    }
}

pub struct MiniTokio {
    scheduled: mpsc::Receiver<Arc<Task>>,

    sender: mpsc::Sender<Arc<Task>>,
}

impl MiniTokio {
    pub fn new() -> MiniTokio {
        let (sender, scheduled) = mpsc::channel();

        MiniTokio { scheduled, sender }
    }

    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(Task {
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

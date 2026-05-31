use std::sync::{Arc, Mutex, mpsc};

use futures::{future::BoxFuture, task::ArcWake};

pub struct MyTask {
    pub future: Mutex<BoxFuture<'static, ()>>,

    pub executor: mpsc::Sender<Arc<MyTask>>,
}

impl ArcWake for MyTask {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let _ = arc_self.executor.send(arc_self.clone());
    }
}

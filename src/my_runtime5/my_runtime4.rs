use std::future::Future;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use futures::task::{self};
use std::task::Context;

use crate::my_runtime5::my_reactor::{Reactor, Registration, WAKER_TOKEN};
use crate::my_runtime5::my_task::MyTask;

use mio::Waker as MioWaker;

pub struct MyRuntime4 {
    scheduled: mpsc::Receiver<Arc<MyTask>>,

    sender: mpsc::Sender<Arc<MyTask>>,

    pub reactor_sender: mpsc::Sender<Registration>,

    pub mio_waker: Arc<MioWaker>,
}

impl MyRuntime4 {
    pub fn new() -> MyRuntime4 {
        let (sender, scheduled) = mpsc::channel();
        let (reactor_sender, reactor_receiver) = mpsc::channel::<Registration>();

        let mut reactor = Reactor::new(reactor_receiver);

        let mio_waker = Arc::new(MioWaker::new(reactor.poll.registry(), WAKER_TOKEN).unwrap());
        let mio_waker_clone = mio_waker.clone();

        thread::spawn(move || {
            reactor.run_loop();
        });

        MyRuntime4 {
            scheduled,
            sender,
            reactor_sender,
            mio_waker: mio_waker_clone,
        }
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

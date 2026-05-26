use futures::future::BoxFuture;
// A utility that allows us to implement a `std::task::Waker` without having to
// use `unsafe` code.
use futures::task::{self, ArcWake};
use std::future::Future;
use std::sync::{Arc, Mutex, mpsc};
use std::task::Context;

// Task harness. Contains the future as well as the necessary data to schedule
// the future once it is woken.
pub struct Task {
    // The future is wrapped with a `Mutex` to make the `Task` structure `Sync`.
    // There will only ever be a single thread that attempts to use `future`.
    // The Tokio runtime avoids the mutex by using `unsafe` code. The box is
    // also avoided.

    // BoxFuture<T> is a type alias for:
    // Pin<Box<dyn Future<Output = T> + Send + 'static>>
    future: Mutex<BoxFuture<'static, ()>>,

    // When a task is notified, it is queued into this channel. The executor
    // pops notified tasks and executes them.
    executor: mpsc::Sender<Arc<Task>>,
}

impl Task {
    // Spawns a new task with the given future.
    //
    // Initializes a new Task harness containing the given future and pushes it
    // onto `sender`. The receiver half of the channel will get the task and
    // execute it.
    pub fn spawn<F>(future: F, sender: &mpsc::Sender<Arc<Task>>)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(future)),
            executor: sender.clone(),
        });

        let _ = sender.send(task);
    }

    // Execute a scheduled task. This creates the necessary `task::Context`
    // containing a waker for the task. This waker pushes the task onto the
    // mini-tokio scheduled channel. The future is then polled with the waker.
    pub fn poll(self: Arc<Self>) {
        // Get a waker referencing the task.
        let waker = task::waker(self.clone());
        // Initialize the task context with the waker.
        let mut cx = Context::from_waker(&waker);

        // This will never block as only a single thread ever locks the future.
        let mut future = self.future.try_lock().unwrap();

        // Poll the future
        let _ = future.as_mut().poll(&mut cx);
    }
}

// The standard library provides low-level, unsafe  APIs for defining wakers.
// Instead of writing unsafe code, we will use the helpers provided by the
// `futures` crate to define a waker that is able to schedule our `Task`
// structure.
impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        // Schedule the task for execution. The executor receives from the
        // channel and polls tasks.
        let _ = arc_self.executor.send(arc_self.clone());
    }
}

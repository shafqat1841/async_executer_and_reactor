use std::future::Future;
use std::sync::{Arc, mpsc};

use crate::mini_tokio::constants::CURRENT;
use crate::mini_tokio::task::Task;

/// A very basic futures executor based on a channel. When tasks are woken, they
/// are scheduled by queuing them in the send half of the channel. The executor
/// waits on the receive half and executes received tasks.
///
/// When a task is executed, the send half of the channel is passed along via
/// the task's Waker.
pub struct MiniTokio {
    // Receives scheduled tasks. When a task is scheduled, the associated future
    // is ready to make progress. This usually happens when a resource the task
    // uses becomes ready to perform an operation. For example, a socket
    // received data and a `read` call will succeed.
    scheduled: mpsc::Receiver<Arc<Task>>,

    // Send half of the scheduled channel.
    sender: mpsc::Sender<Arc<Task>>,
}

impl MiniTokio {
    /// Initialize a new mini-tokio instance.
    pub fn new() -> MiniTokio {
        let (sender, scheduled) = mpsc::channel();

        MiniTokio { scheduled, sender }
    }

    /// Spawn a future onto the mini-tokio instance.
    ///
    /// The given future is wrapped with the `Task` harness and pushed into the
    /// `scheduled` queue. The future will be executed when `run` is called.
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Task::spawn(future, &self.sender);
    }

    /// Run the executor.
    ///
    /// This starts the executor loop and runs it indefinitely. No shutdown
    /// mechanism has been implemented.
    ///
    /// Tasks are popped from the `scheduled` channel receiver. Receiving a task
    /// on the channel signifies the task is ready to be executed. This happens
    /// when the task is first created and when its waker has been used.
    pub fn run(&self) {
        // Set the CURRENT thread-local to point to the current executor.
        //
        // Tokio uses a thread-local variable to implement `tokio::spawn`. When
        // entering the runtime, the executor stores necessary context with the
        // thread-local to support spawning new tasks.
        CURRENT.with(|cell| {
            *cell.borrow_mut() = Some(self.sender.clone());
        });

        // The executor loop. Scheduled tasks are received. If the channel is
        // empty, the thread blocks until a task is received.
        while let Ok(task) = self.scheduled.recv() {
            // Execute the task until it either completes or cannot make further
            // progress and returns `Poll::Pending`.
            task.poll();
        }
    }
}

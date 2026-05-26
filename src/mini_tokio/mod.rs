//! Demonstrates how to implement a (very) basic asynchronous rust executor and
//! timer. The goal of this file is to provide some context into how the various
//! building blocks fit together.

mod mini_tokio_runtime;
mod task;
mod constants;
mod process_data;

use std::time::{Duration};

use mini_tokio_runtime::MiniTokio;

use crate::mini_tokio::constants::CURRENT;
use crate::mini_tokio::process_data::delay;
use crate::mini_tokio::task::Task;


// Main entry point. A mini-tokio instance is created and a few tasks are
// spawned. Our mini-tokio implementation only supports spawning tasks and
// setting delays.
pub fn main_run() {
    // Create the mini-tokio instance.
    let mini_tokio = MiniTokio::new();

    // Spawn the root task. All other tasks are spawned from the context of this
    // root task. No work happens until `mini_tokio.run()` is called.
    mini_tokio.spawn(async {
        // Spawn a task
        spawn(async {
            // Wait for a little bit of time so that "world" is printed after
            // "hello"
            delay(Duration::from_millis(100)).await;
            println!("world");
        });

        // Spawn a second task
        spawn(async {
            println!("hello");
        });

        // We haven't implemented executor shutdown, so force the process to exit.
        delay(Duration::from_millis(200)).await;
        std::process::exit(0);
    });

    // Start the mini-tokio executor loop. Scheduled tasks are received and
    // executed.
    mini_tokio.run();
}


// An equivalent to `tokio::spawn`. When entering the mini-tokio executor, the
// `CURRENT` thread-local is set to point to that executor's channel's Send
// half. Then, spawning requires creating the `Task` harness for the given
// `future` and pushing it into the scheduled queue.
pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    CURRENT.with(|cell| {
        let borrow = cell.borrow();
        let sender = borrow.as_ref().unwrap();
        Task::spawn(future, sender);
    });
}
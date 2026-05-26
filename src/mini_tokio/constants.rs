use std::cell::RefCell;
use std::sync::{Arc, mpsc};
use crate::mini_tokio::task::Task;

// Used to track the current mini-tokio instance so that the `spawn` function is
// able to schedule spawned tasks.
thread_local! {
    pub static CURRENT: RefCell<Option<mpsc::Sender<Arc<Task>>>> =
        RefCell::new(None);
}
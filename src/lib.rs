// lib.rs
mod executor;
mod process_data_state;
mod reactor;

use crate::{
    executor::{Executor, Task},
    process_data_state::process_data,
    reactor::Reactor,
};
use std::{sync::mpsc, task::Waker};

pub fn run() {
    let addr = "127.0.0.1:8080".parse().unwrap();
    let listener = mio::net::TcpListener::bind(addr).unwrap();

    let (tx, rx) = mpsc::channel::<Waker>();

    // 1. Construct the Reactor first
    let mut reactor = Reactor::new(listener, rx);

    // 2. Safely obtain a pointer to where the reactor lives in memory
    let reactor_ptr: *const Reactor = &reactor;

    // 3. Hand that pointer over to your async pipeline
    let future_instance = process_data(reactor_ptr, tx);
    let task: Task = Box::pin(future_instance);

    // let mut executor = Executor { tasks: vec![task] };
    let mut executor = Executor::new();
    executor.spawn(task);

    // 4. Fire up the engines
    executor.run(&mut reactor);
}

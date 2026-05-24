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

    let mut reactor = Reactor::new(listener, rx);

    let reactor_ptr: *const Reactor = &reactor;

    let future_instance = process_data(reactor_ptr, tx);
    let task: Task = Box::pin(future_instance);

    let mut executor = Executor::new();
    executor.spawn(task);

    executor.run(&mut reactor);
}

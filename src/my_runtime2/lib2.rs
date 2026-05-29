// lib.rs
mod my_runtime;
mod process_data_state;
mod types;

use crate::{
    my_runtime::{MyRuntime, MyTcpListener},
    process_data_state::process_data,
    types::Task,
};

pub fn main_run() {
    let mut runtime = MyRuntime::new();

    let my_tcp_listener: MyTcpListener = runtime.get_tcp_listener_mut();

    let future_instance = process_data(my_tcp_listener);
    let task: Task = Box::pin(future_instance);

    runtime.spawn(task);

    runtime.run();
}

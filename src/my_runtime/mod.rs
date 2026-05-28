mod executor;
mod mini_runtime;
mod my_tcp_listener;
mod process_data_state;
mod reactor;

use crate::my_runtime::{
    mini_runtime::{MyRuntime, MyTcpListener},
    process_data_state::process_data,
};

pub fn main_run2() {
    let mut runtime = MyRuntime::new();

    let my_tcp_listener: MyTcpListener = runtime.get_tcp_listener_mut();

    let future = process_data(my_tcp_listener);

    runtime.spawn(future);
}

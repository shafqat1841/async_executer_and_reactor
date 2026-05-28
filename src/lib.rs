mod mini_tokio;
mod my_runtime;
use crate::mini_tokio::main_run;
use crate::my_runtime::main_run2;

pub fn run() {
    main_run2();
    main_run();
}
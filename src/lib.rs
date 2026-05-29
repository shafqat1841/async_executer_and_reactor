mod mini_tokio;
mod my_runtime;

#[warn(unused_imports)]
use crate::my_runtime::main_run;

pub fn run() {
    main_run();
}
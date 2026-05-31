mod mini_tokio;
mod my_runtime4;

#[warn(unused_imports)]
use crate::my_runtime4::main_run;

pub fn run() {
    main_run();
}
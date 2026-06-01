mod mini_tokio;
mod my_runtime5;

#[warn(unused_imports)]
use crate::my_runtime5::main_run;

pub fn run() {
    main_run();
}
mod mini_tokio_runtime;
mod process_data;

use std::time::Duration;

use mini_tokio_runtime::MiniTokio;
use crate::mini_tokio::process_data::delay;
pub fn main_run() {
    let mini_tokio = MiniTokio::new();
    mini_tokio.spawn(async {
        delay(Duration::from_millis(500)).await;
        println!("world");

        delay(Duration::from_millis(1000)).await;
        println!("hello");
        std::process::exit(0);
    });
    mini_tokio.run();
}
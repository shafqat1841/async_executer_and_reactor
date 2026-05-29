use crate::my_runtime::{my_runtime2::MyRuntime2, my_tcp_listener::MyTcpListener};

mod my_runtime2;
mod my_task;
mod my_tcp_listener;
mod my_reactor;

pub fn main_run() {
    let runtime = MyRuntime2::new();

    let futue = async {
        println!("Hello from the future!");
        let addr = "127.0.0.1:8080".parse().unwrap();
        let listener = MyTcpListener::new(addr);

        let stream = listener.await;

        match stream {
            Ok(stream) => println!("Client connected! stream: {:?}", stream),
            Err(e) => println!("Error accepting connection: {}", e),
        }
    };

    runtime.spawn(futue);
    println!("main_run2");
}

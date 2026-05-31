
mod my_runtime4;
mod my_task;
mod my_tcp_listener;
mod my_reactor;


use crate::my_runtime4::{my_runtime4::MyRuntime4, my_tcp_listener::MyTcpListener};

pub fn main_run() {
    let runtime = MyRuntime4::new();

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

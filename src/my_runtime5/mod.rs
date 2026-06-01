mod my_reactor;
mod my_runtime4;
mod my_task;
mod my_tcp_listener;

use crate::my_runtime5::{my_runtime4::MyRuntime4, my_tcp_listener::MyTcpListener};

pub fn main_run() {
    let runtime = MyRuntime4::new();

    let reactor_sender = runtime.reactor_sender.clone();

    let mio_waker = runtime.mio_waker.clone(); // Clone the waker to pass to listeners

    let futue = async move {
        println!("Hello from the future!");

        let addr = "127.0.0.1:8080".parse().unwrap();
        let mut listener = MyTcpListener::new(addr, reactor_sender.clone(), mio_waker.clone());
        loop {
            listener.has_waker = false;

            println!("\n--- Awaiting next connection request... ---");
            let stream = (&mut listener).await; // Use &mut to preserve listener state

            match stream {
                Ok(stream) => {
                    println!("Client connected! stream: {:?}", stream);
                }
                Err(e) => println!("Error accepting connection: {}", e),
            }
        }
    };

    runtime.spawn(futue);
    println!("main_run2");
}

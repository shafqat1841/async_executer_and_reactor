mod my_reactor;
mod my_runtime4;
mod my_task;
mod my_tcp_listener;

use crate::my_runtime6::{my_runtime4::MyRuntime4, my_tcp_listener::MyTcpListener};

pub fn main_run() {
    let runtime = MyRuntime4::new();

    let reactor_sender = runtime.reactor_sender.clone();

    let mio_waker = runtime.mio_waker.clone(); // Clone the waker to pass to listeners

    let futue = async move {
        let addr = "127.0.0.1:8080".parse().unwrap();
        let mut listener = MyTcpListener::new(addr, reactor_sender.clone(), mio_waker.clone());
        loop {
            println!("--- Awaiting next connection request... ---");
            // Encapsulation is preserved; state is tracked internally
            let stream = (&mut listener).await;

            match stream {
                Ok(mut stream) => {
                    use std::io::Read;
                    let mut buffer = [0; 1024];
                    if let Ok(bytes_read) = stream.read(&mut buffer) {
                        let request_str = String::from_utf8_lossy(&buffer[..bytes_read]);
                        println!("Request received:\n{}", request_str);
                    }
                }
                Err(e) => {
                    eprintln!("Runtime error accepting stream: {:?}", e);
                }
            }
        }
    };

    runtime.spawn(futue);
    dbg!("main_run2");
}

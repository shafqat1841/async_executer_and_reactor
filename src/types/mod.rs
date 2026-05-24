use std::pin::Pin;

pub type Task = Pin<Box<dyn Future<Output = ()>>>;
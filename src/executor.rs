// executor.rs
use crate::reactor::Reactor;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

pub type Task = Pin<Box<dyn Future<Output = ()>>>;

pub struct Executor {
    pub tasks: Vec<Task>,
}

impl Executor {
    pub fn run(&mut self, reactor: &mut Reactor) {
        let waker = dummy_waker();
        let mut cx = Context::from_waker(&waker);

        while !self.tasks.is_empty() {
            self.tasks.retain_mut(|task| {
                match task.as_mut().poll(&mut cx) {
                    Poll::Pending => true, // Keep waiting
                    Poll::Ready(_) => false, // Remove completed task
                }
            });

            if !self.tasks.is_empty() {
                // If a network request actually lands, the reactor wakes up and returns true
                let is_ready = reactor.tick();
                if is_ready {
                    // Force another execution round
                    continue;
                }
            }
        }
        println!("Executor: All tasks complete.");
    }
}

fn dummy_waker() -> Waker {
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}
const VTABLE: RawWakerVTable = RawWakerVTable::new(|_| RawWaker::new(std::ptr::null(), &VTABLE), |_| {}, |_| {}, |_| {});
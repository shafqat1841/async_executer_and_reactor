use core::time;
use std::thread::sleep;

pub enum ProcessDataState {
    Start,
    Waiting(u64), // Imagine this is a timestamp it's waiting for
    Done,
}

impl ProcessDataState {
    // This is a simplified version of the 'poll' method
    pub fn poll(&mut self, current_time: u64) -> String {
        match self {
            Self::Start => {
                println!("Step 1: Starting Task...");
                *self = Self::Waiting(current_time + 2); // Wait 2 "seconds"
                "Pending".to_string()
            }
            Self::Waiting(target_time) => {
                if current_time >= *target_time {
                    println!("Step 2: Time reached! Task Finished.");
                    *self = Self::Done;
                    "Ready".to_string()
                } else {
                    "Pending".to_string()
                }
            }
            Self::Done => "Already Finished".to_string(),
        }
    }
}


pub struct Executor {
    pub tasks: Vec<ProcessDataState>,
}

impl Executor {
    pub fn run(&mut self, reactor: &mut Reactor) {
        // Keep looping as long as we have unfinished tasks
        while self.tasks.iter().any(|t| !matches!(t, ProcessDataState::Done)) {
            
            for task in self.tasks.iter_mut() {
                let status = task.poll(reactor.current_time);
                
                if status == "Pending" {
                    println!("Executor: Task is blocked, moving to next...");
                }
            }
            
            // If tasks are pending, the Reactor waits for something to happen
            reactor.tick();
        }
        println!("Executor: All tasks complete.");
    }
}

pub struct Reactor {
    pub current_time: u64,
}

impl Reactor {
    // The reactor simulates time passing
    pub fn tick(&mut self) {
        self.current_time += 1;
        println!("--- Reactor Tick: {} ---", self.current_time);
    }
}

pub fn simple_run() {
    let tasks = vec![ProcessDataState::Start];
    // let x = processData(10);
    // let tasks = vec![x];

    // 1. Initialize the system
    let mut reactor = Reactor { current_time: 0 };
    let mut executor = Executor { tasks };

    // 2. Start the loop
    executor.run(&mut reactor);
}
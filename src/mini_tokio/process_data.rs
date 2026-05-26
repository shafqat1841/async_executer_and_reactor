
use std::pin::Pin;
use std::sync::{ Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

// Asynchronous equivalent to `thread::sleep`. Awaiting on this function pauses
// for the given duration.
//
// mini-tokio implements delays by spawning a timer thread that sleeps for the
// requested duration and notifies the caller once the delay completes. A thread
// is spawned **per** call to `delay`. This is obviously a terrible
// implementation strategy and nobody should use this in production. Tokio does
// not use this strategy. However, it can be implemented with few lines of code,
// so here we are.
pub async fn delay(dur: Duration) {
    
    // `delay` is a leaf future. Sometimes, this is referred to as a "resource".
    // Other resources include sockets and channels. Resources may not be
    // implemented in terms of `async/await` as they must integrate with some
    // operating system detail. Because of this, we must manually implement the
    // `Future`.
    //
    // However, it is nice to expose the API as an `async fn`. A useful idiom is
    // to manually define a private future and then use it from a public `async
    // fn` API.
    struct Delay {
        // When to complete the delay.
        when: Instant,
        // The waker to notify once the delay has completed. The waker must be
        // accessible by both the timer thread and the future so it is wrapped
        // with `Arc<Mutex<_>>`
        waker: Option<Arc<Mutex<Waker>>>,
    }

    impl Future for Delay {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            // First, if this is the first time the future is called, spawn the
            // timer thread. If the timer thread is already running, ensure the
            // stored `Waker` matches the current task's waker.
            if let Some(waker) = &self.waker {
                let mut waker = waker.lock().unwrap();

                // Check if the stored waker matches the current tasks waker.
                // This is necessary as the `Delay` future instance may move to
                // a different task between calls to `poll`. If this happens, the
                // waker contained by the given `Context` will differ and we
                // must update our stored waker to reflect this change.
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
            } else {
                let when = self.when;
                let waker = Arc::new(Mutex::new(cx.waker().clone()));
                self.waker = Some(waker.clone());

                // This is the first time `poll` is called, spawn the timer thread.
                thread::spawn(move || {
                    let now = Instant::now();

                    if now < when {
                        thread::sleep(when - now);
                    }

                    // The duration has elapsed. Notify the caller by invoking
                    // the waker.
                    let waker = waker.lock().unwrap();
                    waker.wake_by_ref();
                });
            }

            // Once the waker is stored and the timer thread is started, it is
            // time to check if the delay has completed. This is done by
            // checking the current instant. If the duration has elapsed, then
            // the future has completed and `Poll::Ready` is returned.
            if Instant::now() >= self.when {
                Poll::Ready(())
            } else {
                // The duration has not elapsed, the future has not completed so
                // return `Poll::Pending`.
                //
                // The `Future` trait contract requires that when `Pending` is
                // returned, the future ensures that the given waker is signaled
                // once the future should be polled again. In our case, by
                // returning `Pending` here, we are promising that we will
                // invoke the given waker included in the `Context` argument
                // once the requested duration has elapsed. We ensure this by
                // spawning the timer thread above.
                //
                // If we forget to invoke the waker, the task will hang
                // indefinitely.
                Poll::Pending
            }
        }
    }

    // Create an instance of our `Delay` future.
    let future = Delay {
        when: Instant::now() + dur,
        waker: None,
    };

    // Wait for the duration to complete.
    future.await;
}
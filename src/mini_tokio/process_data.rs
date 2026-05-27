use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

pub async fn delay(dur: Duration) {
    struct Delay {
        when: Instant,

        has_waker: bool,
    }

    impl Future for Delay {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if Instant::now() >= self.when {
                Poll::Ready(())
            } else {
                if !self.has_waker {
                    let when = self.when;

                    let waker = Arc::new(Mutex::new(cx.waker().clone()));

                    self.has_waker = true;

                    thread::spawn(move || {
                        let now = Instant::now();

                        if now < when {
                            thread::sleep(when - now);
                        }

                        let waker = waker.lock().unwrap();

                        waker.wake_by_ref();
                    });
                }
                Poll::Pending
            }
        }
    }

    let future = Delay {
        when: Instant::now() + dur,

        has_waker: false,
    };

    future.await;
}

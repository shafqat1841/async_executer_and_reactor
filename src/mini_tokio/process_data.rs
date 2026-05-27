use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

pub async fn delay(dur: Duration) {
    struct Delay {
        when: Instant,
        waker: Option<Arc<Mutex<Waker>>>,
    }

    impl Future for Delay {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if let Some(waker) = &self.waker {
                let mut waker = waker.lock().unwrap();
                if !waker.will_wake(cx.waker()) {
                    *waker = cx.waker().clone();
                }
            } else {
                let when = self.when;
                let waker = Arc::new(Mutex::new(cx.waker().clone()));
                self.waker = Some(waker.clone());
                thread::spawn(move || {
                    let now = Instant::now();

                    if now < when {
                        thread::sleep(when - now);
                    }
                    let waker = waker.lock().unwrap();
                    waker.wake_by_ref();
                });
            }
            if Instant::now() >= self.when {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
    let future = Delay {
        when: Instant::now() + dur,
        waker: None,
    };
    future.await;
}

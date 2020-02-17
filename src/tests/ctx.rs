// Copyright 2020 nytopop (Eric Izoita)
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.
use futures::task::AtomicWaker;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc,
    },
    task::{self, Poll},
};

/// A future that can be resolved externally. Completes [complete][Context::complete] is
/// called on this or any cloned context.
#[derive(Clone)]
pub struct Context {
    cond: Arc<AtomicBool>,
    wake: Arc<AtomicWaker>,
}

impl Default for Context {
    fn default() -> Self {
        let cond = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(AtomicWaker::new());
        Self { cond, wake }
    }
}

impl Future for Context {
    type Output = ();

    fn poll(self: Pin<&mut Self>, ctx: &mut task::Context<'_>) -> Poll<Self::Output> {
        self.wake.register(ctx.waker());

        if self.cond.load(Relaxed) {
            return Poll::Ready(());
        }

        Poll::Pending
    }
}

impl Context {
    /// Returns a RAII guard that will [complete][Context::complete] this context.
    pub fn guard(&self) -> Guard {
        Guard(self.clone())
    }

    /// Complete this context.
    pub fn complete(&self) {
        self.cond.store(true, Relaxed);
        self.wake.wake();
    }
}

pub struct Guard(Context);

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.complete();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn is_pending_if_not_completed() {
        let ctx = Context::default();
        let fut = timeout(Duration::from_millis(10), ctx);

        fut.await.unwrap_err();
    }

    #[tokio::test]
    async fn is_ready_if_completed() {
        let ctx = Context::default();
        let fut = timeout(Duration::from_millis(10), ctx.clone());
        ctx.complete();

        fut.await.unwrap();
    }

    #[tokio::test]
    async fn is_pending_if_guard_is_live() {
        let ctx = Context::default();
        let _guard = ctx.guard();
        let fut = timeout(Duration::from_millis(10), ctx);

        fut.await.unwrap_err();
    }

    #[tokio::test]
    async fn is_ready_if_guard_is_dropped() {
        let ctx = Context::default();
        let guard = ctx.guard();
        let fut = timeout(Duration::from_millis(10), ctx);
        drop(guard);

        fut.await.unwrap();
    }
}

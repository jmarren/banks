use std::pin::pin;
use std::time::Duration;

use async_stream::stream;
use futures_core::stream::Stream;
use futures_util::stream::StreamExt;
use tokio::time::sleep;

fn countdown(from: u32) -> impl Stream<Item = u32> {
    stream! {
        for i in (0..=from).rev() {
            yield i;
            sleep(Duration::from_millis(200)).await;
        }
    }
}

#[tokio::main]
async fn main() {
    // `countdown(3)` returns a generator-backed Stream, which is !Unpin
    // (same reason as in scratch.rs) — calling .next() requires Self to
    // be pinned first.
    //
    // std::pin::pin! is the stable, stack-only equivalent of
    // futures_util::pin_mut!: it takes the value by... well, it takes
    // an expression, evaluates it, and binds the result to a new local
    // that is `Pin<&mut T>` rather than `T`. No heap allocation (unlike
    // Box::pin), and no external crate (unlike pin_mut!).
    let mut s = pin!(countdown(3));

    while let Some(n) = s.next().await {
        println!("{n}");
    }
    println!("liftoff");

    // pin! also works on ordinary, already-Unpin values — there's
    // nothing stream-specific about it. Pinning a plain u32 is legal
    // (if pointless) because u32: Unpin, so nothing is actually being
    // restricted; this just demonstrates the macro isn't limited to
    // generator streams.
    let mut n = pin!(41);
    *n += 1;
    println!("n = {}", *n);
}

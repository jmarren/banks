use std::time::Duration;

use async_stream::stream;

use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use tokio::time::sleep;

fn zero_to_three() -> impl Stream<Item = u32> {
    stream! {
        for i in 0..3 {
            yield i;
            let _ = sleep(Duration::from_secs(1)).await;
        }
    }
}

fn handle_one<S: Stream<Item = u32>>(input: S) -> impl Stream<Item = u32> {
    stream! {
        for await value in input {
            if value == 1 {
                yield 20;
            } else {
                yield value;
            }
        }
    }
}

fn double<S: Stream<Item = u32>>(input: S) -> impl Stream<Item = u32> {
    stream! {
        for await value in input {
            yield value * 2;
        }
    }
}

#[tokio::main]
async fn main() {
    let s = double(handle_one(zero_to_three()));
    pin_mut!(s); // needed for iteration

    while let Some(value) = s.next().await {
        println!("got {}", value);
    }
}

use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr;

/// Self-referential: `self_ptr` points at `value`, which lives inside
/// this very struct. If the struct moves in memory, `self_ptr` becomes
/// a dangling pointer into the old location — this is exactly the
/// problem `Pin` exists to prevent.
struct SelfReferential {
    value: String,
    self_ptr: *const String,
    // opts this struct out of `Unpin`, so the compiler forces callers
    // to go through `Pin` instead of letting them move it freely
    _pin: PhantomPinned,
}

impl SelfReferential {
    /// Returns a pinned, heap-allocated instance with `self_ptr`
    /// pointing at its own `value` field.
    fn new(text: &str) -> Pin<Box<Self>> {
        let unpinned = Box::new(SelfReferential {
            value: text.to_string(),
            self_ptr: ptr::null(),
            _pin: PhantomPinned,
        });

        let mut boxed = Box::into_pin(unpinned);

        // Safety: we only write to `self_ptr`, never move `value` or
        // hand out a way to move `*boxed` — upholding Pin's contract.
        let self_ptr: *const String = &boxed.value;
        unsafe {
            let mut_ref: Pin<&mut Self> = Pin::as_mut(&mut boxed);
            Pin::get_unchecked_mut(mut_ref).self_ptr = self_ptr;
        }

        boxed
    }

    fn value(self: Pin<&Self>) -> &str {
        &self.get_ref().value
    }

    /// Dereferences `self_ptr` — only sound because `self` was never
    /// moved after `self_ptr` was set.
    fn value_via_self_ptr(self: Pin<&Self>) -> &str {
        unsafe { &*self.self_ptr }
    }
}

fn main() {
    let a = SelfReferential::new("hello");
    let b = SelfReferential::new("world");

    println!("a.value()            = {}", a.as_ref().value());
    println!(
        "a.value_via_self_ptr()= {}",
        a.as_ref().value_via_self_ptr()
    );
    println!("b.value()            = {}", b.as_ref().value());
    println!(
        "b.value_via_self_ptr()= {}",
        b.as_ref().value_via_self_ptr()
    );

    // Because `a` is `Pin<Box<SelfReferential>>`, the compiler won't let
    // us do `let moved = *a;` or otherwise relocate the pointee — try
    // uncommenting the next line and it will fail to compile:
    //
    // let moved: SelfReferential = *a;
    //
    // That's the whole point: Pin<P> guarantees the pointee's address
    // is stable for as long as it stays pinned, so `self_ptr` can never
    // dangle. This is the same guarantee async generators (like the
    // `stream!` macro in scratch.rs) rely on internally, which is why
    // consuming a stream from that macro requires `pin_mut!` first.
}

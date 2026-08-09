use std::marker::PhantomPinned;
use std::pin::Pin;

/// Owns `value`, and `first_word`/`second_word` are `&str` slices
/// borrowed *from* `value` — not copies. This only works if `value`
/// never moves after the slices are taken, which is why the struct
/// must be pinned.
struct SplitView {
    value: String,
    // 'static is a lie we tell the compiler — these actually borrow
    // from `value` above, for as long as `Self` stays pinned. See the
    // accessor methods below for the sound way to get these slices out.
    first_word: &'static str,
    second_word: &'static str,
    _pin: PhantomPinned,
}

impl SplitView {
    /// Splits `text` on the first space and returns a pinned instance
    /// whose `first_word`/`second_word` borrow directly from `value`.
    fn new(text: &str) -> Pin<Box<Self>> {
        let boxed = Box::new(SplitView {
            value: text.to_string(),
            first_word: "",
            second_word: "",
            _pin: PhantomPinned,
        });

        let mut pinned = Box::into_pin(boxed);

        // Safety: `split_once` borrows from `pinned.value`, which — once
        // pinned — will not move or be dropped for as long as `pinned`
        // (and therefore these borrows) are alive. We only ever hand the
        // resulting &str views back out through Pin<&Self> accessors
        // whose lifetime is tied to that same borrow, so callers can
        // never observe a dangling reference.
        unsafe {
            let this = Pin::get_unchecked_mut(Pin::as_mut(&mut pinned));
            let (a, b) = this.value.split_once(' ').unwrap_or((&this.value, ""));
            // extend the borrow to 'static as a private implementation
            // detail — never exposed with that lifetime to callers
            this.first_word = std::mem::transmute::<&str, &'static str>(a);
            this.second_word = std::mem::transmute::<&str, &'static str>(b);
        }

        pinned
    }

    fn value(self: Pin<&Self>) -> &str {
        &self.get_ref().value
    }

    /// Lifetime tied to `self`, not `'static` — this is what makes the
    /// internal transmute sound: nothing with a longer lifetime ever
    /// escapes.
    fn first_word(self: Pin<&Self>) -> &str {
        self.get_ref().first_word
    }

    fn second_word(self: Pin<&Self>) -> &str {
        self.get_ref().second_word
    }
}

fn main() {
    let greeting = SplitView::new("hello world");

    println!("value       = {:?}", greeting.as_ref().value());
    println!("first_word  = {:?}", greeting.as_ref().first_word());
    println!("second_word = {:?}", greeting.as_ref().second_word());

    // Moving `*greeting` out is exactly what Pin<Box<Self>> forbids at
    // compile time (uncomment to see it fail):
    //
    // let moved: SplitView = *greeting;
    //
    // Without Pin, that move would relocate `value`'s bytes... except
    // String's *contents* live on the heap already (see note below) —
    // so for THIS specific struct the real hazard is subtler than it
    // looks. Read on.
}

// A note on why this example is trickier than it first appears:
//
// `String`'s bytes live on the heap, so moving a `String` around on the
// stack does NOT move or invalidate slices borrowed from its contents —
// `&str` points at the heap allocation, not at the `String` struct
// itself. That means `SplitView` is *not* actually unsound to move in
// practice, unlike `SelfReferential` in pin_scratch.rs (which points at
// a field's *stack* address).
//
// It is still `unsafe`, though, for a different reason: ordinary safe
// Rust cannot express "this field borrows from that field" within one
// struct at all — there's no lifetime you can write down for it, which
// is exactly why the transmute-to-'static workaround exists. Real code
// facing this problem should reach for a maintained crate like `ouroboros`
// or `self_cell`, which generate the equivalent unsafe code correctly
// (and *do* require Pin, or an equivalent heap-owned indirection, because
// they support types where the borrowed-from field is genuinely
// address-sensitive — not just String/Vec).

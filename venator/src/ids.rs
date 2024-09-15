//! ID generation
//!
//! This module is for generating unique IDs. It keeps thread-local blocks of
//! IDs to hand out and only coordinate with global state if that block is
//! exhausted.
//!
//! This is needed since the tracing-subscriber `Registry` says that it will
//! re-use IDs from closed spans.

use std::cell::Cell;
use std::num::NonZeroU64;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

static ID_COUNTER: AtomicU64 = AtomicU64::new(1);

const LOCAL_ID_COUNTER_BLOCK_SIZE: u64 = 1024 * 1024;

fn get_local_block() -> Range<u64> {
    let block = ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let start = block * LOCAL_ID_COUNTER_BLOCK_SIZE;
    let end = start + LOCAL_ID_COUNTER_BLOCK_SIZE;

    Range { start, end }
}

thread_local! {
    static LOCAL_ID_COUNTER: Cell<Range<u64>> = Cell::new(get_local_block());
}

#[derive(Copy, Clone)]
pub(crate) struct VenatorId(pub(crate) NonZeroU64);

pub(crate) fn generate() -> VenatorId {
    let mut local_counter = LOCAL_ID_COUNTER.take();

    let id = match local_counter.next() {
        Some(id) => id,
        None => {
            local_counter = get_local_block();
            local_counter.next().unwrap()
        }
    };

    LOCAL_ID_COUNTER.set(local_counter);

    VenatorId(NonZeroU64::new(id).unwrap())
}

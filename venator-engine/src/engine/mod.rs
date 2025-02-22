//! The actual engine that handles inserts and queries.
//!
//! There are two variants: sync and async. `SyncEngine` is the core and the
//! `AsyncEngine` variant wraps an `SyncEngine` in a thread and coordinates via
//! message passing.

mod async_engine;
mod sync_engine;

pub use async_engine::AsyncEngine;
pub use sync_engine::SyncEngine;

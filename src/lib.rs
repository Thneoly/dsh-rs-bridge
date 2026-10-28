//! DSH runtime bridge primitives.
//!
//! This crate does not replace DSH's TypeScript AgentLoop. It provides a
//! process boundary and a runtime contract for integration against the pinned
//! dsh-v0.2.1-rc.3 baseline.

pub mod contracts;
pub mod mock;
pub mod store;
pub mod supervisor;

pub use contracts::*;
pub use mock::MockRuntime;
pub use store::{ExecutionEventSink, MemoryEventSink, MemorySessionStore, SessionStore};
pub use supervisor::RuntimeSupervisor;

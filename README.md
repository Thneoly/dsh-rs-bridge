# dsh-rs-bridge

Rust runtime bridge prototype for DSH, grounded in the pinned DSH source baseline dsh-v0.2.1-rc.3 (1f81c047fc48fa190a683e469daa699857e9afe8).

## Goal

Keep DSH's TypeScript Native AgentLoop intact while making external Agent Runtimes first-class execution backends. The bridge owns the runtime contract, adapter registry, lifecycle supervision, external session binding, and event notifications. A thin TypeScript adapter will eventually map the bridge protocol to the exact DSH Agent and Session contracts.

This is not a rewrite of DSH and does not confuse a model API provider with a full Agent Runtime.

## Current status

Milestone 0: Rust contract, supervisor, deterministic Mock Runtime, versioned stdio JSONL server, and CI.

In progress: real-process JSONL integration tests and multi-turn continuation semantics.

Not implemented yet: ACP adapter, durable storage, and the TypeScript DSH Core integration.

## Requirements

- Rust stable toolchain
- Cargo

## Build and test

    cargo fmt --all -- --check
    cargo test --all-targets
    cargo clippy --all-targets -- -D warnings

## Run

Probe the protocol and build version:

    cargo run -- --probe

Start the stdio JSONL server:

    cargo run -- --stdio

The server emits JSONL only on stdout. Keep logs and diagnostics on stderr.

See docs/architecture.md, docs/protocol.md, and docs/dsh-baseline.md.

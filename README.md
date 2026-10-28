# dsh-rs-bridge

Rust runtime bridge prototype for DSH, grounded in the pinned DSH source baseline dsh-v0.2.1-rc.3 (1f81c047fc48fa190a683e469daa699857e9afe8).

## Goal

Keep DSH core's AgentRegistry and Session implementation intact while making external Agent Runtimes first-class execution backends. The TypeScript plugin supplies an alternate DSH AgentFactory instead of loading the native AgentLoop plugin; the Rust process owns runtime adapter registration, external execution lifecycle, and external session binding.

This is not a rewrite of DSH and does not confuse a model API provider with a full Agent Runtime.

## Current status

Implemented: Rust runtime contract and supervisor, versioned stdio JSONL server, real-process integration test, multi-turn continuation semantics, and a TypeScript DSH AgentFactory/Session adapter with CI type-checking.

Not implemented yet: ACP/Claude CLI/CodeAgent adapters, durable external-session binding storage, live assistant-stream frames, external tool-event projection, and reliable DSH persisted-session resume.

## Requirements

- Rust stable toolchain
- Cargo
- Node.js 22+ and npm for the TypeScript DSH integration

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

The DSH plugin lives in `typescript/`; see [its integration guide](typescript/README.md). The plugin must be loaded instead of `@deepseek-ai/dsh-agent-loop`, because DSH exposes one AgentFactory slot.

See docs/architecture.md, docs/protocol.md, and docs/dsh-baseline.md.

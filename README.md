# dsh-rs-bridge

Rust runtime bridge prototype for DSH, grounded in the pinned DSH source baseline dsh-v0.2.1-rc.3 (1f81c047fc48fa190a683e469daa699857e9afe8).

## Goal

Keep DSH core's AgentRegistry and Session implementation intact while making external Agent Runtimes first-class execution backends. The TypeScript plugin supplies an alternate DSH AgentFactory instead of loading the native AgentLoop plugin; the Rust process owns runtime adapter registration, external execution lifecycle, and external session binding.

This is not a rewrite of DSH and does not confuse a model API provider with a full Agent Runtime.

## Current status

Implemented: Rust runtime contract and supervisor, versioned stdio JSONL server, real-process integration test, multi-turn continuation semantics, a TypeScript DSH AgentFactory/Session adapter, and an opt-in ACP v1 subprocess adapter with event translation and cancellation.

Not implemented yet: durable external-session binding storage, restart-safe ACP resume, durable tool-event normalization, and vendor-specific CLI adapters.

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


## ACP runtime (opt-in)

The bridge registers the ACP v1 adapter when `DSH_RS_BRIDGE_ACP_COMMAND` is set.
The command is launched as a subprocess and speaks ACP JSON-RPC over stdin/stdout.
Configure arguments as a JSON array so spaces are preserved:

```powershell
$env:DSH_RS_BRIDGE_ACP_COMMAND = "your-acp-agent"
$env:DSH_RS_BRIDGE_ACP_ARGS = '["--your-option","value with spaces"]'
$env:DSH_RS_BRIDGE_ACP_CWD = "C:\\work\\project"
cargo run -- --stdio
```

Optional `DSH_RS_BRIDGE_ACP_ENV` is a JSON object of environment overrides.
When enabled, the adapter appears as runtime ID `acp`; use that ID in
`session.start`. ACP session IDs are kept separate from DSH session IDs.
Continuation uses the same in-process ACP session. Resume after bridge restart
is intentionally not advertised yet because external session bindings are not
durable. The bridge terminates ACP subprocesses during orderly shutdown.


## Real ACP smoke test

CI includes a real-process handshake against the OpenCode 2.2.0 ACP server.
The test launches the official Linux binary and verifies ACP v1 initialization,
protocol negotiation, and `session/new`. It intentionally does not submit a
prompt, so it requires no model provider credentials and incurs no inference
cost. The deterministic fixture-agent tests still cover streamed output,
continuation, cancellation, and shutdown.

You can run the same probe locally with any installed ACP agent:

```sh
DSH_RS_BRIDGE_ACP_SMOKE_COMMAND=opencode \
DSH_RS_BRIDGE_ACP_SMOKE_ARGS='["acp"]' \
node scripts/acp-smoke.mjs
```

Override the command and JSON-array arguments for another ACP agent. The probe
does not print environment variables or authentication material.

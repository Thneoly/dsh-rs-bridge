# DSH TypeScript AgentFactory adapter

This package is the first DSH Core integration layer for the Rust bridge.

## Baseline

- DSH: `dsh-v0.2.1-rc.3`
- Bridge protocol: `dsh.runtime.v1`
- Plugin name: `dsh-rs-bridge`

## Configuration

Load this plugin **instead of** `@deepseek-ai/dsh-agent-loop`. Both register the single `ctx.agents` factory slot and cannot be active together.

Example configuration:

```yaml
- name: dsh-rs-bridge
  config:
    command: ./target/release/dsh-rs-bridge
    args: ["--stdio"]
    runtimeId: mock
    requestTimeoutMs: 30000
    shutdownTimeoutMs: 3000
```

For a local development build, use `command: cargo` and `args: ["run", "--release", "--", "--stdio"]`, with `cwd` set to the Rust repository root.

Per-agent engine selection is available through the merge-extensible DSH `AgentOptions` field `runtimeId`. The selected ID must be registered by the Rust bridge's `runtime.list` / `runtime.probe`.

## Current integration boundary

- DSH creates and owns the Agent and Session; the Rust process owns the external runtime session and execution lifecycle.
- Each external execution maps to one DSH turn and one step. User input, assistant output, step/turn boundaries, inbox splices, and terminal status are written to the DSH Session.
- Assistant deltas are accumulated and committed as one final DSH assistant message. Live assistant-stream frames and external tool-call/result projection are not implemented yet.
- `AgentFactory.resume()` is intentionally rejected for now: the Rust bridge stores DSH-to-external session bindings in memory. Durable binding storage must be implemented before DSH persisted-session resume can be reliable.
- The Rust bridge currently registers only the deterministic `mock` runtime. ACP, Claude CLI, and CodeAgent adapters are future work.
- This plugin replaces DSH's Native AgentLoop factory for its configured agents; do not load both factories in one DSH context.

## Type checking

From this directory:

```sh
npm install
npm run typecheck
```

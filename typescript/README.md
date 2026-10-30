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
    permissionPolicy: deny
    turnTimeoutMs: 3600000
```

For a local development build, use `command: cargo` and `args: ["run", "--release", "--", "--stdio"]`, with `cwd` set to the Rust repository root.

`turnTimeoutMs` is the per-turn budget for `session.wait`/`session.cancel` —
kept separate from the short `requestTimeoutMs` so real-model turns that
stream for minutes are not failed spuriously. `permissionPolicy` decides how
forwarded runtime permission requests are answered automatically (`deny` is
the safe default); DSH exposes no interactive host channel to the Agent yet,
so both the question and the automatic answer are also surfaced as transient
`agent/runtime-event` notifications for audit and a future permission UI.

## Self-healing after a bridge crash

If the bridge process dies unexpectedly, the plugin does not brick: the
client respawns it (`autoRespawn`, default true, bounded by `maxRestarts`,
default 5) and emits `bridge-restarted`. Each live agent then reattaches its
persisted binding via `session.restore` before its next continuation, so the
conversation carries on with the same external session. A turn that was in
flight when the crash happened settles as failed and repairs through the
standard interrupted-turn path. The durable journals make this sound:
`host_seq` continues across the restart and the OS-level state-directory lock
releases when the process dies, so the successor takes over cleanly.

Per-agent engine selection is available through the merge-extensible DSH `AgentOptions` field `runtimeId`. The selected ID must be registered by the Rust bridge's `runtime.list` / `runtime.probe`.

## Current integration boundary

- DSH creates and owns the Agent and Session; the Rust process owns the external runtime session and execution lifecycle.
- Each external execution maps to one DSH turn and one step. User input, assistant output, step/turn boundaries, inbox splices, and terminal status are written to the DSH Session.
- The bridge's own durable event journal (`events.jsonl`, replayable through the bridge RPC `session.events`) is available for host-side catch-up; this plugin does not consume it yet — DSH history remains the source of truth for resume.
- Assistant deltas are published as transient `agent/assistant-stream` start/chunk/end frames and compacted into the final durable `assistant/message` or `assistant/attempt` event.
- External `tool.*` events are surfaced as transient `agent/runtime-event` notifications. They are deliberately not rewritten as DSH `tool/call` / `tool/result` events because an external agent's internal tool cycle is not necessarily a DSH model tool-call cycle.
- `AgentFactory.resume()` opens the existing DSH JSONL session for write, repairs an interrupted final turn, restores the Rust bridge binding, and reattaches the existing external conversation. The Rust binding journal and DSH session-persistence root must both survive restart.
- The Rust bridge always registers the deterministic `mock` runtime and can optionally register an ACP v1 subprocess runtime through `DSH_RS_BRIDGE_ACP_COMMAND`. Vendor-specific Claude CLI and CodeAgent adapters remain future work.
- This plugin replaces DSH's Native AgentLoop factory for its configured agents; do not load both factories in one DSH context.

## Type checking

From this directory:

```sh
npm install
npm run typecheck
```


## Live event projection

The adapter publishes DSH-native assistant frames through `agent/assistant-stream`.
Each turn emits a `start` frame, zero or more `chunk` frames, and an `end`
frame after its durable settlement commits. The compact timed chunk records are
embedded in the final `assistant/message` or `assistant/attempt` event.

External tool lifecycle events are published transiently through
`agent/runtime-event` with `{ turn, step, event }`. The nested event carries
the runtime ID, external session ID, event type, optional runtime sequence,
payload, and timestamp. This avoids inventing DSH model tool-call/result
semantics for tools executed internally by another agent runtime.


## Resuming a persisted DSH agent

Mount `@deepseek-ai/dsh-session-persistence-jsonl` alongside the Session
service and keep its root stable across process restarts. Configure the bridge
with a stable `DSH_RS_BRIDGE_STATE_DIR`. Call
`ctx.agents.resume({ resumeSessionId })` after creating a fresh Context and
bridge process. The factory loads and repairs the DSH event log, invokes
`session.restore` to reconnect the external conversation, and publishes the
resumed Agent with the same DSH and external session identities.

An interrupted in-flight model turn is not replayed automatically. Its DSH
history is closed with the standard interrupted-turn repair events; the next
user prompt starts a new turn on the restored conversation.

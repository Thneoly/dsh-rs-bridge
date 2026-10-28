# Architecture

## Baseline

- DSH tag: dsh-v0.2.1-rc.3
- DSH commit: 1f81c047fc48fa190a683e469daa699857e9afe8
- This repository is an independent bridge prototype. It does not patch DSH core.

## Ownership

- DSH TypeScript remains the host and retains its AgentRegistry and Session implementation.
- The TypeScript plugin provides an alternate AgentFactory; do not load it together with the native AgentLoop plugin because DSH has one factory slot.
- The Rust process owns runtime adapter registration, external execution lifecycle, event ordering validation, and external-session binding.
- The TypeScript adapter maps external turn output and terminal status onto DSH Session events and publishes live assistant-stream frames. Durable tool-call/result projection from external runtimes remains deliberately deferred (see "DSH live event projection").
- Runtime IDs and external session IDs are separate from DSH Session IDs.

## Verification story

The Rust server exposes a versioned JSONL protocol over stdio and registers a deterministic in-process Mock Runtime alongside the optional ACP adapter. Process-level tests exercise discovery, start, wait, continuation, deterministic cancellation, asynchronous notifications, journal repair, and shutdown against the real binary. The TypeScript plugin implements DSH AgentFactory creation, inbox projections, Session turn/step events, assistant output mapping, and disposal, verified by fixture-driven integration tests.

Beyond fixtures, `scripts/bridge-acp-e2e.mjs` is an opt-in, credentialed
full-conversation acceptance gate against a real ACP agent. It drives one
continuous conversation through streaming, same-session recall, mid-stream
cancellation, durable-journal assertions, and a bridge restart with
`session.restore` plus post-restore recall. It is run manually (it spends real
model tokens); CI covers the agent handshake and fixture-based execution only.

## Protocol

Each request is a single JSON line:

    {"protocol":"dsh.runtime.v1","id":"1","method":"runtime.list","params":{}}

Responses use the same protocol and request ID. Asynchronous notifications are emitted as:

    {"protocol":"dsh.runtime.v1","method":"runtime.event","params":{"host_seq":1,"event":{...}}}

Supported methods:

- runtime.list
- runtime.probe
- session.start
- session.resume
- session.cancel
- session.wait
- session.send (live input or post-terminal continuation, depending on runtime capabilities)
- bridge.shutdown

The protocol is a project-local initial contract, not an existing DSH or ACP standard.


## ACP v1 subprocess adapter

The optional `AcpRuntime` launches one ACP agent process per external ACP
session. It negotiates protocol version 1, creates a session, sends prompts,
and translates `session/update` notifications into the bridge's runtime event
contract. The ACP process and ACP session ID are distinct from the DSH session
identity. Subsequent prompts reuse the same ACP process/session until bridge
shutdown, preserving in-process conversation continuity.

The adapter's session control sends ACP cancellation notifications. The
supervisor drains active turns first and then invokes each adapter's shutdown
hook to release adapter-owned subprocesses — a session that refuses to
converge is reported as an error but does not skip the adapter cleanup. The
in-place resume RPC stays disabled by design; restart recovery goes through
`session.restore`, which reattaches the persisted binding instead of claiming
to resume an interrupted in-flight turn.

Exception recovery: agent-process death mid-turn settles the turn as failed
and marks the process dead, so `session.restore` transparently replaces the
zombie with a fresh agent bound to the same external conversation. Prompt
timeouts kill the process rather than orphaning it, because ACP
`session/update` carries no turn identity and an abandoned agent's late
updates would otherwise contaminate the next turn's event stream. Stdin
writes are deadline-bounded so a wedged agent cannot hold the turn lock, and
`session.cancel` gives up after a bounded wait instead of blocking until the
prompt budget expires.


## DSH live event projection

The TypeScript adapter publishes assistant output using the standard
`agent/assistant-stream` lifecycle: `start`, timed `chunk` frames, then an
`end` frame after the durable `assistant/message` or `assistant/attempt`
settlement commits. The compact stream is stored in the settlement event; token
frames themselves are process-local and are not appended as individual session
events.

External tool activity is published as transient `agent/runtime-event`
notifications. The adapter does not fabricate DSH `tool/call` and
`tool/result` records from another runtime's internal tool cycle, because DSH's
durable tool-call semantics are tied to assistant tool-call blocks and matching
call/result identities.


## Durable bindings and conversation restoration

The executable defaults to a file-backed session binding journal under
`.dsh-rs-bridge/sessions.jsonl`; `DSH_RS_BRIDGE_STATE_DIR` overrides the root.
The journal is append-only JSONL with a versioned record envelope, sync-on-write
durability, latest-record replay, and torn-tail repair. The persisted record
contains the DSH session ID, runtime ID, opaque external session ID, status,
timestamps, and optional working directory.

`session.restore` is distinct from `session.resume`: the former reconnects an
idle conversation after the bridge process restarts, while the latter remains
the active-execution recovery contract. For ACP v1, restoration prefers
`session/resume` when advertised and falls back to `session/load` when
`loadSession` is advertised. A previous `starting` or `running` record is
closed as failed because the bridge cannot safely reconstruct the old in-flight
turn. DSH history is repaired and restored separately by the TypeScript
AgentFactory before the agent is published.

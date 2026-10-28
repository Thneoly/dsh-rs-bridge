# Architecture

## Baseline

- DSH tag: dsh-v0.2.1-rc.3
- DSH commit: 1f81c047fc48fa190a683e469daa699857e9afe8
- This repository is an independent bridge prototype. It does not patch DSH core.

## Ownership

- DSH TypeScript remains the host and retains its AgentRegistry and Session implementation.
- The TypeScript plugin provides an alternate AgentFactory; do not load it together with the native AgentLoop plugin because DSH has one factory slot.
- The Rust process owns runtime adapter registration, external execution lifecycle, event ordering validation, and external-session binding.
- The TypeScript adapter maps external turn output and terminal status onto DSH Session events. Live assistant-stream frames and tool-call/result projection remain future work.
- Runtime IDs and external session IDs are separate from DSH Session IDs.

## First milestone

The Rust server exposes a versioned JSONL protocol over stdio and registers a deterministic in-process Mock Runtime. A process-level test exercises discovery, start, wait, continuation, cancellation, asynchronous notifications, and shutdown. The TypeScript plugin implements DSH AgentFactory creation, inbox projections, Session turn/step events, assistant output mapping, and disposal.

The Rust bridge currently uses in-memory session storage. It does not promise process-restart recovery, so the TypeScript factory rejects DSH persisted-session resume until durable external-session bindings are implemented.

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
hook to release adapter-owned subprocesses. Resume is intentionally disabled
until durable external-session binding and restart recovery are implemented.


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

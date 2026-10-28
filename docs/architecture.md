# Architecture

## Baseline

- DSH tag: dsh-v0.2.1-rc.3
- DSH commit: 1f81c047fc48fa190a683e469daa699857e9afe8
- This repository is an independent bridge prototype. It does not patch DSH core.

## Ownership

- DSH TypeScript remains the host and retains its AgentFactory, Native AgentLoop, and Session implementation.
- This Rust process owns runtime adapter registration, external execution lifecycle, event ordering validation, and external-session binding.
- A thin TypeScript bridge must map these events onto the exact DSH SessionEventMap before production use.
- Runtime IDs and external session IDs are separate from DSH Session IDs.

## First milestone

The initial server exposes a versioned JSONL protocol over stdio and registers a deterministic in-process Mock Runtime. The Mock Runtime proves the process boundary, lifecycle supervision, cancellation, resume identity, and event notifications before an ACP adapter is added.

The first server uses in-memory session storage. It does not promise process-restart recovery. Durable storage is a later milestone.

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
- session.send
- bridge.shutdown

The protocol is a project-local initial contract, not an existing DSH or ACP standard.

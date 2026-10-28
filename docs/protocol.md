# JSONL Protocol v1

Transport: one JSON object per line over stdin/stdout. Do not write logs to stdout; diagnostics belong on stderr.

Request envelope:

    {"protocol":"dsh.runtime.v1","id":"req-1","method":"runtime.list","params":{}}

Successful response:

    {"protocol":"dsh.runtime.v1","id":"req-1","result":[]}

Error response:

    {"protocol":"dsh.runtime.v1","id":"req-1","error":{"code":"capability","message":"..."}}

Asynchronous event notification:

    {"protocol":"dsh.runtime.v1","method":"runtime.event","params":{"host_seq":1,"event":{"dsh_session_id":"session-1"}}}

The host_seq value is monotonic per DSH session and is assigned by the bridge. The runtime_seq value is supplied by the selected adapter and is validated independently.

## Methods

- runtime.list: list registered runtime adapters, capabilities, and health.
- runtime.probe: inspect one runtime adapter.
- session.start: create an execution for a DSH session.
- session.resume: reattach to the external session referenced by a DSH session.
- session.cancel: interrupt an active execution and wait for its terminal state.
- session.wait: wait until the session reaches a terminal state.
- session.send: send input to an active runtime if it advertises live_input; after a terminal turn, start a continuation on the same external session if continuation is supported.
- bridge.shutdown: stop reading requests and exit.

## Current limitations

- Only the deterministic mock runtime is registered by the initial server.
- Session bindings are in memory and are lost when the process exits.
- Continuation reuses the same external session ID but starts a new runtime attachment; active in-flight input is a separate capability (live_input).
- The protocol is request/notification JSONL, not a complete JSON-RPC 2.0 implementation.
- ACP and vendor-specific CLI adapters are not implemented yet.


## ACP adapter

The bridge can optionally register an ACP v1 subprocess adapter. Configure it
with `DSH_RS_BRIDGE_ACP_COMMAND`; `DSH_RS_BRIDGE_ACP_ARGS` is a JSON array,
`DSH_RS_BRIDGE_ACP_CWD` selects a default working directory, and
`DSH_RS_BRIDGE_ACP_ENV` is a JSON object of environment overrides.

The adapter performs `initialize`, `session/new`, and `session/prompt`.
ACP `session/update` notifications are translated into runtime events:
assistant chunks/messages, tool-call updates, and status updates. Cancellation
is forwarded using `session/cancel`. A completed turn can continue on the same
ACP session while the bridge process remains alive.

The adapter currently reports `resume: false`: session IDs are retained only
in memory, and the bridge cannot safely restore the external process binding
after a restart. It does not advertise live input during an active prompt.


## DSH TypeScript event projection

Assistant text events are forwarded as process-local `agent/assistant-stream`
frames and compacted into the final durable `assistant/message` /
`assistant/attempt` stream. External tool lifecycle events are emitted through
the transient `agent/runtime-event` event with the original runtime envelope.
They are not currently converted into durable DSH `tool/call` and
`tool/result` records.


## Session restoration RPC

`session.restore` accepts `{ "dsh_session_id": "..." }`. It looks up the
persisted runtime binding, reattaches the external conversation when the
adapter supports session restoration, and returns the original runtime and
external session IDs. It does not start a prompt. If the prior record was
`starting` or `running`, the bridge marks that old execution failed and emits
`runtime.interrupted`; successful reattachment emits
`runtime.session_restored`. The next `session.send` can start a new turn on
the restored conversation.

The default binding journal is `.dsh-rs-bridge/sessions.jsonl`; configure
`DSH_RS_BRIDGE_STATE_DIR` to keep it in a stable deployment directory. The
journal is separate from DSH's own durable session event log. Both stores are
required to reconstruct the full agent session.

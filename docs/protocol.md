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
- session.send: send follow-up input if the runtime advertises continuation.
- bridge.shutdown: stop reading requests and exit.

## Current limitations

- Only the deterministic mock runtime is registered by the initial server.
- Session bindings are in memory and are lost when the process exits.
- The protocol is request/notification JSONL, not a complete JSON-RPC 2.0 implementation.
- ACP and vendor-specific CLI adapters are not implemented yet.

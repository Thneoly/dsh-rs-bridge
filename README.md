# dsh-rs-bridge

Rust runtime bridge prototype for DSH, grounded in the pinned DSH source baseline dsh-v0.2.1-rc.3 (1f81c047fc48fa190a683e469daa699857e9afe8).

## Goal

Keep DSH core's AgentRegistry and Session implementation intact while making external Agent Runtimes first-class execution backends. The TypeScript plugin supplies an alternate DSH AgentFactory instead of loading the native AgentLoop plugin; the Rust process owns runtime adapter registration, external execution lifecycle, and external session binding.

This is not a rewrite of DSH and does not confuse a model API provider with a full Agent Runtime.

## Current status

Implemented: Rust runtime contract and supervisor, versioned stdio JSONL server, real-process integration test, multi-turn continuation semantics, a TypeScript DSH AgentFactory/Session adapter, an opt-in ACP v1 subprocess adapter with event translation, cancellation, permission forwarding, and failure containment, durable session bindings and a durable execution-event journal with cross-restart replay, and TypeScript-side self-healing (bridge respawn plus automatic `session.restore` on the next turn after a crash).

Not implemented yet: active in-flight turn recovery (an interrupted turn is closed as failed, then restored), durable normalized DSH tool-call semantics, and vendor-specific CLI adapters.

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

## Operating the bridge

### State directory

The bridge keeps all durable state under one directory (default
`.dsh-rs-bridge/`, override with `DSH_RS_BRIDGE_STATE_DIR`; keep it stable
across restarts):

| File | Contents | Durability |
|---|---|---|
| `sessions.jsonl` | session bindings: DSH session ID → runtime ID, external session ID, status, cwd | append-only, versioned, synced per write, torn-tail repair, last-record replay |
| `events.jsonl` | the full execution event stream (assistant deltas, tool lifecycle, permission questions/answers, turn outcomes, lifecycle markers) | append-only, versioned, synced per write, torn-tail repair; `host_seq` per DSH session continues across restarts |

The event journal is runtime observability data: it **does contain model
output** (that is what makes replay possible), but no credentials. Retain both
files (plus DSH's own session log) for full recovery.

One bridge owns a state directory at a time: startup takes an OS-level
exclusive lock on `bridge.lock` and a second live bridge on the same directory
refuses to start (journals assume a single writer — dual writers would
duplicate `host_seq` silently). The OS releases the lock when the process
dies, orderly or not, so a crashed bridge never blocks its successor.

### RPC quick reference

One JSON object per line over stdin/stdout, envelope
`{"protocol":"dsh.runtime.v1","id":"req-1","method":"...","params":{...}}`.
Responses correlate by `id`; the bridge also emits unsolicited
`runtime.event` notifications carrying `{host_seq, event}`.

| Method | Params | Result |
|---|---|---|
| `runtime.list` | `{}` | runtime descriptors with capabilities and health |
| `runtime.probe` | `{runtime_id}` | one descriptor |
| `session.start` | `{dsh_session_id, runtime_id, prompt, cwd?}` | execution summary (`running`) |
| `session.send` | `{dsh_session_id, text}` | `{accepted: true}` — live input (needs `live_input`) or post-terminal continuation |
| `session.wait` | `{dsh_session_id}` | summary at terminal state |
| `session.cancel` | `{dsh_session_id, reason?}` | summary at terminal state (bounded: 30 s without an agent that honors `session/cancel`) |
| `session.restore` | `{dsh_session_id}` | reattach the persisted binding after a bridge restart |
| `session.events` | `{dsh_session_id, after_host_seq?}` | durable events of the session, optionally only after a sequence — survives restarts |
| `permission.respond` | `{dsh_session_id, request_id, option_id}` | answer a forwarded `permission.requested` event |
| `bridge.shutdown` | `{}` | `{stopping: true}`, then exit |

Event types (full list in docs/protocol.md): `assistant.delta`,
`assistant.message`, `tool.started`, `tool.completed`, `permission.requested`,
`permission.responded`, `runtime.status`, the terminal
`turn.completed`/`turn.cancelled`/`turn.failed`, and supervisor lifecycle
markers (`runtime.started`, `runtime.continued`, `runtime.resumed`,
`runtime.session_restored`, `runtime.interrupted`, `runtime.restore_failed`,
`runtime.failed`, `runtime.ended`, `runtime.cleanup_failed`).

### Environment variables

| Variable | Purpose |
|---|---|
| `DSH_RS_BRIDGE_STATE_DIR` | state directory for both journals |
| `DSH_RS_BRIDGE_ACP_COMMAND` | register the ACP runtime with this agent command |
| `DSH_RS_BRIDGE_ACP_ARGS` | JSON array of agent arguments (e.g. `["acp"]`) |
| `DSH_RS_BRIDGE_ACP_CWD` | default working directory for agent processes |
| `DSH_RS_BRIDGE_ACP_ENV` | JSON object of extra agent environment |
| `DSH_RS_BRIDGE_MOCK_DELAY_MS` | mock runtime turn delay (tests) |
| `DSH_RS_BRIDGE_MOCK_TOOL_EVENTS` | enable mock tool lifecycle events (tests) |


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
Continuation uses the same in-process ACP session while the bridge lives;
after a bridge restart, `session.restore` reattaches the persisted binding to
a fresh agent process. The bridge terminates ACP subprocesses during orderly
shutdown.


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


## Durable state: bindings and events

The bridge stores runtime session records in an append-only, versioned JSONL
journal at `.dsh-rs-bridge/sessions.jsonl` by default (see "Operating the
bridge" for the layout). Every create/update is synced before acknowledgement;
startup replays the latest record per DSH session and truncates an incomplete
final journal line. The binding journal stores runtime IDs, external session
IDs, status, and the host working directory — not credentials or conversation
content.

Every execution event is **also** journaled to `.dsh-rs-bridge/events.jsonl`
before it is forwarded on stdout, and the journal allocates the per-session
`host_seq`: sequence numbers therefore **continue across bridge restarts**
instead of restarting at 1. The `session.events` RPC replays a session's full
durable history (optionally only events after a given `host_seq`), so a host
that reconnects after a restart gap can recover exactly the events it missed —
streaming output, tool lifecycle, permission questions and answers, and turn
outcomes alike.

The `session.restore` RPC reconnects a persisted DSH session to its external
agent conversation without claiming that an interrupted in-flight model turn
can be resumed. ACP restoration uses `session/resume` when advertised, or
`session/load` when that is the supported capability. If the bridge died
during a turn, the prior runtime record is marked failed before restoration;
the DSH AgentFactory repairs an incomplete DSH turn before publishing the
resumed agent. A subsequent prompt continues the same external ACP session.

The TypeScript AgentFactory supports `resume({ resumeSessionId })` using
the DSH JSONL persistence backend. The Rust journals and DSH session log
must all be retained for full cross-process restoration.

## Real-model full-conversation gate (opt-in)

The ordinary CI smoke launches a real OpenCode ACP binary and verifies
`initialize` + `session/new` **without requiring credentials**. This proves
the agent handshake, not model generation.

`scripts/bridge-acp-e2e.mjs` is the full-conversation acceptance gate: one
DSH session, one continuous real-model conversation, driven through

- **A** a first turn: streamed `assistant.delta` strictly before
  `turn.completed`, terminal status `completed`, with a random codeword
  planted in the prompt;
- **B** a same-session recall turn that must echo the codeword (verifies real
  conversational memory, not just session identity);
- **T** a tool turn: the agent must read a marker file in the session cwd; if
  it asks permission (force it with opencode's `permission.bash: ask`), the
  gate answers allow via `permission.respond` and asserts the permission and
  tool round-trip end to end;
- **C** a mid-stream cancellation: cancel after the first streamed delta and
  require `turn.cancelled`;
- **D** a durable-journal assertion: `sessions.jsonl` replay must match the
  observed terminal states;
- **E** a bridge **restart** followed by `session.restore` and another
  codeword recall on the same external session (verifies the conversation
  survives the restart, not just the binding);
- **F** a durable event replay: `session.events` must return the full
  cross-restart history — streaming, tool, permission, cancellation, and
  restore events — with globally continuous `host_seq`.

The gate is strict about protocol framing: any non-JSON stdout line or
envelope violation from the bridge fails the run. On failure it keeps a
`gate-report.json` (full event log including model output) and per-bridge
stderr logs in a temp directory and prints the path. Console output stays
clean of prompts, model output, credentials, and external IDs.

### Running the gate

The gate is agent-agnostic: it drives whatever `DSH_RS_BRIDGE_ACP_COMMAND`
points at. With OpenCode (2.2.0, the same version CI pins) against an
Anthropic-compatible endpoint:

```bash
cargo build --bin dsh-rs-bridge
export DSH_RS_BRIDGE_BIN="$PWD/target/debug/dsh-rs-bridge"
export DSH_RS_BRIDGE_ACP_COMMAND="/path/to/opencode"
export DSH_RS_BRIDGE_ACP_ARGS='["acp"]'
export OPENCODE_CONFIG="/path/to/opencode-gate.json"
node scripts/bridge-acp-e2e.mjs
```

`opencode-gate.json` selects a real model and, when the provider sits behind
an Anthropic-compatible gateway, points the provider at it. Note the SDK
appends `/messages?beta=true` itself, so `baseURL` must include the `/v1`
suffix:

```json
{
  "model": "anthropic/claude-sonnet-5",
  "provider": {
    "anthropic": {
      "options": {
        "baseURL": "{env:ANTHROPIC_BASE_URL}/v1",
        "apiKey": "{env:ANTHROPIC_API_KEY}"
      }
    }
  }
}
```

Tuning knobs: `DSH_RS_BRIDGE_GATE_RPC_TIMEOUT_MS` (short RPC budget, default
20 s), `DSH_RS_BRIDGE_GATE_TURN_TIMEOUT_MS` (per-turn budget, default 300 s),
`DSH_RS_BRIDGE_GATE_CANCEL_PROBE_TIMEOUT_MS` (how long to wait for the first
delta before cancelling, default 30 s),
`DSH_RS_BRIDGE_GATE_PERMISSION_PROBE_TIMEOUT_MS` (how long to wait for a
permission request on the tool turn, default 45 s),
`DSH_RS_BRIDGE_GATE_CANCEL_ATTEMPTS` (default 3, for models that finish
before a delta can be cancelled), `DSH_RS_BRIDGE_GATE_KEEP=1` and
`DSH_RS_BRIDGE_GATE_REPORT=<path>` to keep or copy the report on success.

The gate spends real model tokens; it is **not run by ordinary CI**. The
bridge forwards ACP permission requests to the host; the gate answers allow
for the tool turn. Through the DSH plugin the answer comes from the
configured `permissionPolicy` (deny by default) — see the TypeScript README.
Unanswered requests are auto-denied after a bounded deadline.

## Troubleshooting

- **`provider response ended unexpectedly` from the agent (opencode), retried
  ~10 times**: the provider `baseURL` is missing its `/v1` suffix. The
  Anthropic SDK appends `/messages?beta=true` itself; a bare gateway root
  answers 200 with a non-SSE body. Use
  `"baseURL": "{env:ANTHROPIC_BASE_URL}/v1"`.
- **`provider authentication required` / `invalid x-api-key`**: the agent is
  not picking up your gateway credentials. Configure them explicitly through
  the agent's config file (`apiKey: "{env:ANTHROPIC_API_KEY}"`) — do not rely
  on ambient `ANTHROPIC_BASE_URL` being honored; opencode ignores it without
  the provider override.
- **The agent's tool turn is silently degraded / no tool events appear**: a
  permission request was denied. Runtimes that ask permission wait for a
  `permission.respond`; through the DSH plugin the `permissionPolicy`
  (default `deny`) answers automatically — watch for `permission.requested`
  / `permission.responded` events in `session.events` output.
- **A turn never settles**: `session.cancel` gives up after 30 s and reports
  a transport error; the agent may be stuck. Agent death, prompt timeouts,
  and wedged stdin all converge to `turn.failed` + a `session.restore` path.
- **Bridge refuses to start (`persistence error ... invalid ... journal
  entry`)**: a journal line is corrupt. The repair handles a torn *final*
  line automatically; a malformed complete line fails closed by design —
  restore the state directory from backup or remove the damaged journal when
  its history is disposable.
- **Where to look after a failure**: the gate keeps `gate-report.json` (full
  event log including model output) and per-bridge stderr logs in a temp
  directory and prints the path; the bridge's own durable trail is
  `events.jsonl`, replayable per session via `session.events`.

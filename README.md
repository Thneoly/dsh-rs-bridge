# dsh-rs-bridge

Rust runtime bridge prototype for DSH, grounded in the pinned DSH source baseline dsh-v0.2.1-rc.3 (1f81c047fc48fa190a683e469daa699857e9afe8).

## Goal

Keep DSH core's AgentRegistry and Session implementation intact while making external Agent Runtimes first-class execution backends. The TypeScript plugin supplies an alternate DSH AgentFactory instead of loading the native AgentLoop plugin; the Rust process owns runtime adapter registration, external execution lifecycle, and external session binding.

This is not a rewrite of DSH and does not confuse a model API provider with a full Agent Runtime.

## Current status

Implemented: Rust runtime contract and supervisor, versioned stdio JSONL server, real-process integration test, multi-turn continuation semantics, a TypeScript DSH AgentFactory/Session adapter, and an opt-in ACP v1 subprocess adapter with event translation and cancellation.

Not implemented yet: durable execution-event history, active in-flight turn recovery, durable normalized tool-event semantics, and vendor-specific CLI adapters.

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


## Durable runtime session bindings

The bridge stores runtime session records in an append-only, versioned JSONL
journal at `.dsh-rs-bridge/sessions.jsonl` by default. Set
`DSH_RS_BRIDGE_STATE_DIR` to an absolute deployment-owned directory to choose a
stable location. Every create/update is synced before acknowledgement; startup
replays the latest record per DSH session and truncates an incomplete final
journal line. The journal stores runtime IDs, external session IDs, status, and
the host working directory — not credentials or conversation content.

The `session.restore` RPC reconnects a persisted DSH session to its external
agent conversation without claiming that an interrupted in-flight model turn
can be resumed. ACP restoration uses `session/resume` when advertised, or
`session/load` when that is the supported capability. If the bridge died
during a turn, the prior runtime record is marked failed before restoration;
the DSH AgentFactory repairs an incomplete DSH turn before publishing the
resumed agent. A subsequent prompt continues the same external ACP session.

The TypeScript AgentFactory now supports `resume({ resumeSessionId })` using
the DSH JSONL persistence backend. The Rust binding journal and DSH session log
must both be retained for full cross-process restoration.

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
- **C** a mid-stream cancellation: cancel after the first streamed delta and
  require `turn.cancelled`;
- **D** a durable-journal assertion: `sessions.jsonl` replay must match the
  observed terminal states;
- **E** a bridge **restart** followed by `session.restore` and another
  codeword recall on the same external session (verifies the conversation
  survives the restart, not just the binding).

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
delta before cancelling, default 30 s), `DSH_RS_BRIDGE_GATE_CANCEL_ATTEMPTS`
(default 3, for models that finish before a delta can be cancelled),
`DSH_RS_BRIDGE_GATE_KEEP=1` and `DSH_RS_BRIDGE_GATE_REPORT=<path>` to keep or
copy the report on success.

The gate spends real model tokens; it is **not run by ordinary CI**. The
bridge currently declines ACP permission requests, so turns whose tools
require permission are denied — the gate prompts avoid mutating tool use.

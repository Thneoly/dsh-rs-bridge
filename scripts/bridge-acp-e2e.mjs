#!/usr/bin/env node
// Opt-in, credentialed FULL-CONVERSATION acceptance gate against a real ACP agent
// (e.g. opencode) through the Rust bridge. Not run by ordinary CI: it needs a
// configured, authenticated agent and spends real model tokens.
//
// Phases (one DSH session, one continuous external conversation):
//   A  first turn: streamed assistant.delta strictly before turn.completed, terminal completed
//   B  same-session recall turn: the model must echo a codeword planted in turn 1
//   T  tool turn: the agent must read a marker file; if it asks permission,
//      the gate answers allow and asserts the permission + tool round-trip
//   C  mid-stream cancellation: cancel after the first streamed delta -> turn.cancelled
//   D  durable journal: sessions.jsonl replay matches the observed terminal states
//   E  bridge restart + session.restore + another codeword recall on the SAME external session
//
// Determinism notes:
// - The codeword is a random per-run token, so the recall assertions verify real
//   conversational memory without needing a judge model.
// - host_seq is allocated per bridge process; after the phase-E restart it starts
//   at 1 again. Ordering is therefore asserted per bridge process, and the restart
//   is recorded in the report rather than treated as a violation.
//
// Console output avoids prompt text, model output, credentials, and external IDs.
// The on-disk report DOES contain full event payloads (including model output):
// it is a local diagnostic artifact for the operator who ran the gate.
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { createInterface } from 'node:readline'

const PROTOCOL = 'dsh.runtime.v1'

function numenv(name, fallback) {
  const raw = process.env[name]
  if (raw === undefined || raw === '') return fallback
  const value = Number(raw)
  if (!Number.isFinite(value) || value < 100) throw new Error(`Invalid ${name}: ${raw}`)
  return value
}

const binary = process.env.DSH_RS_BRIDGE_BIN ?? resolve('target/debug/dsh-rs-bridge')
const command = process.env.DSH_RS_BRIDGE_ACP_COMMAND
if (!command) {
  throw new Error('Set DSH_RS_BRIDGE_ACP_COMMAND to a real ACP agent binary (e.g. opencode)')
}
const rpcTimeoutMs = numenv('DSH_RS_BRIDGE_GATE_RPC_TIMEOUT_MS', 20000)
const turnTimeoutMs = numenv('DSH_RS_BRIDGE_GATE_TURN_TIMEOUT_MS', 300000)
const cancelProbeTimeoutMs = numenv('DSH_RS_BRIDGE_GATE_CANCEL_PROBE_TIMEOUT_MS', 30000)
const permissionProbeTimeoutMs = numenv('DSH_RS_BRIDGE_GATE_PERMISSION_PROBE_TIMEOUT_MS', 45000)
const cancelAttempts = Number(process.env.DSH_RS_BRIDGE_GATE_CANCEL_ATTEMPTS ?? 3)
if (!Number.isInteger(cancelAttempts) || cancelAttempts < 1) throw new Error('Invalid DSH_RS_BRIDGE_GATE_CANCEL_ATTEMPTS')
const keepArtifacts = process.env.DSH_RS_BRIDGE_GATE_KEEP === '1'
const reportPath = process.env.DSH_RS_BRIDGE_GATE_REPORT
const codeWord = ('XLY-' + randomUUID().slice(0, 8)).toUpperCase()

const temp = mkdtempSync(join(tmpdir(), 'dsh-real-acp-gate-'))
const stateDir = join(temp, 'state')
const sessionId = 'real-acp-gate-' + process.pid
// The marker file is only obtainable by actually running a tool in the
// session cwd; its content reuses the codeword token.
writeFileSync(join(temp, 'gate-tool.txt'), `tool-marker-${codeWord}\n`)

const report = {
  startedAt: new Date().toISOString(),
  binary,
  command,
  rpcTimeoutMs,
  turnTimeoutMs,
  sessionId,
  codeWordLength: codeWord.length,
  phases: [],
  hostSeqRestartObserved: false,
}
const recordedEvents = [] // {bridge, host_seq, event}
const bridges = []
const turnMarkers = new Set(['runtime.started', 'runtime.continued', 'runtime.resumed'])

function phase(name, detail, startedAt) {
  report.phases.push({ name, ok: true, detail, durationMs: Date.now() - startedAt })
  console.log(`[conversation-gate] PASS ${name}: ${detail}`)
}

function failPhase(name, error) {
  report.phases.push({ name, ok: false, detail: String(error?.message ?? error) })
  console.error(`[conversation-gate] FAIL ${name}: ${String(error?.message ?? error)}`)
}

function killTree(child) {
  if (child.exitCode !== null) return
  if (process.platform === 'win32') {
    spawn('taskkill', ['/pid', String(child.pid), '/T', '/F'], { windowsHide: true })
  } else {
    child.kill('SIGTERM')
  }
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

function turnTerminal(event) {
  return event.event_type === 'turn.completed' || event.event_type === 'turn.cancelled' || event.event_type === 'turn.failed'
}

// One bridge process: JSONL client with strict framing validation.
class Bridge {
  constructor(label) {
    this.label = label
    this.child = spawn(binary, ['--stdio'], {
      cwd: process.cwd(),
      env: { ...process.env, DSH_RS_BRIDGE_STATE_DIR: stateDir },
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
    })
    this.closed = false
    this.nextId = 0
    this.pending = new Map()
    this.events = []
    this.stderrChunks = []
    bridges.push(this)
    this.child.stderr.setEncoding('utf8')
    this.child.stderr.on('data', chunk => this.stderrChunks.push(chunk))
    this.child.once('error', error => this.rejectPending(`Bridge ${label} launch failed: ${error.message}`))
    this.child.once('close', () => {
      this.closed = true
      this.rejectPending(`Bridge ${label} exited unexpectedly`)
    })
    this.lines = createInterface({ input: this.child.stdout })
    this.lines.on('line', line => this.onLine(line))
  }

  rejectPending(message) {
    for (const item of this.pending.values()) {
      clearTimeout(item.timer)
      item.reject(new Error(message))
    }
    this.pending.clear()
  }

  onLine(line) {
    let message
    try {
      message = JSON.parse(line)
    } catch (error) {
      this.closed = true
      this.rejectPending(`Bridge ${this.label} emitted a non-JSON stdout line (${line.slice(0, 120)}): ${error}`)
      return
    }
    if (message.protocol !== PROTOCOL) {
      this.closed = true
      this.rejectPending(`Bridge ${this.label} violated the protocol envelope: ${line.slice(0, 120)}`)
      return
    }
    if (message.method === 'runtime.event') {
      const persisted = message.params ?? {}
      const event = persisted.event
      if (!event || typeof persisted.host_seq !== 'number') {
        // Protocol corruption must fail the gate even when no RPC is in flight.
        this.closed = true
        this.rejectPending(`Bridge ${this.label} emitted a malformed runtime.event: ${line.slice(0, 120)}`)
        return
      }
      this.events.push({ host_seq: persisted.host_seq, event })
      recordedEvents.push({ bridge: this.label, host_seq: persisted.host_seq, event })
      return
    }
    if (message.id === undefined) return
    const key = String(message.id)
    const item = this.pending.get(key)
    if (!item) return
    this.pending.delete(key)
    clearTimeout(item.timer)
    if (message.error) item.reject(new Error(`${item.method}: ${message.error.code}: ${message.error.message}`))
    else item.resolve(message.result)
  }

  request(method, params = {}, timeoutMs = rpcTimeoutMs) {
    if (this.closed || this.child.exitCode !== null) throw new Error(`Bridge ${this.label} unavailable`)
    const id = String(++this.nextId)
    return new Promise((res, rej) => {
      const timer = setTimeout(() => {
        this.pending.delete(id)
        rej(new Error(`${method} timed out after ${timeoutMs}ms on bridge ${this.label}`))
      }, timeoutMs)
      this.pending.set(id, { resolve: res, reject: rej, timer, method })
      this.child.stdin.write(JSON.stringify({ protocol: PROTOCOL, id, method, params }) + '\n')
    })
  }

  sessionEvents(dshSessionId) {
    return this.events
      .filter(entry => entry.event?.dsh_session_id === dshSessionId)
      .map(entry => entry.event)
  }

  assertOrdering() {
    const lastSeq = new Map()
    for (const entry of this.events) {
      const sid = entry.event?.dsh_session_id
      if (!sid) continue
      const previous = lastSeq.get(sid)
      if (previous !== undefined && entry.host_seq <= previous) {
        throw new Error(`host_seq regressed for session ${sid} on bridge ${this.label}: ${previous} -> ${entry.host_seq}`)
      }
      lastSeq.set(sid, entry.host_seq)
    }
  }

  async shutdown() {
    if (this.closed) return
    await this.request('bridge.shutdown', {}, 10000)
    await new Promise(resolve => {
      const timer = setTimeout(() => {
        killTree(this.child)
        resolve()
      }, 5000)
      this.child.once('close', () => { clearTimeout(timer); resolve() })
    })
  }

  stderrText() {
    return this.stderrChunks.join('')
  }
}

// Split a session's event stream into per-turn windows. A window starts at a
// turn marker; anything before the first marker is not part of a turn window.
function turnWindows(events) {
  const windows = []
  let current = null
  for (const event of events) {
    if (turnMarkers.has(event.event_type)) {
      current = []
      windows.push(current)
    }
    if (current) current.push(event)
  }
  return windows
}

function windowText(window) {
  return window
    .filter(event => event.event_type === 'assistant.delta')
    .map(event => String(event.payload?.text ?? ''))
    .join('')
}

// Poll the live event stream until an event at or after fromIndex matches.
async function waitForSessionEvent(bridge, dshSessionId, fromIndex, predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const events = bridge.sessionEvents(dshSessionId)
    const match = events.slice(fromIndex).find(predicate)
    if (match) return match
    if (Date.now() >= deadline) {
      throw new Error(`Timed out after ${timeoutMs}ms waiting for ${label} on session ${dshSessionId}`)
    }
    await sleep(100)
  }
}

function replaySessionJournal() {
  const path = join(stateDir, 'sessions.jsonl')
  const raw = readFileSync(path, 'utf8')
  const lastRecord = new Map()
  let lines = 0
  for (const line of raw.split('\n')) {
    if (line.trim() === '') continue
    lines += 1
    const entry = JSON.parse(line) // a corrupt journal line must fail the gate, not slip through
    assert.equal(entry.version, 1, 'journal entry has an unsupported version')
    lastRecord.set(entry.record.dsh_session_id, entry.record)
  }
  return { lines, records: lastRecord }
}

async function runCancelPhase(bridge, dshSessionId) {
  for (let attempt = 1; attempt <= cancelAttempts; attempt++) {
    await bridge.request('session.send', {
      dsh_session_id: dshSessionId,
      text: 'Count slowly from 1 to 50, one number per line. Do not use any tools.',
    })
    const fromIndex = bridge.sessionEvents(dshSessionId).length
    // The turn is only cancellable while live: trigger on the first streamed
    // delta, bailing out if the turn settles on its own first.
    const trigger = await waitForSessionEvent(
      bridge,
      dshSessionId,
      fromIndex,
      event => event.event_type === 'assistant.delta' || turnTerminal(event),
      cancelProbeTimeoutMs,
      'first streamed delta or terminal event of the cancel turn',
    )
    if (turnTerminal(trigger)) {
      console.log(`[conversation-gate] note: cancel attempt ${attempt} settled before streaming; retrying`)
      // Synchronize with the bridge's terminal bookkeeping before re-sending:
      // the record must be terminal for continue_session to accept it.
      await bridge.request('session.wait', { dsh_session_id: dshSessionId }, turnTimeoutMs)
      continue
    }
    const cancelled = await bridge.request('session.cancel', {
      dsh_session_id: dshSessionId,
      reason: 'conversation gate cancel phase',
    }, turnTimeoutMs)
    assert.equal(cancelled.status, 'cancelled', 'cancel did not converge to a cancelled terminal state')
    return attempt
  }
  throw new Error(`the model finished every cancel attempt before a delta could be cancelled (${cancelAttempts} attempts)`)
}

async function main() {
  let startedAt = Date.now()
  const bridge = new Bridge('primary')
  const runtimes = await bridge.request('runtime.list')
  const acp = runtimes.find(runtime => runtime.id === 'acp')
  assert.ok(acp, 'ACP runtime was not registered (is DSH_RS_BRIDGE_ACP_COMMAND set to an agent?)')
  assert.equal(acp.capabilities.continuation, true, 'ACP runtime must support continuation')
  assert.equal(acp.capabilities.session_restore, true, 'ACP runtime must support session restore')

  // Phase A: first turn with a planted codeword.
  const first = await bridge.request('session.start', {
    dsh_session_id: sessionId,
    runtime_id: 'acp',
    prompt: `Remember the codeword ${codeWord} for this conversation. Reply with exactly: acknowledged.`,
    cwd: temp,
  }, turnTimeoutMs)
  const externalSessionId = first.external_session_id
  assert.ok(externalSessionId, 'session.start did not return an external session id')
  assert.equal(first.status, 'running', 'session.start did not report a running turn')
  const firstTerminal = await bridge.request('session.wait', { dsh_session_id: sessionId }, turnTimeoutMs)
  assert.equal(firstTerminal.status, 'completed', 'first real-model turn did not complete')
  assert.equal(firstTerminal.external_session_id, externalSessionId, 'terminal summary changed external session identity')
  const firstWindow = turnWindows(bridge.sessionEvents(sessionId))[0] ?? []
  const deltaIndex = firstWindow.findIndex(event => event.event_type === 'assistant.delta')
  const completedIndex = firstWindow.findIndex(event => event.event_type === 'turn.completed')
  assert.ok(deltaIndex >= 0, 'no streamed assistant output in the first turn')
  assert.ok(completedIndex >= 0, 'no turn.completed terminal event in the first turn')
  assert.ok(deltaIndex < completedIndex, 'assistant output did not stream before the terminal event')
  phase('A first turn', 'streamed output preceded turn.completed; status completed', startedAt)

  // Phase B: same-session recall of the codeword.
  startedAt = Date.now()
  await bridge.request('session.send', {
    dsh_session_id: sessionId,
    text: 'Reply with only the codeword I gave you earlier in this conversation.',
  })
  const secondTerminal = await bridge.request('session.wait', { dsh_session_id: sessionId }, turnTimeoutMs)
  assert.equal(secondTerminal.status, 'completed', 'recall turn did not complete')
  assert.equal(secondTerminal.external_session_id, externalSessionId, 'recall turn changed external session identity')
  const windows = turnWindows(bridge.sessionEvents(sessionId))
  const recallWindow = windows[windows.length - 1] ?? []
  assert.ok(
    recallWindow.some(event => event.event_type === 'assistant.delta'),
    'recall turn produced no streamed output',
  )
  const recallText = windowText(recallWindow).toUpperCase()
  assert.ok(
    recallText.includes(codeWord),
    'the model did not recall the codeword planted in turn 1 (same-session memory broken)',
  )
  phase('B same-session recall', 'codeword recalled on the same external session', startedAt)

  // Phase T: tool use behind a permission round-trip. The model can only
  // produce the marker by reading gate-tool.txt in the session cwd.
  startedAt = Date.now()
  await bridge.request('session.send', {
    dsh_session_id: sessionId,
    text: 'Use a shell command to print the contents of the file named gate-tool.txt in the current working directory, then reply with the exact command output and nothing else.',
  })
  const toolFromIndex = bridge.sessionEvents(sessionId).length
  const toolTrigger = await waitForSessionEvent(
    bridge,
    sessionId,
    toolFromIndex,
    event => event.event_type === 'permission.requested' || turnTerminal(event),
    permissionProbeTimeoutMs,
    'permission.requested or terminal event of the tool turn',
  )
  let answeredPermission = false
  if (toolTrigger.event_type === 'permission.requested') {
    const options = toolTrigger.payload?.options ?? []
    const allow = options.find(option => String(option.kind ?? '').startsWith('allow')) ?? options[0]
    assert.ok(allow?.optionId, 'permission request offered no options')
    await bridge.request('permission.respond', {
      dsh_session_id: sessionId,
      request_id: toolTrigger.payload?.request_id,
      option_id: allow.optionId,
    })
    answeredPermission = true
  }
  const toolTerminal = await bridge.request('session.wait', { dsh_session_id: sessionId }, turnTimeoutMs)
  assert.equal(toolTerminal.status, 'completed', 'tool turn did not complete')
  assert.equal(toolTerminal.external_session_id, externalSessionId, 'tool turn changed external session identity')
  const toolWindows = turnWindows(bridge.sessionEvents(sessionId))
  const toolWindow = toolWindows[toolWindows.length - 1] ?? []
  const toolText = windowText(toolWindow).toUpperCase()
  assert.ok(
    toolText.includes(codeWord),
    'the model did not echo the marker file content (tool round-trip broken)',
  )
  if (answeredPermission) {
    assert.ok(
      toolWindow.some(event => event.event_type === 'permission.responded' && event.payload?.source === 'host'),
      'host permission decision must be reported as permission.responded',
    )
    assert.ok(
      toolWindow.some(event => event.event_type === 'tool.started' || event.event_type === 'tool.completed'),
      'an allowed permission must surface tool lifecycle events',
    )
    phase('T tool + permission', 'permission asked and answered allow; marker read via tool', startedAt)
  } else {
    console.log('[conversation-gate] note: the agent read the marker without asking for permission')
    phase('T tool', 'marker read via tool without a permission prompt', startedAt)
  }

  // Phase C: cancel a live turn after the first streamed delta.
  startedAt = Date.now()
  const attempt = await runCancelPhase(bridge, sessionId)
  const cancelWindows = turnWindows(bridge.sessionEvents(sessionId))
  const cancelWindow = cancelWindows[cancelWindows.length - 1] ?? []
  assert.ok(
    cancelWindow.some(event => event.event_type === 'assistant.delta'),
    'cancel turn streamed no output before cancellation',
  )
  assert.ok(
    cancelWindow.some(event => event.event_type === 'turn.cancelled'),
    'no turn.cancelled terminal event after session.cancel',
  )
  phase(`C mid-stream cancel (attempt ${attempt})`, 'first delta streamed, then turn.cancelled reached', startedAt)

  startedAt = Date.now()
  bridge.assertOrdering()
  phase('ordering', 'host_seq strictly increasing per session (primary bridge)', startedAt)

  // Phase D: durable journal matches observed reality.
  startedAt = Date.now()
  const journal = replaySessionJournal()
  assert.ok(journal.lines > 0, 'sessions.jsonl is empty')
  const record = journal.records.get(sessionId)
  assert.ok(record, 'sessions.jsonl has no record for the gate session')
  assert.equal(record.runtime_id, 'acp', 'journal runtime id mismatch')
  assert.equal(record.external_session_id, externalSessionId, 'journal external session id mismatch')
  assert.equal(record.status, 'cancelled', 'journal terminal status should be cancelled after phase C')
  phase('D durable journal', `sessions.jsonl replay (${journal.lines} lines) matches the live terminal state`, startedAt)

  // Phase E: bridge restart + restore + post-restore recall.
  startedAt = Date.now()
  await bridge.shutdown()
  const restarted = new Bridge('restored')
  const restored = await restarted.request('session.restore', { dsh_session_id: sessionId }, turnTimeoutMs)
  assert.equal(restored.external_session_id, externalSessionId, 'restore changed the external session identity')
  assert.ok(['cancelled', 'completed'].includes(restored.status), `unexpected restored status: ${restored.status}`)
  await restarted.request('session.send', {
    dsh_session_id: sessionId,
    text: 'Reply with only the codeword from the very beginning of this conversation.',
  })
  const finalTerminal = await restarted.request('session.wait', { dsh_session_id: sessionId }, turnTimeoutMs)
  assert.equal(finalTerminal.status, 'completed', 'post-restore turn did not complete')
  assert.equal(finalTerminal.external_session_id, externalSessionId, 'post-restore turn changed external session identity')
  const finalWindows = turnWindows(restarted.sessionEvents(sessionId))
  const finalWindow = finalWindows[finalWindows.length - 1] ?? []
  assert.ok(
    finalWindow.some(event => event.event_type === 'assistant.delta'),
    'post-restore turn produced no streamed output',
  )
  const finalText = windowText(finalWindow).toUpperCase()
  assert.ok(
    finalText.includes(codeWord),
    'the model did not recall the codeword after the bridge restart (session restore did not restore the conversation)',
  )
  const lastPrimarySeq = bridge.events
    .filter(entry => entry.event?.dsh_session_id === sessionId)
    .at(-1)?.host_seq ?? 0
  const firstRestoredSeq = restarted.events
    .filter(entry => entry.event?.dsh_session_id === sessionId)[0]?.host_seq
  if (typeof firstRestoredSeq === 'number' && firstRestoredSeq <= lastPrimarySeq) {
    report.hostSeqRestartObserved = true
  }
  restarted.assertOrdering()
  const finalJournal = replaySessionJournal()
  assert.equal(finalJournal.records.get(sessionId)?.status, 'completed', 'journal status should be completed after phase E')
  phase('E restart + restore', 'bridge restarted, session restored, codeword recalled across the restart', startedAt)
}

let exitCode = 0
try {
  await main()
} catch (error) {
  failPhase(report.phases.length ? `after ${report.phases[report.phases.length - 1].name}` : 'startup', error)
  console.error(`[conversation-gate] gate FAILED; artifacts kept at: ${temp}`)
  exitCode = 1
} finally {
  // Tear down every bridge we started, even on failure: live children keep the
  // Node event loop (and the agent subprocesses) alive after the gate verdict.
  for (const instance of bridges) {
    if (instance.closed || instance.child.exitCode !== null) continue
    try {
      await instance.shutdown()
    } catch {
      killTree(instance.child)
    }
  }
  await sleep(300)
  report.finishedAt = new Date().toISOString()
  report.eventCount = recordedEvents.length
  report.events = recordedEvents
  try {
    writeFileSync(join(temp, 'gate-report.json'), JSON.stringify(report, null, 2))
    for (const instance of bridges) {
      writeFileSync(join(temp, `${instance.label}-stderr.log`), instance.stderrText())
    }
    if (reportPath) {
      writeFileSync(reportPath, JSON.stringify(report, null, 2))
      console.log(`[conversation-gate] report copied to ${reportPath}`)
    }
  } catch (error) {
    console.error('[conversation-gate] failed to write report: ' + String(error?.message ?? error))
  }
  if (exitCode === 0 && !keepArtifacts && !reportPath) {
    rmSync(temp, { recursive: true, force: true })
    console.log('[conversation-gate] all phases passed')
  } else if (exitCode === 0) {
    console.log(`[conversation-gate] all phases passed; artifacts kept at: ${temp}`)
  }
}
process.exitCode = exitCode

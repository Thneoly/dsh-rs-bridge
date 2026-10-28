#!/usr/bin/env node
// Opt-in, credentialed end-to-end smoke against a real ACP agent via Rust bridge.
// Does not print prompt content, model output, credentials, or external IDs.
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { createInterface } from 'node:readline'

const binary = process.env.DSH_RS_BRIDGE_BIN ?? resolve('target/debug/dsh-rs-bridge')
const command = process.env.DSH_RS_BRIDGE_ACP_COMMAND
if (!command) throw new Error('Set DSH_RS_BRIDGE_ACP_COMMAND to a real ACP agent binary')
const timeoutMs = Number(process.env.DSH_RS_BRIDGE_E2E_TIMEOUT_MS ?? 120000)
if (!Number.isFinite(timeoutMs) || timeoutMs < 1000) throw new Error('Invalid E2E timeout')
const temp = mkdtempSync(join(tmpdir(), 'dsh-real-acp-'))
const child = spawn(binary, ['--stdio'], {
  cwd: process.cwd(),
  env: { ...process.env, DSH_RS_BRIDGE_STATE_DIR: join(temp, 'state') },
  stdio: ['pipe', 'pipe', 'inherit'],
  windowsHide: true,
})
let closed = false
let nextId = 0
const pending = new Map()
const events = []
const lines = createInterface({ input: child.stdout })
function rejectPending(message) {
  for (const item of pending.values()) {
    clearTimeout(item.timer)
    item.reject(new Error(message))
  }
  pending.clear()
}
child.once('error', error => rejectPending('Bridge launch failed: ' + error.message))
child.once('close', () => {
  closed = true
  rejectPending('Bridge exited unexpectedly')
})
lines.on('line', line => {
  let message
  try { message = JSON.parse(line) } catch { return }
  if (message.method === 'runtime.event') {
    events.push(message.params?.event)
    return
  }
  if (message.id === undefined) return
  const key = String(message.id)
  const item = pending.get(key)
  if (!item) return
  pending.delete(key)
  clearTimeout(item.timer)
  if (message.error) item.reject(new Error(item.method + ': ' + message.error.code))
  else item.resolve(message.result)
})
function request(method, params = {}) {
  if (closed || child.exitCode !== null) throw new Error('Bridge unavailable')
  const id = String(++nextId)
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id)
      reject(new Error(method + ' timed out after ' + timeoutMs + 'ms'))
    }, timeoutMs)
    pending.set(id, { resolve, reject, timer, method })
    child.stdin.write(JSON.stringify({ protocol: 'dsh.runtime.v1', id, method, params }) + '\n')
  })
}
const sessionId = 'real-acp-' + process.pid + '-' + Date.now()
const prompt = process.env.DSH_RS_BRIDGE_E2E_PROMPT ?? 'Reply with a short greeting, without using any tools.'
async function main() {
  const runtimes = await request('runtime.list')
  const acp = runtimes.find(runtime => runtime.id === 'acp')
  assert.ok(acp, 'ACP runtime was not registered')
  assert.equal(acp.capabilities.continuation, true)
  const first = await request('session.start', {
    dsh_session_id: sessionId, runtime_id: 'acp', prompt, cwd: resolve(temp),
  })
  assert.ok(first.external_session_id, 'Missing external session ID')
  const result = await request('session.wait', { dsh_session_id: sessionId })
  assert.equal(result.status, 'completed', 'First real agent prompt did not complete')
  const firstEvents = events.filter(event => event?.dsh_session_id === sessionId)
  assert.ok(firstEvents.some(event => event.event_type === 'assistant.delta'), 'No streamed assistant output')
  assert.ok(firstEvents.some(event => event.event_type === 'turn.completed'), 'No terminal event')
  console.log('[real-acp-e2e] PASS: real agent prompt, streaming events, terminal status')
  await request('session.send', { dsh_session_id: sessionId, text: 'Reply briefly: second turn.' })
  const second = await request('session.wait', { dsh_session_id: sessionId })
  assert.equal(second.status, 'completed', 'Continuation did not complete')
  assert.equal(second.external_session_id, first.external_session_id, 'Continuation changed external session identity')
  console.log('[real-acp-e2e] PASS: same-session multi-turn continuation')
}
try {
  await main()
} catch (error) {
  console.error('[real-acp-e2e] FAIL: ' + String(error?.message ?? error))
  process.exitCode = 1
} finally {
  if (!closed) {
    try { await Promise.race([request('bridge.shutdown'), new Promise((_, reject) => setTimeout(() => reject(new Error('shutdown timeout')), 3000))]) } catch {}
    if (!closed) child.kill('SIGTERM')
  }
  rmSync(temp, { recursive: true, force: true })
}

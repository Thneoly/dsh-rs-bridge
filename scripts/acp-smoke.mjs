#!/usr/bin/env node
import { spawn } from 'node:child_process'
import { createInterface } from 'node:readline'
import { setTimeout as delay } from 'node:timers/promises'

const protocolVersion = 1
const command = process.env.DSH_RS_BRIDGE_ACP_SMOKE_COMMAND ?? 'opencode'
const argsValue = process.env.DSH_RS_BRIDGE_ACP_SMOKE_ARGS ?? '["acp"]'
const cwd = process.env.DSH_RS_BRIDGE_ACP_SMOKE_CWD ?? process.cwd()
const timeoutMs = Number(process.env.DSH_RS_BRIDGE_ACP_SMOKE_TIMEOUT_MS ?? 30_000)

let args
try {
  args = JSON.parse(argsValue)
} catch (error) {
  throw new Error('DSH_RS_BRIDGE_ACP_SMOKE_ARGS must be a JSON string array', { cause: error })
}
if (!Array.isArray(args) || args.some((arg) => typeof arg !== 'string')) {
  throw new Error('DSH_RS_BRIDGE_ACP_SMOKE_ARGS must be a JSON string array')
}
if (!Number.isFinite(timeoutMs) || timeoutMs < 1000) {
  throw new Error('DSH_RS_BRIDGE_ACP_SMOKE_TIMEOUT_MS must be at least 1000')
}

const child = spawn(command, args, {
  cwd,
  env: process.env,
  stdio: ['pipe', 'pipe', 'inherit'],
  windowsHide: true,
})

let nextId = 0
let closed = false
const pending = new Map()
const closePromise = new Promise((resolve) => {
  child.once('close', (code, signal) => {
    closed = true
    for (const [id, entry] of pending) {
      clearTimeout(entry.timer)
      entry.reject(new Error('ACP process exited before response to request ' + id +
        ' (code=' + code + ', signal=' + signal + ')'))
    }
    pending.clear()
    resolve({ code, signal })
  })
})

child.once('error', (error) => {
  for (const entry of pending.values()) {
    clearTimeout(entry.timer)
    entry.reject(new Error('Unable to launch ACP agent "' + command + '": ' + error.message))
  }
  pending.clear()
})

const lineReader = createInterface({ input: child.stdout })
lineReader.on('line', (line) => {
  if (!line.trim()) return
  let message
  try {
    message = JSON.parse(line)
  } catch {
    console.error('[acp-smoke] ignored non-JSON stdout line: ' + line.slice(0, 160))
    return
  }
  if (message.id === undefined || message.id === null) return
  const key = String(message.id)
  const entry = pending.get(key)
  if (!entry) return
  pending.delete(key)
  clearTimeout(entry.timer)
  if (message.error !== undefined) {
    const error = new Error('ACP ' + entry.method + ' failed: ' + JSON.stringify(message.error))
    error.rpcError = message.error
    entry.reject(error)
  } else {
    entry.resolve(message.result ?? {})
  }
})

async function request(method, params) {
  if (closed || child.exitCode !== null) {
    throw new Error('ACP process is not running: ' + command)
  }
  const id = ++nextId
  const key = String(id)
  const result = new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(key)
      reject(new Error('Timed out after ' + timeoutMs + 'ms waiting for ACP ' + method))
    }, timeoutMs)
    pending.set(key, { method, resolve, reject, timer })
  })
  const message = JSON.stringify({ jsonrpc: '2.0', id, method, params })
  child.stdin.write(message + '\n')
  return result
}

async function main() {
  console.log('[acp-smoke] launching: ' + command + ' ' + args.join(' '))
  const initialized = await request('initialize', {
    protocolVersion,
    clientInfo: { name: 'dsh-rs-bridge-smoke', version: '0.1.0' },
    clientCapabilities: {},
  })
  if (initialized.protocolVersion !== protocolVersion) {
    throw new Error('ACP protocol mismatch: expected ' + protocolVersion +
      ', received ' + JSON.stringify(initialized.protocolVersion))
  }
  const agentInfo = initialized.agentInfo ?? {}
  console.log('[acp-smoke] initialize passed: ' +
    (agentInfo.name ?? 'unknown-agent') + ' ' + (agentInfo.version ?? ''))
  if (Array.isArray(initialized.authMethods) && initialized.authMethods.length > 0) {
    console.log('[acp-smoke] agent advertises ' + initialized.authMethods.length +
      ' authentication method(s); credentials are not inspected or logged')
  }

  const session = await request('session/new', {
    cwd,
    mcpServers: [],
  })
  if (typeof session.sessionId !== 'string' || session.sessionId.trim() === '') {
    throw new Error('ACP session/new returned no sessionId: ' + JSON.stringify(session))
  }
  console.log('[acp-smoke] session/new passed: received a non-empty external session id')
  console.log('[acp-smoke] PASS: real ACP process launch, initialize, protocol negotiation, and session creation')
}

try {
  await main()
} catch (error) {
  console.error('[acp-smoke] FAIL: ' + (error instanceof Error ? error.message : String(error)))
  process.exitCode = 1
} finally {
  if (!closed) {
    child.kill('SIGTERM')
    await Promise.race([closePromise, delay(2000)])
    if (!closed) child.kill('SIGKILL')
  }
}

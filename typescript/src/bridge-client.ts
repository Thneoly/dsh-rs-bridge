import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { createInterface, type Interface as ReadlineInterface } from 'node:readline'
import { EventEmitter } from 'node:events'
import type { Context } from '@deepseek-ai/cordis'

export const BRIDGE_PROTOCOL = 'dsh.runtime.v1'

export interface BridgeProcessConfig {
  command: string
  args: string[]
  cwd?: string
  env: Record<string, string>
  requestTimeoutMs: number
  shutdownTimeoutMs: number
}

export interface RuntimeEventEnvelope {
  dsh_session_id: string
  runtime_id: string
  external_session_id: string
  event_type: string
  runtime_seq?: number
  payload: Record<string, unknown>
  at_unix_ms: number
}

export interface RuntimeNotification {
  host_seq: number
  event: RuntimeEventEnvelope
}

export interface RuntimeExecutionSummary {
  dsh_session_id: string
  runtime_id: string
  external_session_id: string | null
  status: 'starting' | 'running' | 'completed' | 'cancelled' | 'failed'
}

export interface RuntimeDescriptor {
  id: string
  capabilities: {
    streaming_events: boolean
    resume: boolean
    session_restore: boolean
    interrupt: boolean
    live_input: boolean
    continuation: boolean
  }
  health: {
    status: 'ready' | 'degraded' | 'unavailable'
    detail?: string | null
  }
}

interface RpcMessage {
  protocol?: string
  id?: string | number | null
  method?: string
  params?: unknown
  result?: unknown
  error?: { code?: string; message?: string }
}

interface PendingRequest {
  resolve(value: unknown): void
  reject(error: Error): void
  timeout: ReturnType<typeof setTimeout>
}

/** Owns one Rust bridge process and multiplexes request responses with async event notifications. */
export class JsonlBridgeClient extends EventEmitter {
  private readonly child: ChildProcessWithoutNullStreams
  private readonly lines: ReadlineInterface
  private readonly pending = new Map<string, PendingRequest>()
  private readonly lastHostSeq = new Map<string, number>()
  private nextId = 0
  private stopping = false

  constructor(private readonly ctx: Context, private readonly config: BridgeProcessConfig) {
    super()
    this.child = spawn(config.command, config.args, {
      cwd: config.cwd,
      env: { ...process.env, ...config.env },
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    this.lines = createInterface({ input: this.child.stdout })
    this.lines.on('line', line => this.onLine(line))
    this.child.stderr.on('data', chunk => {
      const message = String(chunk).trim()
      if (message.length > 0) this.ctx.logger.warn('dsh-rs-bridge stderr: ' + message)
    })
    this.child.on('error', error => this.failAll(error))
    this.child.on('exit', (code, signal) => {
      if (!this.stopping) {
        this.failAll(new Error('dsh-rs-bridge exited unexpectedly (code=' + code + ', signal=' + signal + ')'))
      }
    })
  }

  request<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    if (this.child.killed || this.child.stdin.destroyed || !this.child.stdin.writable) {
      return Promise.reject(new Error('dsh-rs-bridge stdin is not writable'))
    }

    const id = String(++this.nextId)
    const payload = JSON.stringify({
      protocol: BRIDGE_PROTOCOL,
      id,
      method,
      params,
    }) + '\n'

    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pending.delete(id)
        reject(new Error('dsh-rs-bridge request timed out: ' + method))
      }, this.config.requestTimeoutMs)
      this.pending.set(id, {
        resolve: value => resolve(value as T),
        reject,
        timeout,
      })
      this.child.stdin.write(payload, error => {
        if (!error) return
        const pending = this.pending.get(id)
        if (!pending) return
        this.pending.delete(id)
        clearTimeout(pending.timeout)
        pending.reject(error)
      })
    })
  }

  async shutdown(): Promise<void> {
    try {
      await this.request<{ stopping: boolean }>('bridge.shutdown')
    } catch (error) {
      this.child.kill()
      throw error
    }
    this.stopping = true
    await new Promise<void>((resolve, reject) => {
      let settled = false
      const onExit = (): void => {
        if (settled) return
        settled = true
        clearTimeout(timeout)
        resolve()
      }
      const timeout = setTimeout(() => {
        if (settled) return
        settled = true
        this.child.kill()
        reject(new Error('timed out waiting for dsh-rs-bridge shutdown'))
      }, this.config.shutdownTimeoutMs)
      this.child.once('exit', onExit)
      if (this.child.exitCode !== null || this.child.signalCode !== null) onExit()
    })
    this.lines.close()
  }

  private onLine(line: string): void {
    let message: RpcMessage
    try {
      message = JSON.parse(line) as RpcMessage
    } catch (error) {
      this.failAll(new Error('invalid JSONL from dsh-rs-bridge: ' + String(error)))
      return
    }
    if (message.protocol !== BRIDGE_PROTOCOL) {
      this.failAll(new Error('unsupported bridge protocol: ' + String(message.protocol)))
      return
    }

    if (message.method === 'runtime.event') {
      const notification = message.params as RuntimeNotification
      const sessionId = notification?.event?.dsh_session_id
      const seq = notification?.host_seq
      if (typeof sessionId !== 'string' || !Number.isSafeInteger(seq)) {
        this.failAll(new Error('malformed runtime.event notification'))
        return
      }
      const previous = this.lastHostSeq.get(sessionId) ?? 0
      if ((seq as number) <= previous) {
        this.failAll(new Error('non-monotonic host event sequence for session ' + sessionId))
        return
      }
      this.lastHostSeq.set(sessionId, seq as number)
      this.emit('runtime-event', notification)
      return
    }

    if (message.id === undefined || message.id === null) return
    const id = String(message.id)
    const pending = this.pending.get(id)
    if (!pending) return
    this.pending.delete(id)
    clearTimeout(pending.timeout)
    if (message.error) {
      pending.reject(new Error(message.error.message ?? ('bridge request failed: ' + id)))
    } else {
      pending.resolve(message.result)
    }
  }

  private failAll(error: Error): void {
    for (const [id, pending] of this.pending) {
      clearTimeout(pending.timeout)
      pending.reject(error)
      this.pending.delete(id)
    }
    this.emit('bridge-error', error)
  }
}

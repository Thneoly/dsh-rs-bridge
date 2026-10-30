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
  /** Respawn the child after an unexpected exit (default true). */
  autoRespawn?: boolean
  /** Give up respawning after this many unexpected exits (default 5); the
   * client then stays dead and only 'bridge-error' fires. */
  maxRestarts?: number
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
    /** Adapters that forward permission requests set this; older bridges omit it. */
    permission_requests?: boolean
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

/** Owns one Rust bridge process and multiplexes request responses with async event notifications.
 * The durable journals make a bridge restart survivable (host_seq continues,
 * bindings replay), so after an unexpected exit the child is respawned and a
 * 'bridge-restarted' event tells agents to reattach via session.restore. */
export class JsonlBridgeClient extends EventEmitter {
  private child!: ChildProcessWithoutNullStreams
  private lines!: ReadlineInterface
  private readonly pending = new Map<string, PendingRequest>()
  private readonly lastHostSeq = new Map<string, number>()
  private nextId = 0
  private stopping = false
  private restarts = 0
  private generation = 0

  constructor(private readonly ctx: Context, private readonly config: BridgeProcessConfig) {
    super()
    this.child = this.spawnChild()
  }

  /** How many times the child died and came back. */
  get respawnGeneration(): number {
    return this.generation
  }

  /** Test-only: kill the child exactly the way a crash would (no orderly
   * shutdown), driving the respawn path deterministically. */
  simulateUnexpectedExit(): void {
    this.child.kill()
  }

  private spawnChild(): ChildProcessWithoutNullStreams {
    const child = spawn(this.config.command, this.config.args, {
      cwd: this.config.cwd,
      env: { ...process.env, ...this.config.env },
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    const lines = createInterface({ input: child.stdout })
    lines.on('line', line => this.onLine(line))
    child.stderr.on('data', chunk => {
      const message = String(chunk).trim()
      if (message.length > 0) this.ctx.logger.warn('dsh-rs-bridge stderr: ' + message)
    })
    child.on('error', error => this.failAll(error))
    child.on('exit', (code, signal) => this.onExit(code, signal))
    this.lines = lines
    return child
  }

  private onExit(code: number | null, signal: NodeJS.Signals | null): void {
    if (this.stopping) return
    this.failAll(new Error('dsh-rs-bridge exited unexpectedly (code=' + code + ', signal=' + signal + ')'))
    const maxRestarts = this.config.maxRestarts ?? 5
    if ((this.config.autoRespawn ?? true) === false || this.restarts >= maxRestarts) {
      this.ctx.logger.error(
        'dsh-rs-bridge is gone (' + this.restarts + ' restarts used); giving up',
      )
      return
    }
    this.restarts += 1
    this.generation += 1
    this.ctx.logger.info('dsh-rs-bridge exited; respawning (generation ' + this.generation + ')')
    this.child = this.spawnChild()
    this.emit('bridge-restarted', { generation: this.generation })
  }

  request<T>(method: string, params: Record<string, unknown> = {}, timeoutMs?: number): Promise<T> {
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
      }, timeoutMs ?? this.config.requestTimeoutMs)
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
    // Declare shutdown intent BEFORE the RPC: if the child dies while the
    // request is in flight, onExit must not respawn a successor nobody owns.
    this.stopping = true
    try {
      await this.request<{ stopping: boolean }>('bridge.shutdown')
    } catch (error) {
      if (this.child.exitCode === null && this.child.signalCode === null) this.child.kill()
      throw error
    }
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

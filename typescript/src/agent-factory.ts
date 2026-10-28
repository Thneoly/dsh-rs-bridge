import type { Context } from '@deepseek-ai/cordis'
import type {
  Agent,
  AgentCancelCause,
  AgentFactory,
  AgentHandle,
  AgentOptions,
  AgentStatus,
  CancelOptions,
  CreateAgentOptions,
  Inbox,
  InboxTarget,
  ResumeAgentOptions,
  TurnBoundaryProjection,
} from '@deepseek-ai/dsh-agent'
import { AssistantStreamAccumulator, createAssistantMessage, LlmAttemptId } from '@deepseek-ai/dsh-llm'
import type { AssistantStreamRecord, StreamChunk } from '@deepseek-ai/dsh-llm'
import type { AssistantStreamFrame } from '@deepseek-ai/dsh-agent'
import { createScope, scopeTarget } from '@deepseek-ai/dsh-scope'
import type { Scope } from '@deepseek-ai/dsh-scope'
import {
  interruptedTurnClosers,
  SessionPreparation,
  SessionSeq,
} from '@deepseek-ai/dsh-session'
import type {
  Session,
  SessionEvent,
  SessionId,
  UserMessage,
} from '@deepseek-ai/dsh-session'
import type {
  SessionHandle,
  SessionPersistence,
} from '@deepseek-ai/dsh-session-persistence'
import type {
  BridgeProcessConfig,
  RuntimeDescriptor,
  RuntimeEventEnvelope,
  RuntimeExecutionSummary,
  RuntimeNotification,
} from './bridge-client.js'
import { JsonlBridgeClient } from './bridge-client.js'

declare module '@deepseek-ai/dsh-agent' {
  interface AgentOptions {
    /** Select a registered Rust Runtime Adapter for this agent. */
    runtimeId?: string
  }
}

/** How the DSH side answers runtime permission requests it cannot surface
 * interactively yet (DSH exposes no host round-trip channel to the Agent). */
export type PermissionPolicy = 'deny' | 'allow'

export interface RuntimeBridgeFactoryConfig extends BridgeProcessConfig {
  runtimeId: string
  permissionPolicy: PermissionPolicy
  /** Budget for one external turn (session.wait), separate from the short
   * request timeout: real-model turns routinely exceed the default 30s RPC
   * budget and must not be failed spuriously. */
  turnTimeoutMs: number
}

interface StoredSession {
  handle: SessionHandle
}

function abortError(signal: AbortSignal, label: string): Error {
  return signal.reason instanceof Error ? signal.reason : new Error(label + ': ' + String(signal.reason))
}

async function raceAbort<T>(promise: Promise<T>, signal: AbortSignal | undefined, label: string): Promise<T> {
  if (signal === undefined) return promise
  if (signal.aborted) throw abortError(signal, label)
  let onAbort: (() => void) | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        onAbort = () => reject(abortError(signal, label))
        signal.addEventListener('abort', onAbort, { once: true })
      }),
    ])
  } finally {
    if (onAbort !== undefined) signal.removeEventListener('abort', onAbort)
  }
}

function promptText(message: UserMessage): string {
  return message.content.map(block => {
    if (block.type === 'text') return block.text
    return JSON.stringify(block)
  }).join('\n')
}

function outputText(event: RuntimeEventEnvelope): string {
  const value = event.payload.text
  if (typeof value === 'string') return value
  const content = event.payload.content
  if (typeof content === 'string') return content
  return ''
}

class DurableInbox implements Inbox {
  constructor(
    private readonly owner: RemoteRuntimeAgent,
    private readonly session: Session,
  ) {}

  get nextTurn(): readonly UserMessage[] {
    return this.current()['next-turn']
  }

  get nextStep(): readonly UserMessage[] {
    return this.current()['next-step']
  }

  clear(): void {
    for (const target of ['next-step', 'next-turn'] as const) {
      const messages = this.current()[target]
      if (messages.length === 0) continue
      this.mutate(target, 0, messages.length, [], 'canceled', true)
    }
  }

  append(target: InboxTarget, message: UserMessage): void {
    this.mutate(target, this.current()[target].length, 0, [message], undefined, true)
  }

  prepend(target: InboxTarget, message: UserMessage): void {
    this.mutate(target, 0, 0, [message], undefined, true)
  }

  replace(messageId: string, newMessage: UserMessage): boolean {
    for (const target of ['next-turn', 'next-step'] as const) {
      const index = this.current()[target].findIndex(message => message.id === messageId)
      if (index < 0) continue
      this.mutate(target, index, 1, [newMessage], undefined, true)
      return true
    }
    return false
  }

  remove(messageId: string): boolean {
    for (const target of ['next-turn', 'next-step'] as const) {
      const index = this.current()[target].findIndex(message => message.id === messageId)
      if (index < 0) continue
      this.mutate(target, index, 1, [], undefined, true)
      return true
    }
    return false
  }

  splice(
    target: InboxTarget,
    start: number,
    deleteCount: number,
    inserted: UserMessage[],
  ): UserMessage[] {
    return this.mutate(target, start, deleteCount, inserted, undefined, true)
  }

  claim(target: InboxTarget, turn: number): UserMessage | undefined {
    const removed = this.mutate(target, 0, 1, [], undefined, false)
    const message = removed[0]
    if (message !== undefined) this.owner.notify('agent/inbox/claimed', { message, turn })
    return message
  }

  private current() {
    return this.owner.ctx.sessionProjections.stateOf(this.session, 'inbox') ?? {
      'next-turn': [],
      'next-step': [],
    }
  }

  private mutate(
    target: InboxTarget,
    start: number,
    deleteCount: number,
    inserted: UserMessage[],
    outcome: 'canceled' | undefined,
    notifyDiscarded: boolean,
  ): UserMessage[] {
    const list = this.current()[target]
    const actualStart = start < 0 ? Math.max(list.length + start, 0) : Math.min(start, list.length)
    const actualDeleteCount = Math.min(Math.max(deleteCount, 0), list.length - actualStart)
    const removed = list.slice(actualStart, actualStart + actualDeleteCount)
    this.session.append('agent/inbox/spliced', {
      target,
      start: actualStart,
      ...(actualDeleteCount === 0 ? {} : { removedCount: actualDeleteCount }),
      inserted,
      ...(outcome === undefined || actualDeleteCount === 0 ? {} : { outcome }),
    })
    for (const message of inserted) this.owner.notify('agent/inbox/inserted', { message })
    if (notifyDiscarded) {
      for (const message of removed) this.owner.notify('agent/inbox/discarded', { message })
    }
    return removed
  }
}

/**
 * DSH Agent implementation whose turn execution is delegated to the Rust
 * runtime bridge. External text is published through DSH's process-local
 * assistant-stream frames and compacted into the final durable Session event.
 */
class RemoteRuntimeAgent implements Agent {
  readonly id: SessionId
  readonly options: AgentOptions
  readonly session: Session
  readonly ctx: Context
  readonly scope: Scope
  readonly inbox: DurableInbox

  private readonly carrier: object
  private readonly stored?: StoredSession
  private statusValue: AgentStatus = 'idle'
  private turnTask?: Promise<void>
  private maintenance?: { promise: Promise<unknown>; controller: AbortController }
  private disposed = false
  private currentExecutionActive = false
  private externalSessionId?: string
  private cancelCause?: AgentCancelCause
  private streamRevision = 0
  private attemptCounter = 0

  constructor(
    private readonly hostCtx: Context,
    session: Session,
    options: AgentOptions,
    private readonly runtimeId: string,
    private readonly bridge: JsonlBridgeClient,
    private readonly permissionPolicy: PermissionPolicy,
    private readonly turnTimeoutMs: number,
    stored?: StoredSession,
    externalSessionId?: string,
  ) {
    this.id = session.id
    this.session = session
    this.options = options
    this.stored = stored
    this.externalSessionId = externalSessionId
    this.scope = createScope(hostCtx, this)
    this.ctx = this.scope.ctx
    this.carrier = scopeTarget(this, this)
    this.inbox = new DurableInbox(this, session)
  }

  get status(): AgentStatus {
    return this.statusValue
  }

  notify(name: string, payload: Record<string, unknown>): void {
    const args: unknown[] = [this.carrier, name, { ...payload, agent: this }]
    const callbacks = this.hostCtx.events.dispatch('emit', args)
    for (const callbackValue of callbacks) {
      const callback = callbackValue as (...args: unknown[]) => unknown
      try {
        const result = callback(...args)
        void Promise.resolve(result).catch(error => {
          this.hostCtx.logger.warn('agent event ' + name + ' listener rejected: ' + String(error))
        })
      } catch (error) {
        this.hostCtx.logger.warn('agent event ' + name + ' listener threw: ' + String(error))
      }
    }
  }

  send(message: UserMessage, target: InboxTarget, wakeup: boolean): void {
    this.inbox.append(target, message)
    if (wakeup) this.wake()
  }

  followup(message: UserMessage): void {
    this.inbox.append('next-turn', message)
    this.wake()
  }

  steer(message: UserMessage): void {
    this.inbox.append('next-step', message)
    this.wake()
  }

  inject(message: UserMessage): void {
    this.inbox.append('next-step', message)
  }

  cancel(cause: AgentCancelCause, options?: CancelOptions): void {
    if (!this.turnTask && !this.maintenance) return
    this.cancelCause ??= cause
    if (!options?.keepInbox) this.inbox.clear()
    this.maintenance?.controller.abort(new Error('agent maintenance cancelled: ' + cause.kind))
    if (this.currentExecutionActive && this.externalSessionId !== undefined) {
      void this.bridge.request<RuntimeExecutionSummary>('session.cancel', {
        dsh_session_id: String(this.id),
        reason: cause.kind,
      }, this.turnTimeoutMs).catch(error => {
        this.hostCtx.logger.warn('runtime cancellation failed for session ' + String(this.id) + ': ' + String(error))
      })
    }
  }

  whenIdle(): Promise<void> {
    const pending: Promise<unknown>[] = []
    if (this.turnTask !== undefined) pending.push(this.turnTask)
    if (this.maintenance !== undefined) pending.push(this.maintenance.promise)
    return Promise.all(pending).then(() => undefined)
  }

  runMaintenance<T>(task: (signal: AbortSignal) => Promise<T>): Promise<T> {
    if (this.turnTask !== undefined || this.maintenance !== undefined) {
      return Promise.reject(new Error('agent is already active: ' + String(this.id)))
    }
    const controller = new AbortController()
    const promise = Promise.resolve().then(() => task(controller.signal))
    let tracked: Promise<T>
    tracked = promise.finally(() => {
      if (this.maintenance?.promise === tracked) this.maintenance = undefined
    })
    this.maintenance = { promise: tracked, controller }
    return tracked
  }

  beginDispose(): void {
    this.disposed = true
    this.cancel({ kind: 'disposed' }, { keepInbox: true })
  }

  private wake(): void {
    if (this.disposed || this.turnTask !== undefined || this.maintenance !== undefined) return
    this.setStatus('running')
    let tracked: Promise<void>
    tracked = this.drive()
      .catch(error => {
        this.hostCtx.logger.warn('external runtime driver failed for session ' + String(this.id) + ': ' + String(error))
      })
      .finally(() => {
        if (this.turnTask === tracked) this.turnTask = undefined
        this.setStatus('idle')
        if (!this.disposed && (this.inbox.nextTurn.length > 0 || this.inbox.nextStep.length > 0)) this.wake()
      })
    this.turnTask = tracked
  }

  /** Pick an option per the configured permission policy and deliver it.
   * When no option matches the policy (e.g. deny with only allow options),
   * stay silent: the bridge auto-denies after its own bounded deadline. */
  private async answerPermission(payload: Record<string, unknown>): Promise<void> {
    const requestId = typeof payload.request_id === 'string' ? payload.request_id : undefined
    if (requestId === undefined) {
      this.hostCtx.logger.warn('permission request without request_id for session ' + String(this.id))
      return
    }
    const options = Array.isArray(payload.options) ? payload.options : []
    const optionIdOf = (option: unknown): string | undefined => {
      const record = option as Record<string, unknown> | undefined
      const value = record?.optionId ?? record?.option_id
      return typeof value === 'string' ? value : undefined
    }
    const kindOf = (option: unknown): string => {
      const record = option as Record<string, unknown> | undefined
      return typeof record?.kind === 'string' ? record.kind : ''
    }
    const wanted = this.permissionPolicy === 'allow' ? 'allow' : 'reject'
    const chosen = options.find(option => kindOf(option).startsWith(wanted))
      ?? (this.permissionPolicy === 'allow' ? options[0] : undefined)
    const optionId = chosen === undefined ? undefined : optionIdOf(chosen)
    if (optionId === undefined) {
      this.hostCtx.logger.warn(
        'permission policy ' + this.permissionPolicy + ' matched no option for session ' + String(this.id)
          + '; letting the bridge deadline decide',
      )
      return
    }
    try {
      await this.bridge.request<{ delivered: boolean }>('permission.respond', {
        dsh_session_id: String(this.id),
        request_id: requestId,
        option_id: optionId,
      })
    } catch (error) {
      this.hostCtx.logger.warn(
        'permission response failed for session ' + String(this.id) + ': ' + String(error),
      )
    }
  }

  private setStatus(status: AgentStatus): void {
    if (this.statusValue === status) return
    this.statusValue = status
    this.notify('agent/status', { status })
  }

  private async drive(): Promise<void> {
    while (!this.disposed) {
      const target: InboxTarget | undefined = this.inbox.nextTurn.length > 0
        ? 'next-turn'
        : this.inbox.nextStep.length > 0
          ? 'next-step'
          : undefined
      if (target === undefined) return
      const boundaries = this.ctx.sessionProjections.stateOf(this.session, 'turnBoundary') as TurnBoundaryProjection | undefined
      const turn = (boundaries?.lastTurn ?? 0) + 1
      const message = this.inbox.claim(target, turn)
      if (message === undefined) continue
      await this.runTurn(message, turn)
    }
  }

  private async runTurn(message: UserMessage, turn: number): Promise<void> {
    const step = 1
    const prompt = promptText(message)
    let text = ''
    let failureMessage: string | undefined
    this.cancelCause = undefined
    this.currentExecutionActive = true

    this.session.append('turn/start', { turn })
    this.session.append('user/message', message, { surfaceOp: 'append' })
    this.session.append('step/start', { turn, step })

    const attemptId = LlmAttemptId(String(this.id) + ':' + String(++this.attemptCounter))
    const accumulator = new AssistantStreamAccumulator()
    let chunkIndex = 0
    let streamEnded = false

    this.notify('agent/assistant-stream', {
      frame: {
        type: 'start',
        attemptId,
        revision: ++this.streamRevision,
        turn,
        step,
      } satisfies AssistantStreamFrame,
    })

    const pushChunk = (chunk: StreamChunk): void => {
      const timed = accumulator.push({ time: Date.now(), chunk })
      this.notify('agent/assistant-stream', {
        frame: {
          type: 'chunk',
          attemptId,
          revision: ++this.streamRevision,
          index: chunkIndex++,
          time: timed.time,
          chunk: timed.chunk,
        } satisfies AssistantStreamFrame,
      })
    }

    const pushText = (chunkText: string): void => {
      if (chunkText.length === 0) return
      text += chunkText
      pushChunk({ type: 'text-delta', index: 0, text: chunkText })
    }

    const onRuntimeEvent = (notification: RuntimeNotification): void => {
      const event = notification.event
      if (event.dsh_session_id !== String(this.id)) return
      if (event.event_type === 'assistant.delta') {
        pushText(outputText(event))
      } else if (event.event_type === 'assistant.message' && text.length === 0) {
        pushText(outputText(event))
      } else if (event.event_type.startsWith('tool.')) {
        // Tool activity belongs to the external agent's execution trace. Keep it
        // transient instead of fabricating DSH model tool-call/result semantics.
        this.notify('agent/runtime-event', {
          turn,
          step,
          event: {
            runtime_id: event.runtime_id,
            external_session_id: event.external_session_id,
            event_type: event.event_type,
            ...(event.runtime_seq === undefined ? {} : { runtime_seq: event.runtime_seq }),
            payload: event.payload,
            at_unix_ms: event.at_unix_ms,
          },
        })
      } else if (event.event_type === 'permission.requested') {
        // The bridge forwarded the runtime's permission question. DSH exposes
        // no interactive host channel to the Agent yet, so answer per the
        // configured policy and keep an audit trace; a future DSH permission
        // UI can replace the automatic answer.
        this.notify('agent/runtime-event', {
          turn,
          step,
          event: {
            runtime_id: event.runtime_id,
            external_session_id: event.external_session_id,
            event_type: event.event_type,
            ...(event.runtime_seq === undefined ? {} : { runtime_seq: event.runtime_seq }),
            payload: event.payload,
            at_unix_ms: event.at_unix_ms,
          },
        })
        void this.answerPermission(event.payload)
      } else if (event.event_type === 'permission.responded') {
        this.notify('agent/runtime-event', {
          turn,
          step,
          event: {
            runtime_id: event.runtime_id,
            external_session_id: event.external_session_id,
            event_type: event.event_type,
            ...(event.runtime_seq === undefined ? {} : { runtime_seq: event.runtime_seq }),
            payload: event.payload,
            at_unix_ms: event.at_unix_ms,
          },
        })
      } else if (event.event_type === 'turn.failed' || event.event_type === 'runtime.failed') {
        failureMessage = typeof event.payload.message === 'string'
          ? event.payload.message
          : 'external runtime reported a failed turn'
      }
    }
    this.bridge.on('runtime-event', onRuntimeEvent)

    let terminalStatus: RuntimeExecutionSummary['status'] = 'failed'
    try {
      if (this.externalSessionId === undefined) {
        const started = await this.bridge.request<RuntimeExecutionSummary>('session.start', {
          dsh_session_id: String(this.id),
          runtime_id: this.runtimeId,
          prompt,
          cwd: this.session.header.cwd,
        })
        if (started.external_session_id === null) {
          throw new Error('runtime start did not return an external session id')
        }
        this.externalSessionId = started.external_session_id
      } else {
        await this.bridge.request<{ accepted: boolean }>('session.send', {
          dsh_session_id: String(this.id),
          text: prompt,
        })
      }

      const cancellationCause = this.getCancelCause()
      if (cancellationCause !== undefined) {
        await this.bridge.request<RuntimeExecutionSummary>('session.cancel', {
          dsh_session_id: String(this.id),
          reason: cancellationCause.kind,
        }, this.turnTimeoutMs)
      }
      const summary = await this.bridge.request<RuntimeExecutionSummary>('session.wait', {
        dsh_session_id: String(this.id),
      }, this.turnTimeoutMs)
      terminalStatus = summary.status
      if (summary.external_session_id !== null) this.externalSessionId = summary.external_session_id
    } catch (error) {
      failureMessage = String(error)
      terminalStatus = this.getCancelCause() === undefined ? 'failed' : 'cancelled'
    } finally {
      this.bridge.off('runtime-event', onRuntimeEvent)
    }

    const finishChunk: StreamChunk = terminalStatus === 'completed'
      ? { type: 'finish', reason: { kind: 'stop' } }
      : terminalStatus === 'cancelled'
        ? {
          type: 'finish',
          reason: {
            kind: 'aborted',
            failure: {
              message: failureMessage ?? 'external runtime cancelled',
              code: 'CANCELLED',
            },
          },
        }
        : {
          type: 'finish',
          reason: {
            kind: 'error',
            failure: {
              message: failureMessage ?? 'external runtime failed',
              code: 'EXTERNAL_RUNTIME_FAILED',
            },
          },
        }

    try {
      pushChunk(finishChunk)
      const stream = [...accumulator.snapshot()] as AssistantStreamRecord[]
      let eventType: 'assistant/message' | 'assistant/attempt'
      let settlementSeq: SessionSeq

      if (text.length > 0) {
        const assistantMessage = createAssistantMessage({
          content: [{ type: 'text', text }],
          source: {
            provider: 'external-runtime',
            model: this.runtimeId,
          },
        })
        const settlementEvent = this.session.append('assistant/message', {
          turn,
          step,
          message: assistantMessage,
          stream,
          ...(terminalStatus === 'cancelled' ? { interrupted: true as const } : {}),
        }, { surfaceOp: 'append' })
        settlementSeq = settlementEvent.seq
        eventType = 'assistant/message'
      } else {
        const settlementEvent = this.session.append('assistant/attempt', { turn, step, stream })
        settlementSeq = settlementEvent.seq
        eventType = 'assistant/attempt'
      }

      this.notify('agent/assistant-stream', {
        frame: {
          type: 'end',
          attemptId,
          revision: ++this.streamRevision,
          index: chunkIndex,
          outcome: { kind: 'committed', eventType, seq: settlementSeq },
        } satisfies AssistantStreamFrame,
      })
      streamEnded = true

      this.session.append('step/end', { turn, step })
      if (terminalStatus === 'completed') {
        this.session.append('turn/end', { turn, reason: { kind: 'completed' } })
      } else if (terminalStatus === 'cancelled') {
        this.session.append('turn/end', {
          turn,
          reason: {
            kind: 'aborted',
            reason: this.getCancelCause() ?? { kind: 'user' },
          },
        })
      } else {
        this.session.append('turn/end', {
          turn,
          reason: {
            kind: 'error',
            error: {
              message: failureMessage ?? 'external runtime failed',
              code: 'UNKNOWN',
            },
          },
        })
      }
    } catch (error) {
      if (!streamEnded) {
        this.notify('agent/assistant-stream', {
          frame: {
            type: 'end',
            attemptId,
            revision: ++this.streamRevision,
            index: chunkIndex,
            outcome: { kind: 'abandoned' },
          } satisfies AssistantStreamFrame,
        })
      }
      throw error
    } finally {
      this.currentExecutionActive = false
    }
  }

  private getCancelCause(): AgentCancelCause | undefined {
    return this.cancelCause
  }


}

/** AgentFactory that maps DSH Agent turns onto a selected Rust Runtime Adapter. */
export class BridgeAgentFactory implements AgentFactory {
  private readonly live = new Set<() => Promise<void>>()

  constructor(
    private readonly ctx: Context,
    private readonly bridge: JsonlBridgeClient,
    private readonly config: RuntimeBridgeFactoryConfig,
  ) {}

  async createAgent(ownerCtx: Context, options: CreateAgentOptions): Promise<AgentHandle> {
    ownerCtx.fiber.assertActive()
    const runtimeId = options.agentOptions?.runtimeId ?? this.config.runtimeId
    const descriptor = await raceAbort(
      this.bridge.request<RuntimeDescriptor>('runtime.probe', { runtime_id: runtimeId }),
      options.signal,
      'runtime probe cancelled',
    )
    if (descriptor.health.status !== 'ready') {
      throw new Error('runtime ' + runtimeId + ' is not ready: ' + String(descriptor.health.detail ?? descriptor.health.status))
    }

    const preparation = SessionPreparation.create(this.ctx.sessions.prepare(options.sessionId, {
      ...(options.seed === undefined ? {} : { seed: options.seed }),
      ...(options.meta === undefined ? {} : { meta: options.meta }),
      ...(options.inheritedEventCount === undefined ? {} : { inheritedEventCount: options.inheritedEventCount }),
    }))
    const session = preparation.session
    let stored: StoredSession | undefined
    try {
      stored = await raceAbort(this.createStoredSession(session, options.signal), options.signal, 'session persistence create cancelled')
      const agent = new RemoteRuntimeAgent(this.ctx, session, options.agentOptions ?? {}, runtimeId, this.bridge, this.config.permissionPolicy, this.config.turnTimeoutMs, stored)
      let detachSession: (() => void) | undefined
      let detachAgent: (() => void) | undefined
      let teardownPromise: Promise<void> | undefined
      const ownerAbort = new AbortController()
      const lifecycleSignal = options.signal === undefined
        ? ownerAbort.signal
        : AbortSignal.any([options.signal, ownerAbort.signal])

      const teardown = (): Promise<void> => teardownPromise ??= (async () => {
        ownerAbort.abort(new Error('agent owner disposed: ' + String(options.sessionId)))
        agent.beginDispose()
        const failures: unknown[] = []
        try {
          await agent.whenIdle()
        } catch (error) {
          failures.push(error)
        }
        try {
          detachAgent?.()
        } catch (error) {
          failures.push(error)
        }
        try {
          detachSession?.()
        } catch (error) {
          failures.push(error)
        }
        try {
          await agent.scope.dispose()
        } catch (error) {
          failures.push(error)
        }
        try {
          await stored?.handle.close()
        } catch (error) {
          failures.push(error)
        } finally {
          stored = undefined
        }
        this.live.delete(disposeHandle)
        if (failures.length > 0) throw new AggregateError(failures, 'failed to dispose DSH runtime agent')
      })()

      let ownerDisposer: (() => Promise<void> | void) | undefined
      const disposeHandle = async (): Promise<void> => {
        try {
          await teardown()
        } finally {
          await ownerDisposer?.()
        }
      }
      ownerDisposer = ownerCtx.effect(function* () {
        yield async () => teardown()
      }, 'dshRuntimeBridge.lifecycle(' + String(options.sessionId) + ')')
      this.live.add(disposeHandle)

      try {
        lifecycleSignal.throwIfAborted()
        const setupCommit = await raceAbort(
          Promise.resolve(options.setup?.(agent.ctx, agent)),
          lifecycleSignal,
          'agent setup cancelled',
        )
        setupCommit?.commit()
        lifecycleSignal.throwIfAborted()
        detachSession = agent.ctx.sessions.enter(session)
        detachAgent = this.ctx.agents.enter(agent, options.parentAgent)
        agent.ctx.sessions.announce(session)
        lifecycleSignal.throwIfAborted()
        await this.ctx.agents.announce(agent, 'startup', lifecycleSignal)
        lifecycleSignal.throwIfAborted()
        return { agent, dispose: disposeHandle }
      } catch (error) {
        await teardown().catch(() => {})
        throw error
      }
    } catch (error) {
      await stored?.handle.close().catch(() => {})
      throw error
    } finally {
      preparation[Symbol.dispose]()
    }
  }

  async resume(ownerCtx: Context, options: ResumeAgentOptions): Promise<AgentHandle> {
    ownerCtx.fiber.assertActive()
    const persistence = this.ctx.get('sessionPersistence') as SessionPersistence | undefined
    if (persistence === undefined) {
      throw new Error('cannot resume DSH session: session persistence is not configured')
    }

    let openedHandle: SessionHandle | undefined
    let stored: StoredSession | undefined
    let preparation: SessionPreparation | undefined
    try {
      const openPromise = persistence.open(options.resumeSessionId, 'write', {
        ...(options.signal === undefined ? {} : { signal: options.signal }),
      })
      try {
        openedHandle = await raceAbort(
          openPromise,
          options.signal,
          'session persistence open cancelled',
        )
      } catch (error) {
        void openPromise.then(handle => handle.close()).catch(() => {})
        throw error
      }

      const coldRead = await raceAbort(
        openedHandle.read(0, undefined, {
          ...(options.signal === undefined ? {} : { signal: options.signal }),
        }),
        options.signal,
        'session persistence read cancelled',
      )
      const persisted = [...coldRead.events]
      const closers = interruptedTurnClosers(persisted)
      if (closers.length > 0) {
        await raceAbort(
          openedHandle.append(closers, {
            ...(options.signal === undefined ? {} : { signal: options.signal }),
          }),
          options.signal,
          'interrupted-turn repair append cancelled',
        )
      }

      preparation = SessionPreparation.create(this.ctx.sessions.prepare(options.resumeSessionId, {
        seed: [...persisted, ...closers],
        meta: structuredClone(openedHandle.header),
        inheritedEventCount: openedHandle.inheritedEventCount,
        eventState: coldRead.eventState,
      }))
      const session = preparation.session
      stored = { handle: openedHandle }
      openedHandle = undefined

      const restored = await raceAbort(
        this.bridge.request<RuntimeExecutionSummary>('session.restore', {
          dsh_session_id: String(options.resumeSessionId),
        }),
        options.signal,
        'runtime session restore cancelled',
      )
      if (restored.external_session_id === null) {
        throw new Error('runtime restore did not return an external session id')
      }
      const requestedRuntime = options.agentOptions?.runtimeId
      if (requestedRuntime !== undefined && requestedRuntime !== restored.runtime_id) {
        throw new Error(
          'cannot resume DSH session ' + String(options.resumeSessionId) +
          ' with runtime ' + requestedRuntime +
          '; persisted session belongs to runtime ' + restored.runtime_id,
        )
      }

      const agent = new RemoteRuntimeAgent(
        this.ctx,
        session,
        options.agentOptions ?? {},
        restored.runtime_id,
        this.bridge,
        this.config.permissionPolicy,
        this.config.turnTimeoutMs,
        stored,
        restored.external_session_id,
      )
      let detachSession: (() => void) | undefined
      let detachAgent: (() => void) | undefined
      let teardownPromise: Promise<void> | undefined
      const ownerAbort = new AbortController()
      const lifecycleSignal = options.signal === undefined
        ? ownerAbort.signal
        : AbortSignal.any([options.signal, ownerAbort.signal])

      const teardown = (): Promise<void> => teardownPromise ??= (async () => {
        ownerAbort.abort(new Error('agent owner disposed: ' + String(options.resumeSessionId)))
        agent.beginDispose()
        const failures: unknown[] = []
        try {
          await agent.whenIdle()
        } catch (error) {
          failures.push(error)
        }
        try {
          detachAgent?.()
        } catch (error) {
          failures.push(error)
        }
        try {
          detachSession?.()
        } catch (error) {
          failures.push(error)
        }
        try {
          await agent.scope.dispose()
        } catch (error) {
          failures.push(error)
        }
        try {
          await stored?.handle.close()
        } catch (error) {
          failures.push(error)
        } finally {
          stored = undefined
        }
        this.live.delete(disposeHandle)
        if (failures.length > 0) throw new AggregateError(failures, 'failed to dispose resumed DSH runtime agent')
      })()

      let ownerDisposer: (() => Promise<void> | void) | undefined
      const disposeHandle = async (): Promise<void> => {
        try {
          await teardown()
        } finally {
          await ownerDisposer?.()
        }
      }
      ownerDisposer = ownerCtx.effect(function* () {
        yield async () => teardown()
      }, 'dshRuntimeBridge.resume(' + String(options.resumeSessionId) + ')')
      this.live.add(disposeHandle)

      try {
        lifecycleSignal.throwIfAborted()
        const setupCommit = await raceAbort(
          Promise.resolve(options.setup?.(agent.ctx, agent)),
          lifecycleSignal,
          'resumed agent setup cancelled',
        )
        setupCommit?.commit()
        lifecycleSignal.throwIfAborted()
        detachSession = agent.ctx.sessions.enter(session)
        detachAgent = this.ctx.agents.enter(agent, options.parentAgent)
        agent.ctx.sessions.announce(session)
        lifecycleSignal.throwIfAborted()
        await this.ctx.agents.announce(agent, 'startup', lifecycleSignal)
        lifecycleSignal.throwIfAborted()
        return { agent, dispose: disposeHandle }
      } catch (error) {
        await teardown().catch(() => {})
        throw error
      }
    } catch (error) {
      await stored?.handle.close().catch(() => {})
      throw error
    } finally {
      await openedHandle?.close().catch(() => {})
      preparation?.[Symbol.dispose]()
    }
  }

  async disposeAll(): Promise<void> {
    const results = await Promise.allSettled([...this.live].map(dispose => dispose()))
    const failures = results
      .filter((result): result is PromiseRejectedResult => result.status === 'rejected')
      .map(result => result.reason)
    if (failures.length > 0) throw new AggregateError(failures, 'failed to dispose DSH runtime bridge agents')
  }

  private async createStoredSession(
    session: Session,
    signal?: AbortSignal,
  ): Promise<StoredSession | undefined> {
    const persistence = this.ctx.get('sessionPersistence') as SessionPersistence | undefined
    if (persistence === undefined) return undefined
    if (signal?.aborted) throw abortError(signal, 'session persistence create cancelled')
    const handle = await persistence.create(session.header, {
      inheritedEventCount: session.inheritedEventCount,
      ...(signal === undefined ? {} : { signal }),
    })
    return { handle }
  }


}

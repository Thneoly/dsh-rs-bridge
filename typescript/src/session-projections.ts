import { SessionSeq } from '@deepseek-ai/dsh-session'
import type { SessionEvent, SessionHeader, SessionLogOffset, UserMessage } from '@deepseek-ai/dsh-session'
import type {
  InboxState,
  InboxWireState,
  TurnBoundaryProjection,
} from '@deepseek-ai/dsh-agent'
import type { ProjectionDefinition } from '@deepseek-ai/dsh-session-projection'
import { z } from 'zod'

declare module '@deepseek-ai/dsh-session-projection/types' {
  interface SessionProjectionStateMap {
    inbox: InboxState
    turnBoundary: TurnBoundaryProjection
  }
  interface SessionProjectionMap {
    inbox: InboxWireState
  }
}

const inboxProjectionSchema = z.object({
  'next-turn': z.array(z.custom<UserMessage>()).readonly(),
  'next-step': z.array(z.custom<UserMessage>()).readonly(),
}).readonly()

/** Durable pending input, reconstructed from the DSH agent inbox splice log. */
export const inboxProjectionDefinition = {
  key: 'inbox',
  stateSchema: inboxProjectionSchema,
  init: (): InboxState => ({ 'next-turn': [], 'next-step': [] }),
  apply(state: InboxState, event: SessionEvent): InboxState {
    if (event.type !== 'agent/inbox/spliced') return state
    const splice = event.data
    try {
      const current = state[splice.target]
      const removedCount = splice.removedCount ?? 0
      if (
        !Number.isSafeInteger(splice.start) ||
        splice.start < 0 ||
        splice.start > current.length ||
        !Number.isSafeInteger(removedCount) ||
        removedCount < 0 ||
        splice.start + removedCount > current.length
      ) {
        throw new Error('invalid inbox splice')
      }
      const next = current.toSpliced(splice.start, removedCount, ...splice.inserted)
      const ids = new Set<string>()
      const all = splice.target === 'next-turn'
        ? [...next, ...state['next-step']]
        : [...state['next-turn'], ...next]
      for (const message of all) {
        if (ids.has(message.id)) throw new Error('message is already pending: ' + message.id)
        ids.add(message.id)
      }
      return splice.target === 'next-turn'
        ? { 'next-turn': next, 'next-step': state['next-step'] }
        : { 'next-turn': state['next-turn'], 'next-step': next }
    } catch (error) {
      throw new Error('invalid persisted inbox splice at session seq ' + event.seq, { cause: error })
    }
  },
  wire: {
    viewSchema: inboxProjectionSchema as unknown as z.ZodType<InboxWireState>,
    view: (state: InboxState): InboxWireState => state as unknown as InboxWireState,
  },
  stateVersion: 1,
} satisfies ProjectionDefinition<'inbox', InboxState>

const turnBoundaryProjectionSchema: z.ZodType<TurnBoundaryProjection> = z.object({
  openTurnStartSeq: z.number().int().nonnegative().transform(SessionSeq).nullable(),
  lastStepStartSeq: z.number().int().nonnegative().transform(SessionSeq).nullable(),
  lastStepBoundary: z.object({
    kind: z.union([z.literal('start'), z.literal('end')]),
    seq: z.number().int().nonnegative().transform(SessionSeq),
  }).nullable(),
  lastTurn: z.number().int().nonnegative(),
})

/** Host projection of turn and step boundaries for sessions driven without AgentLoop. */
export const turnBoundaryProjectionDefinition = {
  key: 'turnBoundary',
  stateVersion: 2,
  stateSchema: turnBoundaryProjectionSchema,
  init: (
    _header: SessionHeader,
    _inheritedEventCount: SessionLogOffset,
  ): TurnBoundaryProjection => ({
    openTurnStartSeq: null,
    lastStepStartSeq: null,
    lastStepBoundary: null,
    lastTurn: 0,
  }),
  apply(state: TurnBoundaryProjection, event: SessionEvent): TurnBoundaryProjection {
    switch (event.type) {
      case 'turn/start':
        return {
          ...state,
          openTurnStartSeq: event.seq,
          lastTurn: event.data.turn,
        }
      case 'turn/end':
        return { ...state, openTurnStartSeq: null }
      case 'step/start':
        return {
          ...state,
          lastStepStartSeq: event.seq,
          lastStepBoundary: { kind: 'start', seq: event.seq },
        }
      case 'step/end':
        return {
          ...state,
          lastStepBoundary: { kind: 'end', seq: event.seq },
        }
      default:
        return state
    }
  },
} satisfies ProjectionDefinition<'turnBoundary', TurnBoundaryProjection>

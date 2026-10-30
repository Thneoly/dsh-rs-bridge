import type { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import { BridgeAgentFactory, type RuntimeBridgeFactoryConfig } from './agent-factory.js'
import { JsonlBridgeClient } from './bridge-client.js'
import type { BridgeProcessConfig } from './bridge-client.js'
import {
  inboxProjectionDefinition,
  turnBoundaryProjectionDefinition,
} from './session-projections.js'

export const name = 'dsh-rs-bridge'
export const inject = ['agents', 'sessions', 'sessionProjections']

/** Rust bridge process and default Runtime Adapter selection. */
export interface Config {
  command: string
  args: string[]
  cwd: string
  env: Record<string, string>
  runtimeId: string
  requestTimeoutMs: number
  shutdownTimeoutMs: number
  /** How forwarded runtime permission requests are answered automatically:
   * 'deny' (safe default) or 'allow'. */
  permissionPolicy: 'deny' | 'allow'
  /** Budget for one external turn; session.wait/cancel use this instead of
   * the short request timeout. Default matches the bridge prompt budget. */
  turnTimeoutMs: number
  /** Respawn the bridge process after an unexpected exit and reattach
   * sessions via session.restore on their next turn (default true). */
  autoRespawn: boolean
  /** Give up respawning after this many unexpected exits (default 5). */
  maxRestarts: number
}

export const Config: z<Config> = z.object({
  command: z.string().required(),
  args: z.array(z.string()).default([]),
  cwd: z.string().default(''),
  env: z.dict(z.string()).default({}),
  runtimeId: z.string().default('mock'),
  requestTimeoutMs: z.number().default(30000),
  shutdownTimeoutMs: z.number().default(3000),
  permissionPolicy: z.union([z.const('deny'), z.const('allow')]).default('deny'),
  turnTimeoutMs: z.number().default(3600000),
  autoRespawn: z.boolean().default(true),
  maxRestarts: z.number().default(5),
})

export function apply(ctx: Context, config: Config): void {
  const processConfig: BridgeProcessConfig = {
    command: config.command,
    args: config.args,
    ...(config.cwd.length === 0 ? {} : { cwd: config.cwd }),
    env: config.env,
    requestTimeoutMs: config.requestTimeoutMs,
    shutdownTimeoutMs: config.shutdownTimeoutMs,
    autoRespawn: config.autoRespawn,
    maxRestarts: config.maxRestarts,
  }
  const factoryConfig: RuntimeBridgeFactoryConfig = {
    ...processConfig,
    runtimeId: config.runtimeId,
    permissionPolicy: config.permissionPolicy,
    turnTimeoutMs: config.turnTimeoutMs,
  }
  const bridge = new JsonlBridgeClient(ctx, processConfig)
  const factory = new BridgeAgentFactory(ctx, bridge, factoryConfig)

  ctx.effect(function* () {
    const unregisterTurnBoundary = ctx.sessionProjections.register(turnBoundaryProjectionDefinition)
    yield unregisterTurnBoundary
    const unregisterInbox = ctx.sessionProjections.register(inboxProjectionDefinition)
    yield unregisterInbox
    yield async () => {
      const failures: unknown[] = []
      try {
        await factory.disposeAll()
      } catch (error) {
        failures.push(error)
      }
      try {
        await bridge.shutdown()
      } catch (error) {
        failures.push(error)
      } finally {
        unregisterInbox()
        unregisterTurnBoundary()
      }
      if (failures.length > 0) throw new AggregateError(failures, 'dsh-rs-bridge plugin shutdown failed')
    }
  }, 'dshRuntimeBridge.transactions()')

  ctx.effect(() => ctx.agents.setFactory(factory), 'dshRuntimeBridge.setFactory()')
}

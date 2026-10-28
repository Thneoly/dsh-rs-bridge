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
}

export const Config: z<Config> = z.object({
  command: z.string().required(),
  args: z.array(z.string()).default([]),
  cwd: z.string().default(''),
  env: z.dict(z.string()).default({}),
  runtimeId: z.string().default('mock'),
  requestTimeoutMs: z.number().default(30000),
  shutdownTimeoutMs: z.number().default(3000),
})

export function apply(ctx: Context, config: Config): void {
  const processConfig: BridgeProcessConfig = {
    command: config.command,
    args: config.args,
    ...(config.cwd.length === 0 ? {} : { cwd: config.cwd }),
    env: config.env,
    requestTimeoutMs: config.requestTimeoutMs,
    shutdownTimeoutMs: config.shutdownTimeoutMs,
  }
  const factoryConfig: RuntimeBridgeFactoryConfig = {
    ...processConfig,
    runtimeId: config.runtimeId,
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

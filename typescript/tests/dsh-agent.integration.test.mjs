import assert from 'node:assert/strict'
import { resolve } from 'node:path'
import { test } from 'node:test'
import { Context } from '@deepseek-ai/cordis'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import SessionProjectionRegistry from '@deepseek-ai/dsh-session-projection'
import { BridgeAgentFactory } from '../dist/agent-factory.js'
import { JsonlBridgeClient } from '../dist/bridge-client.js'
import {
  inboxProjectionDefinition,
  turnBoundaryProjectionDefinition,
} from '../dist/session-projections.js'

test('DSH AgentFactory maps real Rust bridge turns into the DSH Session log', async () => {
  const command = process.env.DSH_RS_BRIDGE_BIN
  assert.ok(command, 'DSH_RS_BRIDGE_BIN must point to the built Rust bridge binary')

  const ctx = new Context()
  await ctx.plugin(SessionProjectionRegistry)
  await ctx.plugin(AgentRegistry)
  await ctx.plugin(SessionStore)

  const unregisterTurnBoundary = ctx.sessionProjections.register(turnBoundaryProjectionDefinition)
  const unregisterInbox = ctx.sessionProjections.register(inboxProjectionDefinition)
  const processConfig = {
    command,
    args: ['--stdio'],
    cwd: resolve(process.cwd(), '..'),
    env: { DSH_RS_BRIDGE_MOCK_DELAY_MS: '5' },
    requestTimeoutMs: 30000,
    shutdownTimeoutMs: 3000,
  }
  const bridge = new JsonlBridgeClient(ctx, processConfig)
  const factory = new BridgeAgentFactory(ctx, bridge, {
    ...processConfig,
    runtimeId: 'mock',
  })
  let handle

  try {
    handle = await factory.createAgent(ctx, {
      sessionId: SessionId('dsh-runtime-integration'),
      agentOptions: { runtimeId: 'mock' },
    })

    handle.agent.followup(createUserMessage({
      content: [{ type: 'text', text: 'first prompt' }],
      source: { kind: 'user' },
    }))
    await handle.agent.whenIdle()

    let events = handle.agent.session.snapshotEvents()
    let assistantMessages = events.filter(event => event.type === 'assistant/message')
    assert.equal(assistantMessages.length, 1)
    assert.equal(assistantMessages[0].data.message.content[0].text, 'mock runtime completed the task')
    assert.ok(events.some(event => event.type === 'turn/end' && event.data.reason.kind === 'completed'))

    handle.agent.followup(createUserMessage({
      content: [{ type: 'text', text: 'second prompt' }],
      source: { kind: 'user' },
    }))
    await handle.agent.whenIdle()

    events = handle.agent.session.snapshotEvents()
    assistantMessages = events.filter(event => event.type === 'assistant/message')
    assert.equal(assistantMessages.length, 2)
    assert.equal(assistantMessages[1].data.message.content[0].text, 'continued: second prompt')
    assert.equal(ctx.sessionProjections.stateOf(handle.agent.session, 'turnBoundary')?.lastTurn, 2)
    assert.equal(ctx.agents.get(handle.agent.id), handle.agent)
  } finally {
    if (handle !== undefined) await handle.dispose()
    await factory.disposeAll()
    await bridge.shutdown()
    unregisterInbox()
    unregisterTurnBoundary()
    await ctx.fiber.dispose()
  }
})

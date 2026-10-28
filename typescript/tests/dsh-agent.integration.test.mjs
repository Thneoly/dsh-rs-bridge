import assert from 'node:assert/strict'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { test } from 'node:test'
import { Context } from '@deepseek-ai/cordis'
import AgentRegistry from '@deepseek-ai/dsh-agent'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import * as JsonlSessionPersistence from '@deepseek-ai/dsh-session-persistence-jsonl'
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

  let unregisterTurnBoundary
  let unregisterInbox
  let bridge
  let factory
  const stateDir = mkdtempSync(join(tmpdir(), 'dsh-rs-bridge-ts-test-'))
  const processConfig = {
    command,
    args: ['--stdio'],
    cwd: resolve(process.cwd(), '..'),
    env: {
      DSH_RS_BRIDGE_MOCK_DELAY_MS: '5',
      DSH_RS_BRIDGE_MOCK_TOOL_EVENTS: '1',
      DSH_RS_BRIDGE_STATE_DIR: stateDir,
    },
    requestTimeoutMs: 30000,
    shutdownTimeoutMs: 3000,
  }
  const pluginFiber = ctx.plugin({
    name: 'dsh-runtime-bridge-integration-test',
    inject: ['agents', 'sessions', 'sessionProjections'],
    apply(pluginCtx) {
      unregisterTurnBoundary = pluginCtx.sessionProjections.register(turnBoundaryProjectionDefinition)
      unregisterInbox = pluginCtx.sessionProjections.register(inboxProjectionDefinition)
      bridge = new JsonlBridgeClient(pluginCtx, processConfig)
      factory = new BridgeAgentFactory(pluginCtx, bridge, {
        ...processConfig,
        runtimeId: 'mock',
      })
    },
  })
  await pluginFiber
  let handle
  const assistantFrames = []
  const runtimeToolEvents = []
  ctx.on('agent/assistant-stream', ({ agent, frame }) => {
    if (String(agent.id) === 'dsh-runtime-integration') assistantFrames.push(frame)
  })
  ctx.on('agent/runtime-event', ({ agent, event }) => {
    if (String(agent.id) === 'dsh-runtime-integration') runtimeToolEvents.push(event)
  })

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
    assert.ok(assistantMessages[0].data.stream.some(record => record.type === 'text-chunks'))
    assert.ok(assistantMessages[0].data.stream.some(record => record.type === 'chunk' && record.chunk.type === 'finish'))
    assert.ok(assistantFrames.some(frame => frame.type === 'start'))
    assert.ok(assistantFrames.some(frame => frame.type === 'chunk' && frame.chunk.type === 'text-delta' && frame.chunk.text === 'mock runtime completed the task'))
    assert.ok(assistantFrames.some(frame => frame.type === 'end' && frame.outcome.kind === 'committed'))
    assert.ok(runtimeToolEvents.some(event => event.event_type === 'tool.started'))
    assert.ok(runtimeToolEvents.some(event => event.event_type === 'tool.completed'))
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
    assert.equal(handle.agent.ctx.sessionProjections.stateOf(handle.agent.session, 'turnBoundary')?.lastTurn, 2)
    assert.equal(handle.agent.ctx.agents.get(handle.agent.id), handle.agent)
  } finally {
    if (handle !== undefined) await handle.dispose()
    await factory.disposeAll()
    await bridge.shutdown()
    rmSync(stateDir, { recursive: true, force: true })
    unregisterInbox()
    unregisterTurnBoundary()
    await pluginFiber.dispose()
    await ctx.fiber.dispose()
  }
})


test('DSH AgentFactory restores persisted ACP conversation after bridge restart', async () => {
  const command = process.env.DSH_RS_BRIDGE_BIN
  const acpAgent = process.env.DSH_RS_BRIDGE_ACP_TEST_BIN
  assert.ok(command, 'DSH_RS_BRIDGE_BIN must point to the built Rust bridge binary')
  assert.ok(acpAgent, 'DSH_RS_BRIDGE_ACP_TEST_BIN must point to the built ACP fixture agent')

  const persistenceRoot = mkdtempSync(join(tmpdir(), 'dsh-rs-bridge-session-log-'))
  const stateDir = mkdtempSync(join(tmpdir(), 'dsh-rs-bridge-state-restart-'))
  const ctx = new Context()
  await ctx.plugin(SessionProjectionRegistry)
  await ctx.plugin(AgentRegistry)
  await ctx.plugin(SessionStore)
  await ctx.plugin(JsonlSessionPersistence, {
    root: persistenceRoot,
    compression: 'none',
  })

  let unregisterTurnBoundary
  let unregisterInbox
  const processConfig = {
    command,
    args: ['--stdio'],
    cwd: resolve(process.cwd(), '..'),
    env: {
      DSH_RS_BRIDGE_MOCK_DELAY_MS: '5',
      DSH_RS_BRIDGE_STATE_DIR: stateDir,
      DSH_RS_BRIDGE_ACP_COMMAND: acpAgent,
      DSH_RS_BRIDGE_ACP_ARGS: '[]',
    },
    requestTimeoutMs: 30000,
    shutdownTimeoutMs: 3000,
  }
  let bridge
  let factory
  let testCtx
  const pluginFiber = ctx.plugin({
    name: 'dsh-runtime-bridge-resume-integration-test',
    inject: ['agents', 'sessions', 'sessionProjections', 'sessionPersistence'],
    apply(pluginCtx) {
      testCtx = pluginCtx
      unregisterTurnBoundary = pluginCtx.sessionProjections.register(turnBoundaryProjectionDefinition)
      unregisterInbox = pluginCtx.sessionProjections.register(inboxProjectionDefinition)
      bridge = new JsonlBridgeClient(pluginCtx, processConfig)
      factory = new BridgeAgentFactory(pluginCtx, bridge, {
        ...processConfig,
        runtimeId: 'acp',
      })
    },
  })
  await pluginFiber
  let firstHandle
  let resumedHandle
  const sessionId = SessionId('dsh-acp-resume-e2e')
  const cwd = resolve(process.cwd(), '..')

  try {
    firstHandle = await factory.createAgent(ctx, {
      sessionId,
      meta: { cwd },
      agentOptions: { runtimeId: 'acp' },
    })
    firstHandle.agent.followup(createUserMessage({
      content: [{ type: 'text', text: 'before restart' }],
      source: { kind: 'user' },
    }))
    await firstHandle.agent.whenIdle()

    let events = firstHandle.agent.session.snapshotEvents()
    let assistantMessages = events.filter(event => event.type === 'assistant/message')
    assert.equal(assistantMessages.length, 1)
    assert.equal(assistantMessages[0].data.message.content[0].text, 'ACP says: before restart')

    await firstHandle.dispose().catch(error => {
      console.error('[resume integration] dispose causes:', error?.errors ?? error)
      throw error
    })
    firstHandle = undefined
    await factory.disposeAll()
    factory = undefined
    await bridge.shutdown()
    bridge = undefined

    // A fresh bridge process loads the JSONL binding journal and the ACP
    // adapter reattaches to the fixture agent's existing external session ID.
    // Restart just the bridge process; retain the DI-owned plugin context.
    bridge = new JsonlBridgeClient(testCtx, processConfig)
    factory = new BridgeAgentFactory(testCtx, bridge, {
      ...processConfig,
      runtimeId: 'acp',
    })
    resumedHandle = await factory.resume(ctx, {
      resumeSessionId: sessionId,
      agentOptions: { runtimeId: 'acp' },
    })

    events = resumedHandle.agent.session.snapshotEvents()
    assistantMessages = events.filter(event => event.type === 'assistant/message')
    assert.equal(assistantMessages.length, 1, 'restored DSH history must be preserved')

    resumedHandle.agent.followup(createUserMessage({
      content: [{ type: 'text', text: 'after restart' }],
      source: { kind: 'user' },
    }))
    await resumedHandle.agent.whenIdle()

    events = resumedHandle.agent.session.snapshotEvents()
    assistantMessages = events.filter(event => event.type === 'assistant/message')
    assert.equal(assistantMessages.length, 2)
    assert.equal(assistantMessages[0].data.message.content[0].text, 'ACP says: before restart')
    assert.equal(assistantMessages[1].data.message.content[0].text, 'ACP says: after restart')
    assert.equal(
      resumedHandle.agent.ctx.sessionProjections.stateOf(resumedHandle.agent.session, 'turnBoundary')?.lastTurn,
      2,
    )
  } finally {
    if (firstHandle !== undefined) await firstHandle.dispose().catch(() => {})
    if (resumedHandle !== undefined) await resumedHandle.dispose().catch(() => {})
    await factory?.disposeAll().catch(() => {})
    await bridge?.shutdown().catch(() => {})
    unregisterInbox()
    unregisterTurnBoundary()
    await pluginFiber.dispose()
    await ctx.fiber.dispose()
    rmSync(persistenceRoot, { recursive: true, force: true })
    rmSync(stateDir, { recursive: true, force: true })
  }
})

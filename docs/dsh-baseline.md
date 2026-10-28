# DSH Baseline

The implementation is grounded in the following published pre-release source baseline:

- Repository: deepseek-ai/deepseek-harness
- Tag: dsh-v0.2.1-rc.3
- Commit: 1f81c047fc48fa190a683e469daa699857e9afe8

Relevant source files:

- packages/core/agent/src/index.ts — AgentRegistry and AgentFactory registration.
- packages/core/agent-loop/src/index.ts — AgentLoop factory and Native ReactLoopAgent construction.
- packages/core/agent/src/runtime-types.ts — Agent lifecycle contract.
- packages/core/session/src/types.ts — canonical DSH session event types.
- packages/subagent/subagent-acp/src/index.ts — existing ACP Subagent provider.

Important constraints:

1. This is a release candidate, not a stable release.
2. The current AgentLoop registers the single AgentFactory. A second competing factory is not the integration approach.
3. Existing ACP Subagent is task delegation; it is not yet a persistent top-level Runtime driver.
4. The Rust bridge's event names are its own protocol vocabulary. They must be translated to valid DSH session events by the TypeScript bridge.

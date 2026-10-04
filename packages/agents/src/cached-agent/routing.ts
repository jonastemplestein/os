// cached-agent/routing.ts — which processor an agent runs: the cached one (processor.ts) for every
// path since 2026-10-03, except a path listed here, which keeps the old loop (../agent/). The
// collection re-enables an agent's row with this answer whenever `itx.agents.create` is called on
// it (chief-of-staff.ts does on every input; every voice call and subagent is created), so a change
// here moves an agent at its next create, its log re-reduced without re-running any effect. Never
// move an agent by disabling and re-enabling its row: that deletes the facet and replays its whole
// history through the effect path.
export const OLD_AGENT_PATHS: readonly string[] = [];

/** The facet class an agent at `path` runs as. */
export function agentFacetClass(path: string): "AgentDurableObject" | "CachedAgentDurableObject" {
  return OLD_AGENT_PATHS.includes(path) ? "AgentDurableObject" : "CachedAgentDurableObject";
}

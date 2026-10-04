// cached-agent/ — the agent whose every request extends the previous one, so OpenAI's prompt cache
// holds the conversation (render.ts, processor.ts). Same events and API as ../ (agent/), so the
// Agents app, the relays and voice read it unchanged; which agents run it is routing.ts's table.
export { CachedAgentDurableObject } from "./durable-object.ts";
export { agentFacetClass } from "./routing.ts";

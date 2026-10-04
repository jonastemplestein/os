// agents/processor.ts — THE AGENT PROCESSOR: the pure reduce of the creation and deletion facts,
// the conversation and the loop's obligations, and the effects over that fold — THE SAGAS (the birth
// `itx.agents.create(path)` opens: the certificate on `/` and here with the default prompt beside it;
// the death `itx.agents.delete(path)` opens: the certificate on `/` and here, after which the loop
// runs no more turns) and THE LOOP (the turn loop, LLM request and codemode parts folded into one
// class, lean). The model call LIVES HERE (`#stream`): a `@cf/…` model through `itx.ai` (the
// Workers AI binding under THIS context's rules, so a test lends a fake there), anything else
// through the account's AI Gateway as a Workers AI partner model, streamed from the Responses API.
// The host (durable-object.ts) hands in `getItx`, so a unit test constructs the processor with
// `new` and reduces rows (processor.test.ts, in node); the saga and the loop are proven on the
// worker (apps/agents/e2e/agents.e2e.test.ts, a fake `itx.ai` lent by rule).
//
// A request is debounced by the at-head scheduling below: one window after its trigger,
// the failure backoff folded in, the delayed append being the intent.
//
// Two kinds of effect, chosen at the dispatch site: a PER-EVENT consequence — the assistant's
// output parsed into a script request, a script's settlement rendered into the next developer item
// — is BLOCKED (`blockProcessorWhile`): the event is delivered once, so losing the append would
// lose the consequence. A STATE-DERIVED consequence — the birth, recording the next request,
// running the open request, tripping a breaker — runs at head in the BACKGROUND: any later delivery
// over the same fold re-derives it, so an attempt lost to an eviction costs nothing, and every
// append is idempotency-keyed so a retry appends nothing twice.
import { z } from "zod";
import { bytesToBase64, errorCode } from "iterate/lib";
import {
  type ConsumedEvent,
  type EmittedEventInput,
  type ProcessEventArgs,
  type ReduceArgs,
  StreamProcessor,
} from "iterate/stream/processor";
import type { IterateContextApi, RewriteRuleListEntry } from "iterate/api";
import {
  AgentContract,
  type AgentState,
  type ChatMessage,
  type FileAttachment,
  type LlmUsage,
} from "./contract.ts";
import { parseCodemodeResponse } from "./codemode-format.ts";
import { renderScriptSettlement } from "./result-render.ts";
import {
  classifyScriptResult,
  envelopeLoader,
  guardPreamble,
  pinnedEntryThrew,
  pinnedEntryThrewNote,
  preambleEntryQuarantined,
  preambleEntrySet,
  preambleSection,
  RETAINED_SCRIPT_RESULTS,
  wrapScript,
} from "./results-preamble.ts";
import { trustBoundary } from "./message.ts";
import { standingFileSection, standingInstructionFiles } from "./standing-instructions.ts";
import { AGENT_COMPACTION_PROMPT, DEFAULT_AGENT_SYSTEM_PROMPT } from "./system-prompt.ts";

/** The secret the agent loop calls OpenAI with: the project's own key (see `#stream`). */
const OPENAI_SECRET = "/secrets/openai";
/** How hard the model thinks before answering: "medium", Jonas's default since 2026-10-03 (with
 *  gpt-6.1-sol; it was "low" with gpt-6-astra). Reasoning tokens are billed as output. */
const REASONING_EFFORT = "medium";
/** The zone each request's "Requested at" stamp also reads in: Jonas's. */
const LOCAL_TIME_ZONE = "Europe/London";

/** The failure backoff, folded into the debounce window: doubling from the policy's base per
 *  consecutive failure, capped at its ceiling; nothing after a success. */
function retryBackoffMs(state: Pick<AgentState, "consecutiveLlmFailures" | "config">): number {
  const { backoffBaseMs, backoffMaxMs } = state.config.llmRequestRetryPolicy;
  if (state.consecutiveLlmFailures <= 0) return 0;
  return Math.min(2 ** (state.consecutiveLlmFailures - 1) * backoffBaseMs, backoffMaxMs);
}

/** The conversation as the model reads it. An item another context appended opens with
 *  `[from <context>]` and how to answer it. An item's images become image parts (a data: URL of the
 *  bytes in `images`, keyed by path — a vision model sees the pixels); any other attachment, or an
 *  image whose bytes are gone, is a line naming it and how a script reads it (a hint line). The
 *  loop's own developer notes (a script's result, a correction, a request stamp) read as system
 *  instructions; a developer item anyone else wrote, and a compaction summary, read as the user's —
 *  data and memory, never elevated to instructions. */
export function buildChatMessages(
  items: AgentState["contextItems"],
  images: Map<string, { contentType: string; base64: string }>,
  tree: RewriteRuleListEntry[] = [],
  standing: string[] = [],
): ChatMessage[] {
  const messages = items.map((item): ChatMessage => {
    const trusted = !item.compaction && (!item.actor || item.actor.type !== "user");
    const role = item.role === "developer" ? (trusted ? "system" : "user") : item.role;
    const content = item.from ? `${fromLine(item.from)} ${item.content}` : item.content;
    if (!item.files?.length) return { role, content };
    const parts: Extract<ChatMessage["content"], unknown[]> = [];
    const hints: string[] = [];
    for (const file of item.files) {
      const image = images.get(file.path);
      if (image)
        parts.push({
          type: "image_url",
          image_url: { url: `data:${image.contentType};base64,${image.base64}` },
        });
      else hints.push(fileHintLine(file));
    }
    const text = [content, ...hints].filter(Boolean).join("\n");
    if (parts.length === 0) return { role, content: text };
    return { role, content: [{ type: "text", text }, ...parts] };
  });
  // THE STANDING INSTRUCTIONS (the project's AGENTS.md and the agent's own `<name>.md`, read fresh)
  // and THE TREE this turn — the sandbox's `rewriteRules.list()`, rendered — as system messages
  // after the journaled system items, before the conversation: least-changing first, so a
  // provider's prefix cache keeps them.
  const rendered = renderCapabilityTree(tree);
  const turnSystem = [...standing, ...(rendered ? [rendered] : [])];
  if (turnSystem.length > 0) {
    const firstNonSystem = messages.findIndex((message) => message.role !== "system");
    messages.splice(
      firstNonSystem === -1 ? messages.length : firstNonSystem,
      0,
      ...turnSystem.map((content): ChatMessage => ({ role: "system", content })),
    );
  }
  return messages;
}

/** Words from another agent: who, and how to answer (the sender's sandbox answers for its agent). */
function fromLine(from: string): string {
  const agent = from.replace(/\/sandbox$/, "");
  return `[from ${from} — it cannot see this conversation; answer by appending to its log: \`await itx.cd(${JSON.stringify(agent)}).append({ type: "events.iterate.com/agent/context-added", payload: { role: "user", content } })\`]`;
}

/** The request stamp a newly opened request adds to the conversation: the moment it was asked for,
 *  in UTC and in the local zone. It stays in place, so the prompt a provider cached stays a prefix. */
function requestStamp(createdAt: string): string {
  const local = new Intl.DateTimeFormat("en-GB", {
    timeZone: LOCAL_TIME_ZONE,
    dateStyle: "full",
    timeStyle: "short",
  }).format(new Date(createdAt));
  return `Requested at: ${createdAt} (${local} in ${LOCAL_TIME_ZONE})`;
}

/** The sandbox's table as the model reads it: one line per name it can spell (`match — description`,
 *  a row without a description shows its target), grouped by the context each row came from when
 *  more than one; masks and the bare `itx` row are not names. Null when nothing is spellable (a jail
 *  with no grants yet): then no tree message at all. */
export function renderCapabilityTree(rows: RewriteRuleListEntry[]): string | null {
  const visible = rows.filter((row) => row.target && row.match !== "itx");
  if (visible.length === 0) return null;
  const contexts = [...new Set(visible.map((row) => row.context))];
  const body = contexts.flatMap((context) => [
    `from ${context}:`,
    ...visible
      .filter((row) => row.context === context)
      .map((row) => `${row.match} — ${row.description || `⇒ ${row.target}`}`),
  ]);
  return [
    "`itx` IS THIS CONTEXT'S CAPABILITY TREE (`await itx.rewriteRules.list()`) — every name below is one you can spell inside a tag; nothing else resolves:",
    ...body,
  ].join("\n");
}

/** How a non-image (or gone) attachment is named to the model. */
function fileHintLine(file: FileAttachment): string {
  return `[Attached file: ${file.filename} (${file.contentType}, ${String(file.size)} bytes) — read it with \`await itx.files.get(${JSON.stringify(file.path)}).bytes()\`]`;
}

/** The coalescing window: how much streamed text rides one `llm-response-frame` append — ~7
 *  repaints a second, and one commit per window instead of per token. */
const FRAME_WINDOW_MS = 150;
/** A window whose text grew past this lands early rather than as one oversized append. */
const FRAME_WINDOW_MAX_CHARS = 64_000;
/** The idle watchdog: a stream that carries nothing for this long fails the attempt, so a stalled
 *  provider never wedges a turn until its expiry. Short, because a person may be waiting on the
 *  line: a low-effort reasoning model streams within seconds, and a stall is retried at once. */
const STREAM_IDLE_BUDGET_MS = 25_000;

/** The context windows of the models this loop names; a conservative floor for the rest. OpenAI's
 *  figures are the operating window (where pricing doubles), not the documented one. */
function contextWindowTokens(model: string): number {
  if (/^gpt-(6|5)/.test(model)) return 272_000;
  if (model.startsWith("@cf/meta/llama-4-scout")) return 131_072;
  return 128_000;
}

/** The abort reason an interruption carries, so the runner tells it from a clock. */
class InterruptedError extends Error {
  constructor() {
    super("interrupted by the person's next words");
    this.name = "InterruptedError";
  }
}

/** An append that may LOSE to an earlier one under the same idempotency key with a different
 *  body — the settle of a request an interruption already settled — and then appends nothing:
 *  the first settlement stands, the later one was never a fact. */
async function appendUnlessLost(
  append: AgentArgs["append"],
  ...events: AgentEmitted[]
): Promise<void> {
  try {
    await append(...events);
  } catch (error) {
    if (errorCode(error) !== "IDEMPOTENCY_CONFLICT") throw error;
  }
}

// ── the model call's wire shapes ──

/** What Workers AI answers when it does not stream: `{ response }`, or the chat-completions shape. */
const ChatAnswer = z.union([
  z.object({ response: z.string() }),
  z.object({ choices: z.array(z.object({ message: z.object({ content: z.string() }) })).min(1) }),
]);

/** The usage a provider reports, both dialects: OpenAI Responses
 *  (`input_tokens`/`output_tokens`) and chat completions (`prompt_tokens`/`completion_tokens`),
 *  with the cached/reasoning breakdowns when present. Loose: vendors keep adding fields. */
const ProviderUsage = z.looseObject({
  prompt_tokens: z.number().int().nonnegative().optional(),
  completion_tokens: z.number().int().nonnegative().optional(),
  input_tokens: z.number().int().nonnegative().optional(),
  output_tokens: z.number().int().nonnegative().optional(),
  prompt_tokens_details: z
    .looseObject({ cached_tokens: z.number().int().nonnegative().optional() })
    .optional(),
  completion_tokens_details: z
    .looseObject({ reasoning_tokens: z.number().int().nonnegative().optional() })
    .optional(),
  input_tokens_details: z
    .looseObject({ cached_tokens: z.number().int().nonnegative().optional() })
    .optional(),
  output_tokens_details: z
    .looseObject({ reasoning_tokens: z.number().int().nonnegative().optional() })
    .optional(),
});

function normalizeUsage(raw: unknown): LlmUsage | undefined {
  const parsed = ProviderUsage.safeParse(raw);
  if (!parsed.success) return undefined;
  const inputTokens = parsed.data.prompt_tokens ?? parsed.data.input_tokens;
  const outputTokens = parsed.data.completion_tokens ?? parsed.data.output_tokens;
  if (inputTokens === undefined || outputTokens === undefined) return undefined;
  const cachedInputTokens =
    parsed.data.prompt_tokens_details?.cached_tokens ??
    parsed.data.input_tokens_details?.cached_tokens;
  const reasoningOutputTokens =
    parsed.data.completion_tokens_details?.reasoning_tokens ??
    parsed.data.output_tokens_details?.reasoning_tokens;
  return { inputTokens, outputTokens, cachedInputTokens, reasoningOutputTokens };
}

/** One OpenAI Responses API stream event — the loop reads the few types it knows and skips the
 *  rest. */
const ResponsesEvent = z.looseObject({ type: z.string() });

/** Read an SSE body frame by frame, handing each `data:` JSON to `onEvent`; the reader is cancelled
 *  when `signal` aborts, so nothing lands after the caller has settled. */
async function drainSse(
  body: ReadableStream<Uint8Array>,
  signal: AbortSignal,
  onEvent: (event: unknown) => void,
): Promise<void> {
  const reader = body.getReader();
  let completed = false;
  const cancel = () => void reader.cancel().catch(() => undefined);
  if (signal.aborted) cancel();
  signal.addEventListener("abort", cancel, { once: true });
  const decoder = new TextDecoder();
  let buffered = "";
  const frame = (text: string) => {
    const data = text
      .split("\n")
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice("data:".length).trim())
      .join("\n");
    if (data === "" || data === "[DONE]") return;
    let event: unknown;
    try {
      event = JSON.parse(data);
    } catch {
      event = data;
    }
    onEvent(event);
  };
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffered += decoder.decode(value, { stream: true });
      const frames = buffered.split(/\r?\n\r?\n/);
      buffered = frames.pop() || "";
      frames.forEach(frame);
    }
    buffered += decoder.decode();
    if (buffered.trim()) frame(buffered);
    completed = true;
  } finally {
    signal.removeEventListener("abort", cancel);
    // A parser error (for example, a `response.failed` event) stops this consumer before the
    // provider has finished: cancel the body so the provider's stream stops too. Not awaited, so a
    // stalled provider cannot hold the error path.
    if (!completed) void reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
  if (signal.aborted) throw signal.reason instanceof Error ? signal.reason : new Error("aborted");
}

/** Race an un-abortable dial against the caller's signal: the caller regains control the moment it
 *  aborts (an interruption, the expiry, the idle watchdog). A Response or stream the orphaned dial
 *  answers after that is cancelled, so the provider stops and no unread body holds the edge's
 *  invocation open; a stream already open is cancelled by `drainSse` itself. */
export function raceAbort<T>(signal: AbortSignal, work: Promise<T>): Promise<T> {
  const cancelLateBody = () =>
    void work.then(
      (late) => {
        const body = late instanceof Response ? late.body : late;
        if (body instanceof ReadableStream) void body.cancel(signal.reason).catch(() => undefined);
      },
      () => undefined,
    );
  if (signal.aborted) {
    cancelLateBody();
    return Promise.reject(signal.reason || new Error("aborted"));
  }
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      reject(signal.reason || new Error("aborted"));
      cancelLateBody();
    };
    signal.addEventListener("abort", onAbort, { once: true });
    // The listener goes in the same turn the dial settles: an answer handed over is its reader's.
    work.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

/** The conversation as the Responses API takes it: `input` items with text and image parts. */
function responsesInput(messages: ChatMessage[]) {
  return messages.map((message) =>
    typeof message.content === "string"
      ? { role: message.role, content: message.content }
      : {
          role: message.role,
          content: message.content.map((part) =>
            part.type === "text"
              ? { type: "input_text", text: part.text }
              : { type: "input_image", image_url: part.image_url.url, detail: "auto" },
          ),
        },
  );
}

type AgentEvent = ConsumedEvent<typeof AgentContract>;
type AgentArgs = ProcessEventArgs<AgentState, AgentEvent, AgentEmitted>;
/** What the loop appends: each type the contract emits, its payload as the catalog spells it. */
type AgentEmitted = EmittedEventInput<typeof AgentContract>;

type AgentProcessorDeps = {
  /** The host's scope accessor: `itx.ai`, `itx.files`, `itx.whoami()` — the effects this loop
   *  reaches through the context, under its rules (a test lends a fake `itx.ai` there). */
  getItx: () => IterateContextApi & Disposable;
  /** The clock and the wait, injected only so a unit test can make the debounce instant. */
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
};

export class AgentProcessor extends StreamProcessor<AgentState, AgentEvent> {
  readonly contract = AgentContract;

  private readonly deps: AgentProcessorDeps;
  readonly #now: () => number;
  /** The debounce window's wait — a test makes it instant. */
  readonly #sleep: (ms: number) => Promise<void>;

  constructor(deps: AgentProcessorDeps) {
    super();
    this.deps = deps;
    this.#now = deps.now || (() => Date.now());
    this.#sleep = deps.sleep || ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  }

  /** This incarnation's birth attempt, so one at-head pass does not start a second; the durable
   *  ground is `state.creation`. */
  #creating = false;
  /** The same for this incarnation's death attempt; the durable ground is `state.deletion`. */
  #deleting = false;

  /** The requests THIS incarnation is running, so a later at-head pass over the same fold does
   *  not start a second attempt; the durable ground is the fold (`openRequest`). */
  readonly #llmRequestsInFlight = new Map<
    number,
    { controller: AbortController; partialText: string }
  >();

  #identityRead?: { projectId: string; path: string };
  /** Which context this is — its project and path — read once. */
  /** THE PROJECT'S STANDING INSTRUCTIONS, read fresh every turn from /repos/config: AGENTS.md, and
   *  the files of the agent's own role (standing-instructions.ts: `<name>.md` for an agent at
   *  `/agents/<name>`; on a voice call, the role file of the person rung and voice-call.md).
   *  A file that is missing, or unreadable this turn, is simply left out. */
  async #standingInstructions(path: string): Promise<string[]> {
    const out: string[] = [];
    for (const file of standingInstructionFiles(path)) {
      try {
        using itx = this.deps.getItx();
        const text = await itx.repos.get("/repos/config").readFile(file);
        if (text) out.push(standingFileSection(file, text));
      } catch {
        // left out this turn
      }
    }
    return out;
  }

  async #identity(): Promise<{ projectId: string; path: string }> {
    if (this.#identityRead) return this.#identityRead;
    using itx = this.deps.getItx();
    return (this.#identityRead = await itx.whoami());
  }

  /** A certificate on `/`, where the catalog folds it (catalog.ts): stamped with this agent's path,
   *  which is all the catalog trusts. Unkeyed there: a key on `/` is anyone's to take first, and a
   *  same-body event under it would swallow this one; the catalog's fold is idempotent, so a retry
   *  that lands it twice changes nothing. */
  async #postToTheCatalog({ type, payload }: AgentEmitted): Promise<unknown> {
    using itx = this.deps.getItx();
    return await itx.cd("/").append({ type, payload });
  }

  /** Every change the facts make is stamped with the event's time: `lastActivityAt` moves exactly
   *  when the state does, so a harmless fact (a late intent, a repeated certificate) never reorders
   *  the sidebar. */
  reduce(args: ReduceArgs<AgentState, AgentEvent>): AgentState | undefined {
    const next = this.#reduceFacts(args);
    return next && { ...next, lastActivityAt: args.event.createdAt };
  }

  #reduceFacts({ state, event }: ReduceArgs<AgentState, AgentEvent>): AgentState | undefined {
    switch (event.type) {
      case "events.iterate.com/agent/create-requested":
        // Born once: a request after the certificate is a harmless fact; after a failure, a new
        // attempt. A deleted agent is not re-creatable: `creation` stays as it was, so the request
        // is a harmless fact there too (the collection refuses it before it lands).
        return state.creation?.status === "created"
          ? undefined
          : {
              ...state,
              creation: { status: "requested", offset: event.offset },
            };
      case "events.iterate.com/agent/created":
        return { ...state, creation: { status: "created", offset: event.offset } };
      case "events.iterate.com/agent/create-failed":
        // A failure after the certificate is a harmless fact too (an attempt whose own-path append
        // lost its answer): the entity stays created, and the next create() answers at once.
        return state.creation?.status === "created"
          ? undefined
          : { ...state, creation: { status: "failed", offset: event.offset } };
      case "events.iterate.com/agent/delete-requested":
        // Dies once: a request after the death certificate is a harmless fact. Deletion never
        // touches `creation` — the log still says the agent was born.
        return state.deletion?.status === "deleted"
          ? undefined
          : { ...state, deletion: { status: "requested", offset: event.offset } };
      case "events.iterate.com/agent/deleted":
        return { ...state, deletion: { status: "deleted", offset: event.offset } };

      case "events.iterate.com/agent/configured": {
        const patch = event.payload.config;
        return {
          ...state,
          config: {
            llm: { model: patch.llm?.model || state.config.llm.model },
            maxAutonomousTurns: patch.maxAutonomousTurns ?? state.config.maxAutonomousTurns,
            llmRequestExpiryMs: patch.llmRequestExpiryMs ?? state.config.llmRequestExpiryMs,
            llmRequestDebounceMs: patch.llmRequestDebounceMs ?? state.config.llmRequestDebounceMs,
            scriptResultHistoryLimit:
              patch.scriptResultHistoryLimit ?? state.config.scriptResultHistoryLimit,
            compactionTriggerFraction:
              patch.compactionTriggerFraction ?? state.config.compactionTriggerFraction,
            llmRequestRetryPolicy: {
              maxAttempts:
                patch.llmRequestRetryPolicy?.maxAttempts ??
                state.config.llmRequestRetryPolicy.maxAttempts,
              backoffBaseMs:
                patch.llmRequestRetryPolicy?.backoffBaseMs ??
                state.config.llmRequestRetryPolicy.backoffBaseMs,
              backoffMaxMs:
                patch.llmRequestRetryPolicy?.backoffMaxMs ??
                state.config.llmRequestRetryPolicy.backoffMaxMs,
            },
          },
        };
      }

      case "events.iterate.com/agent/context-added": {
        // THE TRUST BOUNDARY (message.ts): what another agent's context appended is its words, a
        // user item from that context, whatever role, actor or compaction it claims
        const payload = trustBoundary(event);
        const { role, content, actor, llmRequestPolicy, llmRequestOffset, compaction } = payload;
        // A COMPACTION SUMMARY replaces the conversation through its barrier: the system items
        // survive (from either side of it), then the summary, then everything after the barrier
        // verbatim — what arrived while the summary was written included. A summary no newer than
        // one already applied is a harmless fact. It raises no turn.
        if (role === "developer" && compaction) {
          const cutoff = compaction.replacesHistoryThrough;
          const applied = state.contextItems.some(
            (item) => item.compaction && item.compaction.replacesHistoryThrough >= cutoff,
          );
          if (applied || cutoff >= event.offset) return undefined;
          return {
            ...state,
            contextItems: [
              ...state.contextItems.filter((item) => item.role === "system"),
              { offset: event.offset, role, content, actor, compaction },
              ...state.contextItems.filter(
                (item) => item.role !== "system" && item.offset > cutoff,
              ),
            ],
          };
        }
        // WHO SENT IT: another context's stamp (apps/os caller.ts `stampCaller`), else the sender
        // the collection relayed through this agent's own facet (`message`; its base is the
        // caller's to choose through the public `at(base)`, collection.ts). `/` is the people's
        // (the dash, a member's session, the root's collection): a person's words carry no sender.
        // The sender signs nothing, so the label is advisory.
        const { origin } = event.source;
        const sender = origin !== event.path ? origin : payload.from;
        const next: AgentState = {
          ...state,
          contextItems: [
            ...state.contextItems,
            {
              offset: event.offset,
              role,
              content,
              actor,
              llmRequestOffset,
              files: payload.files,
              from: sender === "/" ? undefined : sender,
            },
          ],
        };
        // A person's or a developer's words raise the trigger — the system prompt and the
        // assistant's own output never do, nor words whose policy says not to.
        const triggers =
          (role === "user" || role === "developer") &&
          llmRequestPolicy?.behaviour !== "dont-trigger-request";
        if (!triggers) return next;
        const source =
          actor?.type === "script" || actor?.type === "agent" ? "agent-loop" : "external";
        return {
          ...next,
          pendingLlmRequestTrigger: {
            offset: event.offset,
            atMs: Date.parse(event.createdAt),
            source,
          },
          // a person's words start the breakers over (a transient outage's backoff ends with them)
          // and answer whatever the agent was waiting for
          ...(source === "external" && {
            autonomousTurnCount: 0,
            consecutiveLlmFailures: 0,
            summary: { ...state.summary, waitingFor: null },
          }),
        };
      }

      case "events.iterate.com/agent/llm-request-requested": {
        // A late intent — its trigger answered, moved on, or a request already open — is a harmless
        // stream fact: only the intent naming THE pending trigger opens a request, so a sleep the
        // debounce left behind for a trigger that moved can neither skip the new trigger's window
        // nor a failure's backoff. The request's identity is the offset of the intent that opened it.
        const trigger = state.pendingLlmRequestTrigger;
        if (!trigger || state.openRequest || trigger.offset !== event.payload.triggerOffset)
          return undefined;
        return {
          ...state,
          // the request's stamp: the model's clock, at the request's own offset (processor
          // `#messages` reads items through it)
          contextItems: [
            ...state.contextItems,
            {
              offset: event.offset,
              role: "developer",
              content: requestStamp(event.createdAt),
              actor: { type: "agent" },
            },
          ],
          pendingLlmRequestTrigger: null,
          openRequest: {
            requestedAtOffset: event.offset,
            expiresAt: event.payload.expiresAt,
            model: event.payload.model,
            triggerSource: trigger.source,
          },
          autonomousTurnCount:
            trigger.source === "agent-loop"
              ? state.autonomousTurnCount + 1
              : state.autonomousTurnCount,
        };
      }

      case "events.iterate.com/agent/llm-request-settled": {
        const open = state.openRequest;
        if (!open || open.requestedAtOffset !== event.payload.requestOffset) return undefined;
        const { result } = event.payload;
        if (result.status === "succeeded")
          return { ...state, openRequest: null, consecutiveLlmFailures: 0 };
        if (result.status === "failed")
          // The trigger comes back for the retry, still the same source; the pass caps the retries.
          return {
            ...state,
            openRequest: null,
            consecutiveLlmFailures: state.consecutiveLlmFailures + 1,
            pendingLlmRequestTrigger: {
              offset: open.requestedAtOffset,
              atMs: Date.parse(event.createdAt),
              source: open.triggerSource,
            },
          };
        // Expired: the turn is dropped; the person's next words start fresh.
        return { ...state, openRequest: null };
      }

      case "events.iterate.com/agent/paused":
        // A pause DROPS the parked trigger: what tripped the breaker (a script's result, a retry)
        // must not be what resumes it. Only words that arrive after the pause raise a new one.
        return state.paused
          ? undefined
          : {
              ...state,
              paused: { reason: event.payload.reason, atOffset: event.offset },
              pendingLlmRequestTrigger: null,
            };

      case "events.iterate.com/agent/resumed":
        return state.paused
          ? { ...state, paused: null, autonomousTurnCount: 0, consecutiveLlmFailures: 0 }
          : undefined;

      // THIS LOOP'S OWN RUNS: a run request the engine stamped as this processor's, on this
      // context. A run anyone else asked for here is theirs: its settlement wakes no turn.
      case "events.iterate.com/itx/run-requested":
        if (
          event.source.processor?.slug !== this.contract.slug ||
          event.source.origin !== event.path
        )
          return undefined;
        return {
          ...state,
          pendingRuns: {
            ...state.pendingRuns,
            [String(event.offset)]: { requestedAt: Date.parse(event.createdAt) },
          },
        };

      // An own run's outcome joins `results` (the newest ten); the note itself is processEvent's.
      case "events.iterate.com/itx/run-settled": {
        const { requestOffset, settlement } = event.payload;
        const key = String(requestOffset);
        if (!state.pendingRuns[key]) return undefined;
        const { [key]: _settled, ...pendingRuns } = state.pendingRuns;
        const row = classifyScriptResult({
          agentPath: event.path,
          requestOffset,
          offset: event.offset,
          settlement,
        });
        return {
          ...state,
          pendingRuns,
          scriptResults: [...state.scriptResults, row].slice(-RETAINED_SCRIPT_RESULTS),
        };
      }

      // What the agent says about itself: each field it names replaces that field.
      case "events.iterate.com/agent/summary-updated": {
        const { activity, title, waitingFor, description } = event.payload;
        return {
          ...state,
          summary: {
            title: title ?? state.summary.title,
            activity: activity ?? state.summary.activity,
            waitingFor: waitingFor === undefined ? state.summary.waitingFor : waitingFor,
            description: description ?? state.summary.description,
          },
        };
      }

      // Pinned code (results-preamble.ts): set, replaced or removed by a script, and taken out by
      // the loop when it breaks scripts.
      case "events.iterate.com/agent/preamble-entry-set":
        return { ...state, ...preambleEntrySet(state, event.payload, event.offset) };

      case "events.iterate.com/agent/preamble-entry-quarantined":
        return { ...state, ...preambleEntryQuarantined(state, event.payload, event.offset) };

      default:
        return undefined;
    }
  }

  /** THE PINNED ENTRIES' GUARD before a run (results-preamble.ts `guardPreamble`): an entry the
   *  script envelope would not load with is quarantined just before the run request, which then
   *  carries the envelope without it, so a broken entry costs no failed run and never blocks the
   *  script that removes it. One load per run of an agent with pinned entries, warm while they stay
   *  the same; an agent without any pays nothing. */
  async #guardPinned(
    consequences: AgentEmitted[],
    state: AgentState,
    event: AgentEvent,
    code: string,
  ): Promise<AgentEmitted[]> {
    if (state.preamble.length === 0) return consequences;
    using itx = this.deps.getItx();
    const { keep, quarantined } = await guardPreamble(
      envelopeLoader(itx),
      event.path,
      state.preamble,
    );
    if (quarantined.length === 0) return consequences;
    return consequences.flatMap((consequence): AgentEmitted[] =>
      consequence.type === "events.iterate.com/itx/run-requested"
        ? [
            ...quarantined.map(
              (payload) =>
                ({
                  type: "events.iterate.com/agent/preamble-entry-quarantined",
                  idempotencyKey: this.idempotencyKey(`preamble-quarantined/${payload.key}`, event),
                  payload,
                }) satisfies AgentEmitted,
            ),
            {
              ...consequence,
              payload: {
                code: wrapScript({
                  code,
                  agentPath: event.path,
                  rows: state.scriptResults,
                  preamble: keep,
                }),
              },
            },
          ]
        : [consequence],
    );
  }

  processEvent(args: AgentArgs): undefined {
    const { event, state, previousState, append, blockProcessorWhile, runInBackground } = args;
    // An agent asked to go acts no more: no consequence of a late event (a script result landing
    // after the request raises no turn), no interrupt to settle — only the death itself, at head.
    if (state.deletion) {
      this.#atHead(args);
      return;
    }
    // THE INTERRUPT: cancellation is a property of new input, never a command. The
    // person's words abort whatever this incarnation is streaming, keep what streamed as an
    // assistant item the next turn can see (no llmRequestOffset: a record, never parsed for a
    // script), and settle the request cancelled — blocked, so an eviction can never leave the
    // request open for the next at-head pass to adopt. Their reduce already moved the trigger; the
    // settlement's own delivery re-runs the at-head pass, which then records the next request.
    if (
      event?.type === "events.iterate.com/agent/context-added" &&
      event.payload.llmRequestPolicy?.behaviour === "interrupt-current-request" &&
      (event.payload.role === "user" || event.payload.role === "developer") &&
      state.openRequest
    ) {
      const open = state.openRequest;
      const inFlight = this.#llmRequestsInFlight.get(open.requestedAtOffset);
      inFlight?.controller.abort(new InterruptedError());
      const partialText = inFlight?.partialText || undefined;
      blockProcessorWhile(() =>
        appendUnlessLost(
          append,
          ...(partialText
            ? [
                {
                  type: "events.iterate.com/agent/context-added",
                  idempotencyKey: this.idempotencyKey(
                    `interrupted/${String(open.requestedAtOffset)}`,
                  ),
                  payload: {
                    role: "assistant",
                    content: `[Response interrupted by the user's next message; partial output follows]\n${partialText}`,
                  },
                } satisfies AgentEmitted,
              ]
            : []),
          {
            type: "events.iterate.com/agent/llm-request-settled",
            idempotencyKey: this.idempotencyKey(`settle/${String(open.requestedAtOffset)}`),
            payload: {
              requestOffset: open.requestedAtOffset,
              result: { status: "cancelled", reason: "interrupted-by-user-input", partialText },
            },
          },
        ),
      );
      // Not at head this frame: the pass reads the pre-cancel fold and would adopt the very
      // request the queued settlement cancels.
      return;
    }

    // ── per-event consequences, blocked: the event is delivered once ──
    // The assistant's answer, interpreted (mmkal's order: the status precedes the script so the step
    // is born with its label, the script precedes the prose so a feed groups the turn as one).
    if (
      event?.type === "events.iterate.com/agent/context-added" &&
      event.payload.role === "assistant" &&
      event.payload.llmRequestOffset !== undefined
    ) {
      const { llmRequestOffset } = event.payload;
      const outcome = parseCodemodeResponse(event.payload.content);
      const consequences: AgentEmitted[] = [];
      if (outcome.kind === "malformed" || outcome.kind === "multiple")
        consequences.push({
          type: "events.iterate.com/agent/context-added",
          idempotencyKey: this.idempotencyKey("format-feedback", event),
          payload: { role: "developer", content: outcome.feedback, actor: { type: "agent" } },
        });
      if (outcome.kind === "script") {
        if (outcome.status)
          consequences.push({
            type: "events.iterate.com/agent/summary-updated",
            idempotencyKey: this.idempotencyKey("codemode-status", event),
            payload: { activity: outcome.status },
          });
        consequences.push({
          type: "events.iterate.com/itx/run-requested",
          idempotencyKey: this.idempotencyKey("run-requested", event),
          // the model's script in its envelope: `results`, pinned entries, the serializer
          payload: {
            code: wrapScript({
              code: outcome.code,
              agentPath: event.path,
              rows: state.scriptResults,
              preamble: state.preamble,
            }),
          },
        });
      }
      // The prose — beside a tag or on its own — is the message, appended directly on this context.
      // Where a reply GOES from here is a subscriber's business (events are the interface), never a
      // script the model's sandbox would need a row for.
      if ((outcome.kind === "script" || outcome.kind === "none") && outcome.prose)
        consequences.push({
          type: "events.iterate.com/agent/web-message-sent",
          idempotencyKey: this.idempotencyKey("codemode-prose", event),
          payload: {
            message: outcome.prose,
            llmRequestOffset,
            ...(outcome.kind === "script" && { besideScript: true }),
          },
        });
      // Words with no script end the turn on the person: the agent now waits for their answer.
      if (outcome.kind === "none" && outcome.prose)
        consequences.push({
          type: "events.iterate.com/agent/summary-updated",
          idempotencyKey: this.idempotencyKey("waiting-for", event),
          payload: { waitingFor: "user_input" },
        });
      // A pinned entry the script envelope would not load with is quarantined before the run,
      // which goes ahead without it (`#guardPinned`).
      if (consequences.length > 0)
        blockProcessorWhile(async () =>
          append(
            ...(outcome.kind === "script"
              ? await this.#guardPinned(consequences, state, event, outcome.code)
              : consequences),
          ),
        );
    }

    // THE CONTEXT ran one of this loop's scripts: its settlement is the model's next input — only
    // an own run's (the reduce knew it as pending), with how long it ran. A large result is written
    // to its file first, inside the block: the note must never land before the file it names.
    if (event?.type === "events.iterate.com/itx/run-settled") {
      const { requestOffset, settlement } = event.payload;
      const pending = previousState.pendingRuns[String(requestOffset)];
      const row = state.scriptResults.find((entry) => entry.offset === event.offset);
      if (pending && row)
        blockProcessorWhile(async () => {
          // A PINNED ENTRY THAT THREW before the script began is quarantined (results-preamble.ts):
          // it would throw before every later script too.
          const threw = pinnedEntryThrew(settlement, state.preamble, requestOffset);
          if (threw)
            await append({
              type: "events.iterate.com/agent/preamble-entry-quarantined",
              idempotencyKey: this.idempotencyKey("preamble-threw", event),
              payload: threw,
            });
          const rendered = await renderScriptSettlement({
            settlement,
            row,
            durationMs: Math.max(0, Date.parse(event.createdAt) - pending.requestedAt),
            historyLimit: state.config.scriptResultHistoryLimit,
            write: async (path, text) => {
              using itx = this.deps.getItx();
              await itx.files.get(path).put({
                contentType: row.text ? "text/plain; charset=utf-8" : "application/json",
                data: new TextEncoder().encode(text),
              });
            },
          });
          const content = rendered && threw ? pinnedEntryThrewNote(rendered, threw) : rendered;
          if (content)
            await append({
              type: "events.iterate.com/agent/context-added",
              idempotencyKey: this.idempotencyKey("script-result", event),
              payload: {
                role: "developer",
                content,
                actor: { type: "script", requestOffset },
              },
            });
        });
    }

    if (event?.type === "events.iterate.com/agent/llm-request-settled") {
      const { requestOffset, result } = event.payload;
      const open = previousState.openRequest;
      // A LOST TURN IS SAID: a failed attempt or an expired request becomes a note the next turn
      // reads, so the model can tell the person — raising no turn of its own (the retry is the
      // reduce's; `agent` actor, so it resets no breaker).
      const lost =
        result.status === "failed"
          ? `The model request @${String(requestOffset)} failed (attempt ${String(state.consecutiveLlmFailures)} of ${String(state.config.llmRequestRetryPolicy.maxAttempts)}): ${result.errorMessage.slice(0, 500)}. ${state.consecutiveLlmFailures < state.config.llmRequestRetryPolicy.maxAttempts ? "Retrying." : "Giving up until the next message."}`
          : result.status === "cancelled" && result.reason === "expired"
            ? `The model request @${String(requestOffset)} expired before it finished; that turn was dropped.`
            : null;
      if (lost && open?.requestedAtOffset === requestOffset)
        blockProcessorWhile(() =>
          append({
            type: "events.iterate.com/agent/context-added",
            idempotencyKey: this.idempotencyKey("lost-turn", event),
            payload: {
              role: "developer",
              content: lost,
              actor: { type: "agent" },
              llmRequestPolicy: { behaviour: "dont-trigger-request" },
            },
          }),
        );
      // COMPACTION: a request that filled its share of the window has the conversation through it
      // summarized, in the background — the turn's own consequences (its script) go on meanwhile,
      // and the summary replaces that history when it lands (the reduce). One at a time per
      // incarnation; a summary already covering this request is not asked for twice.
      if (
        result.status === "succeeded" &&
        result.usage &&
        open?.requestedAtOffset === requestOffset
      ) {
        const usedTokens = result.usage.inputTokens + result.usage.outputTokens;
        const thresholdTokens = Math.floor(
          contextWindowTokens(open.model) * state.config.compactionTriggerFraction,
        );
        const covered = state.contextItems.some(
          (item) => item.compaction && item.compaction.replacesHistoryThrough >= requestOffset,
        );
        const history = state.contextItems.some(
          (item) => item.role !== "system" && item.offset < requestOffset,
        );
        if (usedTokens >= thresholdTokens && history && !covered && !this.#compacting) {
          this.#compacting = true;
          runInBackground(async () => {
            try {
              await this.#compact({
                state,
                requestOffset,
                model: open.model,
                usedTokens,
                thresholdTokens,
                append,
              });
            } finally {
              this.#compacting = false;
            }
          });
        }
      }
    }

    this.#atHead(args);
  }

  // ── state-derived consequences, at head, in the background: re-derived by any later delivery ──
  #atHead({ state, delivery, append, runInBackground }: AgentArgs): void {
    if (!delivery.caughtUp) return;

    // THE SAGA — the birth, from state at head, in the background: at most once per incarnation,
    // and any later delivery over the same state runs it again, so an attempt lost to an eviction
    // costs nothing. Nothing to provision: the certificate goes to `/` (the project catalog) first,
    // then lands here in ONE append with the default system prompt beside it — keyed here, so a
    // retry appends nothing twice. An operator's instructions are their own `context-added` after.
    if (state.creation?.status === "requested") {
      if (this.#creating) return;
      this.#creating = true;
      runInBackground(async () => {
        try {
          let whoami;
          {
            using itx = this.deps.getItx();
            whoami = await itx.whoami();
          }
          const { path } = whoami;
          const certificate: AgentEmitted = {
            type: "events.iterate.com/agent/created",
            payload: { path },
            idempotencyKey: `agent/created:${path}`,
          };
          await this.#postToTheCatalog(certificate); // the project catalog first
          await append(certificate, {
            // this path last: the certificate closes the obligation, the prompt rides with it
            type: "events.iterate.com/agent/context-added",
            idempotencyKey: `agent/system-prompt:${path}`,
            payload: {
              role: "system",
              content: `${DEFAULT_AGENT_SYSTEM_PROMPT}\nCURRENT PROJECT: ${JSON.stringify(whoami)}`,
            },
          });
        } catch (error) {
          await append({
            type: "events.iterate.com/agent/create-failed",
            payload: { error: error instanceof Error ? error.message : String(error) },
          });
        } finally {
          this.#creating = false;
        }
      });
      return;
    }
    if (state.creation?.status !== "created") return;

    // THE DEATH — the birth's mirror, only of an agent that was born, and the LOOP's gate: a deleted
    // agent (or one asked to go) runs no more turns, whatever the fold below says. Nothing to tear
    // down, so the saga is the death certificate alone: `/` first (the catalog drops the entry), then
    // here — keyed, so a retry appends nothing twice. A throw appends nothing: the next at-head pass
    // is the retry, and there is no delete-failed fact.
    if (state.deletion) {
      if (state.deletion.status !== "requested" || this.#deleting) return;
      this.#deleting = true;
      runInBackground(async () => {
        try {
          const { path } = await this.#identity();
          const certificate: AgentEmitted = {
            type: "events.iterate.com/agent/deleted",
            payload: { path },
            idempotencyKey: `agent/deleted:${path}`,
          };
          await this.#postToTheCatalog(certificate); // the project catalog first
          await append(certificate); // this path last: closes the obligation
        } finally {
          this.#deleting = false;
        }
      });
      return;
    }
    const now = this.#now();

    // A person's words resume a paused loop; the loop's own never do (they are what paused it).
    const trigger = state.pendingLlmRequestTrigger;
    if (state.paused && trigger?.source === "external") {
      runInBackground(() =>
        append({
          type: "events.iterate.com/agent/resumed",
          idempotencyKey: this.idempotencyKey(`resume/${String(trigger.offset)}`),
          payload: { reason: "external input" },
        }),
      );
      return;
    }

    // A trigger and nothing open: record the request — or trip a breaker instead.
    if (trigger && !state.openRequest && !state.paused) {
      const { maxAutonomousTurns, llmRequestRetryPolicy, llmRequestExpiryMs, llm } = state.config;
      const breaker =
        trigger.source === "agent-loop" && state.autonomousTurnCount >= maxAutonomousTurns
          ? `autonomous turn limit reached (${String(maxAutonomousTurns)} consecutive turns without external input)`
          : state.consecutiveLlmFailures >= llmRequestRetryPolicy.maxAttempts
            ? `the model failed ${String(state.consecutiveLlmFailures)} times in a row`
            : null;
      if (breaker) {
        runInBackground(() =>
          append({
            type: "events.iterate.com/agent/paused",
            idempotencyKey: this.idempotencyKey(`pause/${String(trigger.offset)}`),
            payload: { reason: breaker, triggerOffset: trigger.offset },
          }),
        );
        return;
      }
      // THE DEBOUNCE: wait for more content, plus the failure backoff — one window,
      // anchored at the trigger. The delayed append IS the intent (no wake event): more words inside
      // the window move the trigger; the old trigger's intent then lands as a harmless fact (the
      // reduce opens a request only for the trigger it names) and the moved trigger's own intent, a
      // window later, opens the one request for them all — the prompt is built from the log at run
      // time. Every at-head pass inside the window schedules another
      // sleep-then-append for the same trigger, so the body is DETERMINISTIC from trigger + config
      // (expiresAt anchored at the trigger's time, never `now`): identical bodies dedupe on the key.
      // A droppable attempt: dying mid-window, the revival pass re-runs this with the window long
      // closed and appends at once.
      const windowMs = state.config.llmRequestDebounceMs + retryBackoffMs(state);
      const windowClosesInMs = trigger.atMs + windowMs - now;
      const intent: AgentEmitted = {
        type: "events.iterate.com/agent/llm-request-requested",
        idempotencyKey: this.idempotencyKey(`request/${String(trigger.offset)}`),
        payload: {
          model: llm.model,
          expiresAt: trigger.atMs + llmRequestExpiryMs,
          triggerOffset: trigger.offset,
        },
      };
      runInBackground(async () => {
        if (windowClosesInMs > 0) await this.#sleep(windowClosesInMs);
        await append(intent);
      });
      return;
    }

    // An open request nobody HERE is running, within its expiry: run it — the first time and after
    // an eviction are the same path (the engine's revive wakes a dead context while an attempt is
    // in flight; the wake's push lands here).
    const open = state.openRequest;
    // Past its expiry, a request is settled expired even when this incarnation believes it is
    // running it — its attempt is aborted: a request wedged in an await must not hold the loop.
    if (open && now >= open.expiresAt) {
      this.#llmRequestsInFlight
        .get(open.requestedAtOffset)
        ?.controller.abort(new Error("the model did not finish before the request expired"));
      runInBackground(() =>
        appendUnlessLost(append, {
          type: "events.iterate.com/agent/llm-request-settled",
          idempotencyKey: this.idempotencyKey(`settle/${String(open.requestedAtOffset)}`),
          payload: {
            requestOffset: open.requestedAtOffset,
            result: { status: "cancelled", reason: "expired" },
          },
        }),
      );
      return;
    }
    if (open && !this.#llmRequestsInFlight.has(open.requestedAtOffset)) {
      const inFlight = { controller: new AbortController(), partialText: "" };
      this.#llmRequestsInFlight.set(open.requestedAtOffset, inFlight);
      runInBackground(() => this.#runLlmRequest(open, state, append, inFlight));
    }
  }

  /** The model over the conversation up to the request, STREAMED: each coalescing window of the
   *  answer's text and thinking is one ephemeral `llm-response-frame` (a feed renders the answer as
   *  it is written); ONE batch then settles the request, lands the assistant's words and reports the
   *  cost, so an eviction between them is impossible. An interruption settles the request itself (processEvent) — an
   *  aborted stream ends here silently, and a success that raced it loses on the settle key. */
  async #runLlmRequest(
    open: NonNullable<AgentState["openRequest"]>,
    state: AgentState,
    append: AgentArgs["append"],
    inFlight: { controller: AbortController; partialText: string },
  ): Promise<void> {
    const startedAt = this.#now();
    const { controller } = inFlight;
    // Two clocks fail a stalled stream, never wedge it: the request's own expiry, and the idle
    // budget since the last provider event.
    const expiry = setTimeout(
      () => controller.abort(new Error("the model did not finish before the request expired")),
      Math.max(1_000, open.expiresAt - startedAt),
    );
    let idle = setTimeout(
      () => controller.abort(new Error("the model stream stalled")),
      STREAM_IDLE_BUDGET_MS,
    );
    try {
      const { path } = await raceAbort(controller.signal, this.#identity());
      // raced, so the expiry frees a request stuck before its stream (a hung read) too
      const messages = await raceAbort(
        controller.signal,
        this.#messages(state, open.requestedAtOffset),
      );
      // THE WINDOWS: the text and thinking the stream adds pile into one buffer; a window closes
      // FRAME_WINDOW_MS after its first delta (or at the size cap) and lands as one ephemeral
      // append, windows in order — each waits for the one before. Nothing is stored: the
      // settlement below carries the durable text.
      const llmRequestOffset = open.requestedAtOffset;
      let responseDelta = "";
      let thinkingDelta = "";
      let windowOpen = false;
      let sequence = 0;
      let windows = Promise.resolve();
      const closeWindow = () => {
        windowOpen = false;
        if (!responseDelta && !thinkingDelta) return;
        const payload = { llmRequestOffset, responseDelta, thinkingDelta, sequence: sequence++ };
        responseDelta = "";
        thinkingDelta = "";
        windows = windows
          .then(() =>
            append({
              type: "events.iterate.com/agent/llm-response-frame",
              ephemeral: true,
              payload,
            }),
          )
          .then(
            () => undefined,
            () => undefined, // a lost window loses only its repaint; the settlement is the truth
          );
      };
      const settle = async (
        result: Extract<
          AgentEvent,
          { type: "events.iterate.com/agent/llm-request-settled" }
        >["payload"]["result"],
        ...alongside: AgentEmitted[]
      ) => {
        closeWindow();
        await windows; // every window before the terminal fact
        await appendUnlessLost(
          append,
          {
            type: "events.iterate.com/agent/llm-request-settled",
            idempotencyKey: this.idempotencyKey(`settle/${String(llmRequestOffset)}`),
            payload: {
              requestOffset: llmRequestOffset,
              durationMs: this.#now() - startedAt,
              result,
            },
          },
          ...alongside,
        );
      };
      let answer: { text: string; usage?: LlmUsage };
      try {
        answer = await this.#stream({
          model: open.model,
          messages,
          cacheKey: path,
          signal: controller.signal,
          onDelta: (text, thinking) => {
            if (controller.signal.aborted) return;
            clearTimeout(idle);
            idle = setTimeout(
              () => controller.abort(new Error("the model stream stalled")),
              STREAM_IDLE_BUDGET_MS,
            );
            if (!text && !thinking) return; // a bookkeeping event: alive, nothing to show
            // The partial accrues BEFORE buffering: an interrupt keeps the whole streamed text even
            // when its last window never landed.
            inFlight.partialText += text;
            responseDelta += text;
            thinkingDelta += thinking;
            if (responseDelta.length + thinkingDelta.length >= FRAME_WINDOW_MAX_CHARS)
              return closeWindow();
            if (windowOpen) return;
            windowOpen = true;
            void this.#sleep(FRAME_WINDOW_MS).then(closeWindow);
          },
        });
      } catch (error) {
        // The interrupt path's story — it settled the request itself.
        if (controller.signal.reason instanceof InterruptedError) return;
        await settle({
          status: "failed",
          errorMessage: String(error instanceof Error ? error.message : error).slice(0, 4_000),
          partialText: inFlight.partialText || undefined,
        });
        return;
      }
      // An answer that arrived after the interruption is the interrupt path's story too.
      if (controller.signal.reason instanceof InterruptedError) return;
      const { text, usage } = answer;
      await settle(
        { status: "succeeded", text, usage },
        // an empty answer adds no assistant message: nothing was said
        ...(text
          ? [
              {
                type: "events.iterate.com/agent/context-added",
                idempotencyKey: this.idempotencyKey(`assistant/${String(llmRequestOffset)}`),
                payload: { role: "assistant", content: text, llmRequestOffset },
              } satisfies AgentEmitted,
            ]
          : []),
        ...(usage
          ? [
              {
                type: "events.iterate.com/agent/token-usage-reported",
                idempotencyKey: this.idempotencyKey(`usage/${String(llmRequestOffset)}`),
                payload: {
                  model: open.model,
                  maxContextTokens: contextWindowTokens(open.model),
                  inputTokens: usage.inputTokens,
                  outputTokens: usage.outputTokens,
                },
              } satisfies AgentEmitted,
            ]
          : []),
      );
    } finally {
      clearTimeout(expiry);
      clearTimeout(idle);
      this.#llmRequestsInFlight.delete(open.requestedAtOffset);
    }
  }

  /** THE PROMPT of the request at `throughOffset` (the items up to it, its own stamp included):
   *  the system prompt as the code has it now (the birth's journaled copy is left out — the prompt
   *  is code, like AGENTS.md, read fresh), the journaled system items, the standing instructions,
   *  the pinned preamble and the capability tree, then the conversation. A turn and the compaction
   *  of that turn build the same messages, so the summary request is a cached prefix. */
  async #messages(state: AgentState, throughOffset: number): Promise<ChatMessage[]> {
    const whoami = await this.#identity();
    const { path } = whoami;
    let tree: RewriteRuleListEntry[] = [];
    try {
      using itx = this.deps.getItx();
      tree = await itx.cd(`${path}/sandbox`).rewriteRules.list();
    } catch (error) {
      // A fully masked sandbox deliberately denies introspection too. Give the model no
      // advertised tools; prose replies still work. Transport/runtime failures remain errors.
      if (errorCode(error) !== "NO_ITX_EXPRESSION_MATCH") throw error;
    }
    // the birth lands the certificate and its prompt in one append: the prompt is the next offset
    const birthPromptOffset = state.creation ? state.creation.offset + 1 : 0;
    const items = state.contextItems.filter(
      (item) =>
        item.offset <= throughOffset &&
        !(item.role === "system" && item.offset === birthPromptOffset),
    );
    // The images the model will see: read now, the freshest bytes at the request; one that is
    // gone (deleted meanwhile) is named instead of shown.
    const images = new Map<string, { contentType: string; base64: string }>();
    for (const item of items)
      for (const file of item.files || []) {
        if (!file.contentType.startsWith("image/") || images.has(file.path)) continue;
        try {
          using itx = this.deps.getItx();
          images.set(file.path, {
            contentType: file.contentType,
            base64: bytesToBase64(await itx.files.get(file.path).bytes()),
          });
        } catch {
          // named by its hint line instead
        }
      }
    const pinned = preambleSection(state.preamble, state.preambleQuarantined);
    const preamble = pinned ? [pinned] : [];
    return [
      {
        role: "system",
        content: `${DEFAULT_AGENT_SYSTEM_PROMPT}\nCURRENT PROJECT: ${JSON.stringify(whoami)}`,
      },
      ...buildChatMessages(items, images, tree, [
        ...(await this.#standingInstructions(path)),
        ...preamble,
      ]),
    ];
  }

  /** This incarnation's compaction, so a second over-threshold request waits for the first. */
  #compacting = false;

  /** THE COMPACTION (the legacy platform's, #1834 and #2421): the request's own prompt, the
   *  summarize instruction appended as its last message — a prefix the provider has cached from
   *  the turn itself — on the request's own model; the summary lands as one developer item whose
   *  `compaction` barrier the reduce applies. Best effort: a failure appends nothing, and the next
   *  request over the threshold tries again. */
  async #compact(input: {
    state: AgentState;
    requestOffset: number;
    model: string;
    usedTokens: number;
    thresholdTokens: number;
    append: AgentArgs["append"];
  }): Promise<void> {
    const { state, requestOffset, model, usedTokens, thresholdTokens, append } = input;
    try {
      const { path } = await this.#identity();
      const messages: ChatMessage[] = [
        ...(await this.#messages(state, requestOffset)),
        { role: "system", content: AGENT_COMPACTION_PROMPT },
      ];
      const { text } = await this.#stream({
        model,
        messages,
        cacheKey: path,
        signal: AbortSignal.timeout(state.config.llmRequestExpiryMs),
        onDelta: () => undefined,
      });
      if (!text) throw new Error("the model wrote an empty summary");
      await appendUnlessLost(append, {
        type: "events.iterate.com/agent/context-added",
        idempotencyKey: this.idempotencyKey(`compact/${String(requestOffset)}`),
        payload: {
          role: "developer",
          content: `[Earlier conversation history was compacted through @${String(requestOffset)} (~${String(usedTokens)} tokens > ${String(thresholdTokens)}). Summary:]\n\n${text}`,
          actor: { type: "agent" },
          compaction: { replacesHistoryThrough: requestOffset },
          llmRequestPolicy: { behaviour: "dont-trigger-request" },
        },
      });
    } catch (error) {
      console.error("[agent] compaction failed", { error, requestOffset });
    }
  }

  /** One STREAMED model call over the conversation so far: every provider event the stream carries
   *  reaches `onDelta` as it arrives, with the answer text and the thinking it adds (both "" for a
   *  bookkeeping event); the call answers the whole text once the stream ends, with the usage the
   *  provider reported. Aborting `signal` stops the stream; the call then rejects.
   *
   *  Two routes by the model's name, both `itx.ai` under THIS context's rules (a test lends a fake
   *  there), each drained inside its one `getItx` scope: the call stays open until its body is
   *  read. A `@cf/…` answer may be streamed or whole JSON.
   *  Anything else is OpenAI's Responses API as a Workers AI partner model on Cloudflare's billing
   *  — no key, ours or a project's — the FAST reading of a reasoning model: low effort, with its
   *  summary streamed. */
  async #stream({
    model,
    messages,
    cacheKey,
    signal,
    onDelta,
  }: {
    model: string;
    messages: ChatMessage[];
    /** The provider's prompt-cache routing key: this agent's path, so its turns reach the shard
     *  that holds its prefix. */
    cacheKey: string;
    signal: AbortSignal;
    onDelta(text: string, thinking: string): void;
  }): Promise<{ text: string; usage?: LlmUsage }> {
    if (model.startsWith("@cf/")) {
      using itx = this.deps.getItx();
      // workers-types keys `run`'s inputs and outputs by model-name literal; the model is
      // configuration here (any name the account can reach), so the call is made through the
      // binding's runtime shape and the answer is validated below rather than trusted from a type.
      const ai = itx.ai as unknown as { run(model: string, inputs: unknown): Promise<unknown> };
      const raw: unknown = await raceAbort(signal, ai.run(model, { messages, stream: true }));
      if (raw instanceof ReadableStream) {
        let text = "";
        let usage: LlmUsage | undefined;
        await drainSse(raw, signal, (event) => {
          const chunk = z.looseObject({ response: z.string().optional() }).safeParse(event);
          const delta = chunk.success ? chunk.data.response || "" : "";
          text += delta;
          onDelta(delta, "");
          const reported = z.looseObject({ usage: z.unknown() }).safeParse(event);
          if (reported.success && reported.data.usage !== undefined)
            usage = normalizeUsage(reported.data.usage) ?? usage;
        });
        if (text.trim() === "") throw new Error("the model answered with no text");
        return { text: text.trim(), usage };
      }
      // A binding (or a lent fake) that answered whole: the one delta there is.
      const answer = ChatAnswer.parse(raw);
      const text = (
        "response" in answer ? answer.response : answer.choices[0]!.message.content
      ).trim();
      if (text === "") throw new Error("the model answered with no text");
      onDelta(text, "");
      return { text };
    }
    // No key of ours rides this request: an `openai/…` model is a Workers AI PARTNER model, billed
    // by Cloudflare through the binding — the Responses API shape, streamed, the raw Response asked
    // for so the SSE body is ours to read. The gateway option routes it through the account's AI
    // Gateway; its metadata (project, stream path, context) is what the gateway's spend limits
    // partition on, so a runaway agent hits ITS ceiling. Nothing is trusted from the answer: it is a
    // Response checked for status and parsed event by event below.
    // THIS PROJECT'S OWN KEY (Jonas's userspace agent): the Responses API straight at OpenAI, billed
    // to the key in /secrets/openai (pinned to api.openai.com) rather than through the deployment's
    // shared AI Gateway budget. The key never enters this code: egress swaps the placeholder.
    using itx = this.deps.getItx();
    const response = await raceAbort(
      signal,
      itx.fetch(
        new Request("https://api.openai.com/v1/responses", {
          method: "POST",
          headers: {
            authorization: `Bearer getSecret("${OPENAI_SECRET}")`,
            "content-type": "application/json",
          },
          body: JSON.stringify({
            model,
            input: responsesInput(messages),
            stream: true,
            store: false,
            prompt_cache_key: cacheKey.slice(0, 64),
            reasoning: { effort: REASONING_EFFORT, summary: "auto" },
          }),
        }),
      ),
    );
    if (!response.ok || !response.body)
      throw new Error(
        `openai/${model} ${String(response.status)}: ${(await response.text()).slice(0, 400)}`,
      );
    let text = "";
    let usage: LlmUsage | undefined;
    await drainSse(response.body, signal, (raw) => {
      const event = ResponsesEvent.safeParse(raw);
      if (!event.success) return;
      const { type } = event.data;
      if (type === "response.output_text.delta") {
        const delta = typeof event.data.delta === "string" ? event.data.delta : "";
        text += delta;
        onDelta(delta, "");
      } else if (type === "response.reasoning_summary_text.delta")
        onDelta("", typeof event.data.delta === "string" ? event.data.delta : "");
      else if (type === "response.completed" || type === "response.incomplete") {
        const done = z
          .looseObject({ response: z.looseObject({ usage: z.unknown() }) })
          .safeParse(raw);
        if (done.success) usage = normalizeUsage(done.data.response.usage) ?? usage;
      } else if (type === "response.failed" || type === "error") {
        const failure = z
          .looseObject({
            error: z.looseObject({ message: z.string() }).optional(),
            response: z
              .looseObject({ error: z.looseObject({ message: z.string() }).optional() })
              .optional(),
          })
          .safeParse(raw);
        throw new Error(
          `openai: ${failure.success ? failure.data.error?.message || failure.data.response?.error?.message || type : type}`,
        );
      }
    });
    // An empty answer is the model choosing to say nothing (a chief of staff's input that needs no
    // reply): the turn ends quietly, never a failure that would retry and pause the agent.
    return { text: text.trim(), usage };
  }
}

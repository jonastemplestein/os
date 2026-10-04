// replay-build.test.ts — THE REPLAY EXPERIMENT'S REQUESTS: for each recorded model request of an
// agent in a window, the input the OLD loop (../../processor.ts) and the CACHED loop (../processor.ts)
// would each have sent at that moment, from the same recorded events, folded through each loop's own
// reduce and rendered by its own code. Not a test: run with REPLAY_BUILD=<dir> and
// REPLAY_CONFIG=<a clone of the project's config repo>, with the windows to rebuild in
// <dir>/windows.json (`[{ name, agent, from, to }]`), and it writes <dir>/bodies/<window>.jsonl, one
// request body a line (both arms, in order), for replay-run.mjs.
//
// What each request is rebuilt from:
// - the conversation: the agent's recorded events through the request (raw/<agent>.jsonl);
// - the standing files (AGENTS.md, the role file) and the old system prompt as they were at the
//   request's time: the config repo's history (`git show <commit>:<file>`);
// - the capability tree as it was: the agent's live rows today, with the itx.config row naming the
//   commit published then, and the lent devices (WhatsApp, the speakers) present or gone and
//   ordered as the project's rule log says (traces/root-rules-all.json);
// - a segment marker at the head where the agent sat idle over 30 minutes (OpenAI's cache TTL):
//   both arms start that segment cold, as production did.
import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { test } from "vitest";
import type { RewriteRuleListEntry } from "iterate/api";
import { AgentProcessor, buildChatMessages } from "../../processor.ts";
import { AgentContract } from "../../contract.ts";
import { CachedAgentProcessor } from "../processor.ts";
import { CachedAgentContract, type CachedAgentState } from "../contract.ts";
import {
  buildResponsesInput,
  RUN_TOOL,
  sectionChanges,
  stableCapabilityTree,
  type RenderItem,
} from "../render.ts";
import { CACHED_AGENT_SYSTEM_PROMPT } from "../system-prompt.ts";
import { standingInstructionFiles } from "../../standing-instructions.ts";

const DIR = process.env.REPLAY_BUILD;
/** A clone of the project's config repo, whose history holds the standing files as they were. */
const CONFIG = process.env.REPLAY_CONFIG ?? "";
const MODEL = "gpt-6-astra";
/** A fresh run id keeps one run's cache from serving another's. */
const RUN = process.env.REPLAY_RUN ?? "run1";
const TTL_MS = 30 * 60_000;
const PUBLICATION_LAG_MS = 3_000;

type Raw = {
  offset: number;
  type: string;
  createdAt: string;
  payload: unknown;
  source?: Record<string, unknown>;
  path: string;
};
type Window = { name: string; agent: string; from: string; to: string };

// ── the config repo at a moment ──
let commitsRead: { sha: string; at: number }[] | undefined;
/** The config repo's commits, oldest first: read on first use, so a run without REPLAY_BUILD
 *  touches no repo. */
function configCommits(): { sha: string; at: number }[] {
  return (commitsRead ??= execFileSync(
    "git",
    ["-C", CONFIG, "log", "--format=%H %aI", "--since=2026-09-28"],
    { encoding: "utf8" },
  )
    .trim()
    .split("\n")
    .map((line) => {
      const [sha, at] = line.split(" ");
      return { sha: sha!, at: Date.parse(at!) };
    })
    .sort((a, b) => a.at - b.at));
}
const commitAt = (ms: number) => [...configCommits()].reverse().find((commit) => commit.at <= ms)!;
const fileCache = new Map<string, string | null>();
function fileAt(sha: string, file: string): string | null {
  const key = `${sha}:${file}`;
  if (!fileCache.has(key)) {
    try {
      fileCache.set(
        key,
        execFileSync("git", ["-C", CONFIG, "show", key], {
          encoding: "utf8",
          stdio: ["ignore", "pipe", "ignore"],
        }),
      );
    } catch {
      fileCache.set(key, null);
    }
  }
  return fileCache.get(key)!;
}
const promptCache = new Map<string, string>();
async function oldPromptAt(ms: number): Promise<string> {
  const sha = execFileSync(
    "git",
    [
      "-C",
      CONFIG,
      "log",
      "-1",
      `--before=${new Date(ms).toISOString()}`,
      "--format=%H",
      "--",
      "agent/system-prompt.ts",
    ],
    { encoding: "utf8" },
  ).trim();
  if (!promptCache.has(sha)) {
    const file = join(tmpdir(), `system-prompt-${sha}.ts`);
    writeFileSync(file, fileAt(sha, "agent/system-prompt.ts")!);
    promptCache.set(
      sha,
      (await import(/* @vite-ignore */ file)).DEFAULT_AGENT_SYSTEM_PROMPT as string,
    );
  }
  return promptCache.get(sha)!;
}

// ── the capability tree at a moment ──
const LENT = [
  "itx.whatsapp",
  "itx.whatsappPersonal",
  "itx.whatsappCalls",
  "itx.clients.satellite1",
  "itx.clients.zectrix_note4",
];
function treeAt(
  live: RewriteRuleListEntry[],
  ruleLog: { at: string; match: string; removed: boolean }[],
  ms: number,
): RewriteRuleListEntry[] {
  // a root rule's place in the list: its first registration, or its re-registration after a removal
  const place = new Map<string, { since: number; present: boolean }>();
  for (const rule of ruleLog) {
    const at = Date.parse(rule.at);
    if (at > ms) break;
    const now = place.get(rule.match);
    if (rule.removed) place.set(rule.match, { since: now?.since ?? at, present: false });
    else if (!now || !now.present) place.set(rule.match, { since: at, present: true });
  }
  const published = commitAt(ms - PUBLICATION_LAG_MS);
  const generation = 5000 + configCommits().indexOf(published);
  const rootRules = live
    .filter((row) => row.context === "/" && place.has(row.match))
    .filter((row) => place.get(row.match)!.present || !LENT.includes(row.match))
    .sort((a, b) => place.get(a.match)!.since - place.get(b.match)!.since)
    .map((row) =>
      row.match === "itx.config"
        ? {
            ...row,
            description: `the project's published config: /repos/config at ${published.sha.slice(0, 12)}, publication ${String(generation)}`,
          }
        : row,
    );
  const builtins = live.filter((row) => row.context === "/" && !place.has(row.match));
  const own = live.filter((row) => row.context !== "/");
  return [...own, ...rootRules, ...builtins];
}

/** The old loop's `responsesInput` (../../processor.ts, unexported): messages as Responses items. */
function oldResponsesInput(messages: ReturnType<typeof buildChatMessages>) {
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

/** One event as a processor's reduce reads it: the payload parsed (defaults applied), the rest as recorded. */
function asEvent(contract: typeof AgentContract | typeof CachedAgentContract, raw: Raw) {
  const schema = contract.payloadSchemaFor(raw.type);
  if (!schema) return null;
  const parsed = schema.safeParse(raw.payload ?? {});
  if (!parsed.success) return null;
  return {
    ...raw,
    payload: parsed.data,
    source: { ...raw.source, origin: (raw.source?.origin as string) || raw.path },
  } as never;
}

test.skipIf(!DIR)(
  "build the replay requests",
  async () => {
    const traces = `${process.env.HOME}/../..${DIR}`.replace(/^.*?\/private/, "/private");
    void traces;
    const root = DIR!;
    const live = JSON.parse(readFileSync(`${root}/trees/live.json`, "utf8")) as Record<
      string,
      RewriteRuleListEntry[]
    >;
    const ruleLog = (
      JSON.parse(readFileSync(`${root}/root-rules-all.json`, "utf8")) as {
        at: string;
        match: string;
        removed: boolean;
      }[]
    ).sort((a, b) => Date.parse(a.at) - Date.parse(b.at));
    const requests = readFileSync(`${root}/requests.csv`, "utf8").trim().split("\n");
    const header = requests[0]!.split(",");
    const csvRows = requests
      .slice(1)
      .map((line) => Object.fromEntries(header.map((key, i) => [key, line.split(",")[i]])));
    const windows = JSON.parse(readFileSync(`${root}/windows.json`, "utf8")) as Window[];
    mkdirSync(`${root}/bodies`, { recursive: true });

    for (const window of windows) {
      const file = `${root}/raw/${window.agent.replace(/^\//, "").replaceAll("/", "__")}.jsonl`;
      const raws = readFileSync(file, "utf8")
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line) as Raw);
      const whoami = (() => {
        const birth = raws.find(
          (raw) =>
            raw.type.endsWith("agent/context-added") &&
            (raw.payload as { role?: string }).role === "system",
        );
        const line = ((birth?.payload as { content?: string })?.content ?? "")
          .split("\n")
          .find((l) => l.startsWith("CURRENT PROJECT: "));
        return line
          ? line.slice("CURRENT PROJECT: ".length)
          : JSON.stringify({ path: window.agent });
      })();
      const settled = new Map<number, Record<string, unknown>>();
      for (const raw of raws)
        if (raw.type.endsWith("llm-request-settled"))
          settled.set(
            (raw.payload as { requestOffset: number }).requestOffset,
            raw.payload as Record<string, unknown>,
          );
      const oldProcessor = new AgentProcessor({
        getItx: () => {
          throw new Error("no itx");
        },
      });
      const newProcessor = new CachedAgentProcessor({
        getItx: () => {
          throw new Error("no itx");
        },
      });
      let oldState = AgentContract.initialState();
      let newState = CachedAgentContract.initialState() as CachedAgentState;
      const lines: string[] = [];
      let segment = 0;
      let lastSettledMs: number | null = null;
      let seq = 0;
      const tree = live[window.agent]!;
      for (const raw of raws) {
        const oldEvent = asEvent(AgentContract, raw);
        if (oldEvent)
          oldState = oldProcessor.reduce({ event: oldEvent, state: oldState }) ?? oldState;
        const newEvent = asEvent(CachedAgentContract, raw);
        if (newEvent)
          newState =
            (newProcessor.reduce({ event: newEvent, state: newState }) as
              | CachedAgentState
              | undefined) ?? newState;
        if (!raw.type.endsWith("llm-request-requested")) continue;
        const result = settled.get(raw.offset);
        const at = Date.parse(raw.createdAt);
        const inWindow = raw.createdAt >= window.from && raw.createdAt < window.to;
        if (!inWindow || !result || (result.result as { status: string }).status !== "succeeded") {
          if (result) lastSettledMs = at;
          continue;
        }
        if (lastSettledMs === null || at - lastSettledMs > TTL_MS) segment++;
        const usage = (result.result as { usage?: Record<string, number> }).usage ?? {};
        const commit = commitAt(at);
        const standing = standingInstructionFiles(window.agent).flatMap((name) => {
          const text = fileAt(commit.sha, name);
          return text ? [`${name} (/repos/config), as it is now:\n\n${text}`] : [];
        });
        const rows = treeAt(tree, ruleLog, at);
        const marker = (arm: string) => ({
          role: "developer",
          content: `[replay ${RUN} ${window.name} ${arm} segment ${String(segment)}]`,
        });

        // THE OLD LOOP: its #messages — the prompt, the journaled system items, the standing files, the
        // preamble and the tree, then the conversation, script results as system messages
        const birthPromptOffset = oldState.creation ? oldState.creation.offset + 1 : 0;
        const oldItems = oldState.contextItems.filter(
          (item) =>
            item.offset <= raw.offset &&
            !(item.role === "system" && item.offset === birthPromptOffset),
        );
        const preamble = oldState.preamble.length
          ? [
              `PINNED PREAMBLE — this code runs above every script you write, so its names are in scope (\`setPreamble({ key, code: null })\` removes an entry):\n\n${oldState.preamble.map((entry) => `// ${entry.key}\n${entry.code}`).join("\n\n")}`,
            ]
          : [];
        const oldMessages = [
          {
            role: "system" as const,
            content: `${await oldPromptAt(at)}\nCURRENT PROJECT: ${whoami}`,
          },
          ...buildChatMessages(oldItems, new Map(), rows, [...standing, ...preamble]),
        ];
        const oldInput = [marker("old"), ...oldResponsesInput(oldMessages)];

        // THE CACHED LOOP: the sections synced before the request, then its rendering
        const current: Record<string, string> = { system: CACHED_AGENT_SYSTEM_PROMPT };
        standingInstructionFiles(window.agent).forEach((name, index) => {
          if (standing[index]) current[name] = standing[index]!;
        });
        if (newState.preamble.length) current.preamble = preamble[0]!;
        const stable = stableCapabilityTree(rows);
        if (stable) current["capability-tree"] = stable;
        current.identity = `CURRENT PROJECT: ${whoami}`;
        const first = Object.keys(newState.sections).length === 0;
        const change = first
          ? { sections: current, content: "[standing instructions: the head snapshot]" }
          : sectionChanges(newState.sections, current);
        if (change) {
          const injected = asEvent(CachedAgentContract, {
            offset: raw.offset + 0.5,
            type: "events.iterate.com/agent/context-added",
            createdAt: raw.createdAt,
            path: window.agent,
            source: { origin: window.agent },
            payload: {
              role: "system",
              content: change.content,
              sections: change.sections,
              llmRequestOffset: raw.offset,
              ...(first && { snapshot: true }),
            },
          });
          newState =
            (newProcessor.reduce({ event: injected!, state: newState }) as
              | CachedAgentState
              | undefined) ?? newState;
        }
        const newItems = newState.contextItems.filter(
          (item) =>
            item.offset <= raw.offset ||
            (item.sections !== undefined && item.llmRequestOffset === raw.offset),
        ) as RenderItem[];
        const newInput = [
          marker("new"),
          ...buildResponsesInput({
            items: newItems,
            images: new Map(),
            runs: newState.runs,
            model: MODEL,
            ownPath: window.agent,
            birthPromptOffset: newState.creation ? newState.creation.offset + 1 : undefined,
          }),
        ];

        const key = (arm: string) =>
          `replay/${createHash("sha256")
            .update(`${RUN}/${window.name}/${arm}/${String(segment)}`)
            .digest("hex")
            .slice(0, 24)}`;
        // 128 output tokens: a response that spends its whole budget on reasoning reports zero usage
        const base = {
          model: MODEL,
          store: false,
          max_output_tokens: 128,
          reasoning: { effort: "low" },
        };
        const prod = {
          input: usage.inputTokens ?? 0,
          cached: usage.cachedInputTokens ?? 0,
          output: usage.outputTokens ?? 0,
          gapS: lastSettledMs === null ? null : Math.round((at - lastSettledMs) / 1000),
        };
        seq++;
        lines.push(
          JSON.stringify({
            window: window.name,
            arm: "old",
            seq,
            requestOffset: raw.offset,
            segment,
            prod,
            body: { ...base, input: oldInput, prompt_cache_key: key("old") },
          }),
        );
        lines.push(
          JSON.stringify({
            window: window.name,
            arm: "new",
            seq,
            requestOffset: raw.offset,
            segment,
            prod,
            body: {
              ...base,
              input: newInput,
              tools: [RUN_TOOL],
              tool_choice: "auto",
              parallel_tool_calls: false,
              prompt_cache_key: key("new"),
            },
          }),
        );
        lastSettledMs = at;
      }
      writeFileSync(`${root}/bodies/${window.name}.jsonl`, `${lines.join("\n")}\n`);
      console.log(
        window.name,
        lines.length / 2,
        "requests",
        Math.max(...lines.map((l) => JSON.parse(l).segment)),
        "segments",
        csvRows.length,
      );
    }
  },
  600_000,
);

// chief-of-staff-relay.test.ts — what the chief of staff's relay sends its person for the agent's
// events, and when their chat shows "typing…". The relay runs bare: the agent's side is a log this file
// commits events to, delivered as the engine delivers them (what the contract consumes, payloads
// parsed by its schemas); WhatsApp, email, the typing indicator and the clock are fakes.
import { consumesEvent } from "iterate/stream/processor";
import { expect, test } from "vitest";
import {
  ChiefOfStaffRelay,
  attachmentsOf,
  mediaContent,
  readRelayTarget,
  whatsappContent,
  whatsappMessagesOf,
} from "./chief-of-staff-relay.ts";

const SCRIPT = '<codemode status="Checking the trolley">\nreturn 1\n</codemode>';

type Step =
  /** Words that start a turn (unless `quiet`), as chief-of-staff.ts writes them. */
  | { input: string; quiet?: true }
  /** The agent opened a model request. */
  | { request: string }
  /** That request settled with this text; with `message`, the agent's answer was published. */
  | { settles: string; text: string; besideScript?: true }
  | { status: string }
  /** A running script sent the person words (`sendMessage`). */
  | { scriptSends: string }
  | { agentPaused: string }
  /** The script of the last request settled with no result. */
  | { scriptReturnsNothing: true }
  /** Time passes, and every wait due by then ends. */
  | { elapseMs: number };

test.for<{
  name: string;
  steps: Step[];
  down?: ("whatsapp" | "email")[];
  whatsapp?: string[];
  email?: string[];
  typing?: boolean[];
  failed?: number;
}>([
  {
    name: "the agent's answer goes to WhatsApp, the old 🤖 mark stripped; typing shows from the input until the answer",
    steps: [
      { input: "[pebble · 2026-10-01T10:00:00.000Z] Alex said into his ring:\nBuy milk" },
      { request: "r1" },
      { settles: "r1", text: "🤖 Noted: buy milk." }, // the old mark, still written by habit: stripped
      { elapseMs: 4_000 }, // the typing run sees the turn is over at its next refresh
    ],
    whatsapp: ["Noted: buy milk."],
    typing: [true, false],
  },
  {
    name: "a script step keeps the turn open: prose beside the script is not sent, the final answer is",
    steps: [
      { input: "[whatsapp · Alex] What is in my trolley?" },
      { request: "r1" },
      { settles: "r1", text: `On it.\n${SCRIPT}`, besideScript: true },
      { elapseMs: 4_000 },
      { request: "r2" },
      { settles: "r2", text: "Two things: milk and eggs." },
      { elapseMs: 4_000 },
    ],
    whatsapp: ["Two things: milk and eggs."],
    // on from the input through the script step, off only after the final answer
    typing: [true, false],
  },
  {
    name: "other people's chatter shows no typing, and an empty answer sends nothing",
    steps: [
      { input: "[whatsapp · group 100000000000000001@g.us] Robin: lol" },
      { request: "r1" },
      { settles: "r1", text: "" },
    ],
    whatsapp: [],
    typing: [],
  },
  {
    name: "a Monzo transaction shows no typing, and the silence that usually answers it sends nothing",
    steps: [
      {
        input:
          "[monzo · joint-account · 2026-10-01T13:29:58Z · tx_1] £12.50 out to Corner Shop (Anytown, GBR) · mastercard · groceries",
      },
      { request: "r1" },
      { settles: "r1", text: "" },
    ],
    whatsapp: [],
    typing: [],
  },
  {
    name: "a suspicious Monzo transaction's warning is sent like any answer",
    steps: [
      {
        input:
          "[monzo · personal · 2026-10-01T03:12:00Z · tx_2] £84.33 out to XYZ Games (online) · DECLINED (CARD_BLOCKED) · mastercard",
      },
      { request: "r1" },
      {
        settles: "r1",
        text: "A declined £84.33 payment to XYZ Games at 4am. Was this one of you?",
      },
    ],
    whatsapp: ["A declined £84.33 payment to XYZ Games at 4am. Was this one of you?"],
    typing: [],
  },
  {
    name: "a reaction to the agent's own message starts a turn but shows no typing; its silence sends nothing",
    steps: [
      { input: '[family reaction] Sam reacted 👍 to your message #570: "May we spend up to $1?"' },
      { request: "r1" },
      { settles: "r1", text: "" },
    ],
    whatsapp: [],
    typing: [],
  },
  {
    name: "context that starts no turn is owed nothing",
    steps: [{ input: "[gmail sent · …] Alex → Robin: See you then", quiet: true }],
    whatsapp: [],
    typing: [],
  },
  {
    name: "a script that returns nothing ends the turn: typing stops with no answer",
    steps: [
      { input: "[pebble · …] Alex said into his ring:\nArchive that newsletter" },
      { request: "r1" },
      { settles: "r1", text: SCRIPT },
      { scriptReturnsNothing: true },
      { elapseMs: 4_000 },
    ],
    whatsapp: [],
    typing: [true, false],
  },
  {
    name: "words a script sends go out at once",
    steps: [
      { input: "[pebble · …] Alex said into his ring:\nReorder last week's shop" },
      { request: "r1" },
      { settles: "r1", text: SCRIPT },
      { scriptSends: "On it, this takes a minute." },
    ],
    whatsapp: ["On it, this takes a minute."],
    typing: [true],
  },
  {
    name: "on a long turn a new status is sent as progress after 45 s without news, not before",
    steps: [
      { input: "[pebble · …] Alex said into his ring:\nPlan the trip" },
      { request: "r1" },
      { settles: "r1", text: SCRIPT },
      { status: "Checking the trolley" },
      { elapseMs: 45_000 },
      { status: "Comparing hotels" },
      { status: "Reading reviews" },
    ],
    whatsapp: ["… Comparing hotels"],
  },
  {
    name: "an agent that pauses says so and stops typing",
    steps: [
      { input: "[pebble · …] Alex said into his ring:\nPlan the trip" },
      { request: "r1" },
      { agentPaused: "the model failed 3 times in a row" },
      { elapseMs: 4_000 },
    ],
    whatsapp: [
      "I got stuck and stopped: the model failed 3 times in a row. Send me any message to start me again.",
    ],
    typing: [true, false],
  },
  {
    name: "with WhatsApp down the answer comes by email",
    down: ["whatsapp"],
    steps: [
      { input: "[pebble · …] Alex said into his ring:\nBuy milk" },
      { request: "r1" },
      { settles: "r1", text: "Noted: buy milk." },
    ],
    whatsapp: [],
    email: ["Noted: buy milk."],
  },
  {
    name: "with both down the failure is recorded",
    down: ["whatsapp", "email"],
    steps: [
      { input: "[pebble · …] Alex said into his ring:\nBuy milk" },
      { request: "r1" },
      { settles: "r1", text: "Noted: buy milk." },
    ],
    whatsapp: [],
    email: [],
    failed: 1,
  },
])("$name", async ({ steps, down, whatsapp, email, typing, failed }) => {
  const relay = bareRelay(down);
  for (const step of steps) await relay.take(step);
  const outcome = relay.outcome();
  if (whatsapp) expect(outcome.whatsapp).toEqual(whatsapp);
  if (email) expect(outcome.email).toEqual(email);
  if (typing) expect(outcome.typing).toEqual(typing);
  expect(outcome.failed).toBe(failed ?? 0);
});

test("the target is kv's: with none, the lookup throws an error that names the key", async () => {
  const kv = new Map<string, string>();
  const store = { get: async (key: string) => kv.get(key) ?? null };
  await expect(readRelayTarget(store, "/agents/chief-of-staff")).rejects.toThrow(
    "no relay target in kv whatsapp-relay/agents/chief-of-staff",
  );
  const target = { chats: ["447700900001@s.whatsapp.net"], email: "alex@example.com" };
  kv.set("whatsapp-relay/agents/chief-of-staff", JSON.stringify(target));
  expect(await readRelayTarget(store, "/agents/chief-of-staff")).toEqual(target);
});

test("with no relay target nothing is sent, and the failure is recorded with that one cause", async () => {
  const relay = bareRelay(["whatsapp", "email"], "no relay target in kv whatsapp-relay/agents/x");
  await relay.take({ input: "[pebble · …] Alex said into his ring:\nBuy milk" });
  await relay.take({ request: "r1" });
  await relay.take({ settles: "r1", text: "Noted: buy milk." });
  expect(relay.outcome()).toMatchObject({
    whatsapp: [],
    email: [],
    failures: ["no relay target in kv whatsapp-relay/agents/x"],
  });
});

test("history is folded, never sent: an answer older than ten minutes reaches nobody", async () => {
  const relay = bareRelay();
  await relay.take({ input: "[pebble · …] Alex said into his ring:\nBuy milk" });
  await relay.take({ request: "r1" });
  // the relay meets the answer eleven minutes late (enabled on a log with history, or replaying)
  relay.lag(11 * 60_000);
  await relay.take({ settles: "r1", text: "Noted: buy milk." });
  expect(relay.outcome().whatsapp).toEqual([]);
});

test("the record of a relayed answer names the files it attached", async () => {
  const relay = bareRelay();
  await relay.take({ input: "[whatsapp · Alex] The care guides?" });
  await relay.take({ request: "r1" });
  await relay.take({
    settles: "r1",
    text: "Here you are, sir.\n\n![Care guide](/integrations/gmail/18f0/Care Guide 2024.pdf)",
  });
  expect(relay.outcome().relayed).toEqual([
    expect.objectContaining({
      attached: ["/integrations/gmail/18f0/Care Guide 2024.pdf"],
    }),
  ]);
});

test("a later input keeps the turn owed when an earlier request ends", async () => {
  const relay = bareRelay();
  await relay.take({ input: "[whatsapp · Alex] First" });
  await relay.take({ request: "r1" });
  await relay.take({ input: "[whatsapp · Alex] Second" });
  // r1 was opened before the second input: its answer does not end what is owed
  await relay.take({ settles: "r1", text: "Answer to the first." });
  expect(relay.state().owedSinceOffset).not.toBeNull();
  await relay.take({ request: "r2" });
  await relay.take({ settles: "r2", text: "Answer to the second." });
  expect(relay.state().owedSinceOffset).toBeNull();
  expect(relay.outcome().whatsapp).toEqual(["Answer to the first.", "Answer to the second."]);
});

test.for<{ name: string; text: string; chat: string; mentions?: string[] }>([
  {
    name: "a phone number written @447700900002 in a group message is a mention",
    text: "@447700900002 a gentle reminder to water the plants.",
    chat: "100000000000000001@g.us",
    mentions: ["447700900002@s.whatsapp.net"],
  },
  {
    name: "two numbers, one of them twice, are two mentions",
    text: "For @447700900002 and @447700900001: bins tonight. @447700900002 has the key.",
    chat: "100000000000000001@g.us",
    mentions: ["447700900002@s.whatsapp.net", "447700900001@s.whatsapp.net"],
  },
  {
    name: "a name, an email address and a short number are not mentions",
    text: "@Sam wrote from sam@447700900002.example about room @12.",
    chat: "100000000000000001@g.us",
  },
  {
    name: "a chat with one person has no mentions",
    text: "@447700900002 is Sam's number.",
    chat: "447700900001@s.whatsapp.net",
  },
])("$name", ({ text, chat, mentions }) => {
  expect(whatsappContent(text, chat)).toEqual(mentions ? { text, mentions } : { text });
});

test.for<{
  name: string;
  answer: string;
  files: { path: string; caption: string }[];
  text: string;
}>([
  {
    name: "an answer with no attachment is all words",
    answer: "Two things: milk and eggs.",
    files: [],
    text: "Two things: milk and eggs.",
  },
  {
    name: "Markdown's image form naming a project file is an attachment, taken out of the words",
    answer: "Here is the chest, sir.\n\n![The pine chest](/integrations/whatsapp/AAA1/photo.jpg)",
    files: [{ path: "/integrations/whatsapp/AAA1/photo.jpg", caption: "The pine chest" }],
    text: "Here is the chest, sir.",
  },
  {
    name: "several files, and an answer that is only a file",
    answer: "![](/browser/screenshots/1.png)\n![the letter](/files/letter.pdf)",
    files: [
      { path: "/browser/screenshots/1.png", caption: "" },
      { path: "/files/letter.pdf", caption: "the letter" },
    ],
    text: "",
  },
  {
    // mail attachments keep their file names, spaces and all
    name: "file names with spaces, angled and percent-encoded paths",
    answer:
      "Here you are, sir.\n\n![Cleaning guide](/integrations/gmail/18f0000000000001/Cleaning and Maintenance Guide 2024 R1.pdf)\n\n![Care guide](</integrations/gmail/18f0000000000001/Care Guide 2024.pdf>)\n\n![Warranty](/integrations/gmail/18f0000000000001/Warranty%20Terms.pdf)",
    files: [
      {
        path: "/integrations/gmail/18f0000000000001/Cleaning and Maintenance Guide 2024 R1.pdf",
        caption: "Cleaning guide",
      },
      {
        path: "/integrations/gmail/18f0000000000001/Care Guide 2024.pdf",
        caption: "Care guide",
      },
      { path: "/integrations/gmail/18f0000000000001/Warranty Terms.pdf", caption: "Warranty" },
    ],
    text: "Here you are, sir.",
  },
  {
    name: "an image on the web and an ordinary link stay in the words",
    answer: "See ![logo](https://example.com/logo.png) and [the page](/not-a-file).",
    files: [],
    text: "See ![logo](https://example.com/logo.png) and [the page](/not-a-file).",
  },
])("$name", ({ answer, files, text }) => {
  expect(attachmentsOf(answer)).toEqual({ files, text });
});

test.for<{ file: string; contentType: string; content: Record<string, unknown> }>([
  {
    file: "x.jpg",
    contentType: "image/jpeg",
    content: { image: { url: "https://f/x" }, caption: "Look" },
  },
  {
    file: "x.mp4",
    contentType: "video/mp4",
    content: { video: { url: "https://f/x" }, caption: "Look" },
  },
  {
    // Ogg Opus is a voice note: the type WhatsApp's own carry, and no caption
    file: "x.ogg",
    contentType: "audio/ogg",
    content: { audio: { url: "https://f/x" }, mimetype: "audio/ogg; codecs=opus", ptt: true },
  },
  {
    file: "x.mp3",
    contentType: "audio/mpeg",
    content: { audio: { url: "https://f/x" }, mimetype: "audio/mpeg" },
  },
  {
    file: "x.pdf",
    contentType: "application/pdf",
    content: {
      document: { url: "https://f/x" },
      mimetype: "application/pdf",
      fileName: "x.pdf",
      caption: "Look",
    },
  },
  {
    // what WhatsApp does not play goes as a document: an SVG, a WebM
    file: "x.svg",
    contentType: "image/svg+xml",
    content: {
      document: { url: "https://f/x" },
      mimetype: "image/svg+xml",
      fileName: "x.svg",
      caption: "Look",
    },
  },
  {
    file: "x.webm",
    contentType: "video/webm",
    content: {
      document: { url: "https://f/x" },
      mimetype: "video/webm",
      fileName: "x.webm",
      caption: "Look",
    },
  },
])(
  "a $contentType file goes as its own kind of WhatsApp message",
  ({ file, contentType, content }) => {
    expect(mediaContent(contentType, "https://f/x", file, "Look")).toEqual(content);
  },
);

const GROUP = "100000000000000001@g.us";
const attached = (path: string, contentType: string | null, caption = "") => ({
  path,
  caption,
  found: contentType ? { contentType, url: `https://f${path}` } : null,
});

test.for<{
  name: string;
  words: string;
  files: ReturnType<typeof attached>[];
  messages: Record<string, unknown>[];
}>([
  {
    name: "one picture and a few words are one message, the words its caption",
    words: "Here is the chest, sir.",
    files: [attached("/images/chest.jpg", "image/jpeg", "The chest")],
    messages: [
      { image: { url: "https://f/images/chest.jpg" }, caption: "Here is the chest, sir." },
    ],
  },
  {
    name: "a voice note takes no caption: its words go first, as a message of their own",
    words: "I attached it; here it is again.",
    files: [attached("/files/voice-note.ogg", "audio/ogg", "A voice note")],
    messages: [
      { text: "I attached it; here it is again." },
      {
        audio: { url: "https://f/files/voice-note.ogg" },
        mimetype: "audio/ogg; codecs=opus",
        ptt: true,
      },
    ],
  },
  {
    name: "an answer that is only a voice note is only a voice note",
    words: "",
    files: [attached("/files/voice-note.ogg", "audio/ogg", "A voice note")],
    messages: [
      {
        audio: { url: "https://f/files/voice-note.ogg" },
        mimetype: "audio/ogg; codecs=opus",
        ptt: true,
      },
    ],
  },
  {
    name: "a picture alone keeps the caption written for it",
    words: "",
    files: [attached("/images/chest.jpg", "image/jpeg", "The chest")],
    messages: [{ image: { url: "https://f/images/chest.jpg" }, caption: "The chest" }],
  },
  {
    name: "several files: the words first, then each file with its own caption",
    words: "Two for you.",
    files: [attached("/a.mp4", "video/mp4", "The clip"), attached("/b.pdf", "application/pdf")],
    messages: [
      { text: "Two for you." },
      { video: { url: "https://f/a.mp4" }, caption: "The clip" },
      { document: { url: "https://f/b.pdf" }, mimetype: "application/pdf", fileName: "b.pdf" },
    ],
  },
  {
    name: "words too long for a caption go first, as a message of their own",
    words: "x".repeat(1_001),
    files: [attached("/images/chest.jpg", "image/jpeg", "The chest")],
    messages: [
      { text: "x".repeat(1_001) },
      { image: { url: "https://f/images/chest.jpg" }, caption: "The chest" },
    ],
  },
  {
    name: "a file that does not exist is said, not dropped",
    words: "Here you are.",
    files: [attached("/nope.jpg", null)],
    messages: [
      { text: "Here you are.\n\n(I meant to attach /nope.jpg, but there is no such file.)" },
    ],
  },
  {
    name: "a mention in a caption is a mention",
    words: "@447700900002 look at this",
    files: [attached("/images/chest.jpg", "image/jpeg")],
    messages: [
      {
        image: { url: "https://f/images/chest.jpg" },
        caption: "@447700900002 look at this",
        mentions: ["447700900002@s.whatsapp.net"],
      },
    ],
  },
])("$name", ({ words, files, messages }) => {
  expect(whatsappMessagesOf(words, files, GROUP)).toEqual(messages);
});

type LoggedEvent = {
  type: string;
  payload: Record<string, unknown>;
  offset: number;
  createdAt: string;
  path: string;
  source: { origin: string };
};

/** `cause`: the error both channels that are down throw (their own "<name> is down" without it). */
function bareRelay(down: ("whatsapp" | "email")[] = [], cause?: string) {
  const log: LoggedEvent[] = [];
  const requests = new Map<string, number>();
  const sent = { whatsapp: [] as string[], email: [] as string[], typing: [] as boolean[] };
  const sleeps: { until: number; resolve(): void }[] = [];
  let nowMs = Date.parse("2026-10-01T10:00:00.000Z");
  let lagMs = 0;
  const channel = (name: "whatsapp" | "email") => async (text: string) => {
    if (down.includes(name)) throw new Error(cause ?? `${name} is down`);
    sent[name].push(text);
  };
  const processor = new ChiefOfStaffRelay({
    now: () => nowMs + lagMs,
    sleep: (ms) => new Promise<void>((resolve) => sleeps.push({ until: nowMs + ms, resolve })),
    sendWhatsApp: channel("whatsapp"),
    sendEmail: channel("email"),
    setTyping: async (typing) => {
      sent.typing.push(typing);
    },
  });
  type Consumed = Parameters<typeof processor.reduce>[0]["event"];
  let state = processor.contract.initialState();
  const run = (work: () => Promise<unknown>) => void work();
  function commit(type: string, payload: Record<string, unknown> = {}): LoggedEvent {
    const event: LoggedEvent = {
      type,
      payload,
      offset: log.length + 1,
      createdAt: new Date(nowMs).toISOString(),
      path: "/agents/chief-of-staff",
      source: { origin: "/agents/chief-of-staff" },
    };
    log.push(event);
    if (!consumesEvent(processor.contract.consumes, event)) return event;
    const delivered = {
      ...event,
      payload: processor.contract.payloadSchemaFor(type)?.parse(payload),
    } as Consumed;
    const previousState = state;
    state = processor.reduce({ state, event: delivered }) ?? state;
    processor.processEvent({
      event: delivered,
      state,
      previousState,
      delivery: { caughtUp: true },
      append: async (...inputs) => inputs.map((input) => commit(input.type, input.payload)),
      runInBackground: run,
      blockProcessorWhile: run,
    });
    return event;
  }
  const flush = () => new Promise<void>((resolve) => setImmediate(resolve));
  return {
    state: () => state,
    lag: (ms: number) => void (lagMs = ms),
    outcome: () => ({
      ...sent,
      // consecutive repeats collapse: the indicator is refreshed while owed
      typing: sent.typing.filter((value, index, all) => index === 0 || all[index - 1] !== value),
      failed: log.filter((event) => event.type === "chief-of-staff/relay-failed").length,
      failures: log
        .filter((event) => event.type === "chief-of-staff/relay-failed")
        .map((event) => event.payload.error),
      relayed: log
        .filter((event) => event.type === "chief-of-staff/message-relayed")
        .map((event) => event.payload),
    }),
    async take(step: Step) {
      if ("input" in step)
        commit("events.iterate.com/agent/context-added", {
          role: "user",
          content: step.input,
          actor: { type: "user" },
          ...(step.quiet && { llmRequestPolicy: { behaviour: "dont-trigger-request" } }),
        });
      else if ("request" in step)
        requests.set(
          step.request,
          commit("events.iterate.com/agent/llm-request-requested", {
            model: "m",
            expiresAt: nowMs + 600_000,
            triggerOffset: log.length,
          }).offset,
        );
      else if ("settles" in step) {
        const requestOffset = requests.get(step.settles)!;
        commit("events.iterate.com/agent/llm-request-settled", {
          requestOffset,
          result: { status: "succeeded", text: step.text },
        });
        const prose = step.text.replace(/<codemode[\s\S]*<\/codemode>/, "").trim();
        if (prose)
          commit("events.iterate.com/agent/web-message-sent", {
            message: prose,
            llmRequestOffset: requestOffset,
            ...(step.besideScript && { besideScript: true }),
          });
      } else if ("status" in step)
        commit("events.iterate.com/agent/summary-updated", { activity: step.status });
      else if ("scriptSends" in step)
        commit("events.iterate.com/agent/web-message-sent", { message: step.scriptSends });
      else if ("agentPaused" in step)
        commit("events.iterate.com/agent/paused", { reason: step.agentPaused });
      else if ("scriptReturnsNothing" in step)
        // the run's request is the newest event: it was asked for after the input
        commit("events.iterate.com/itx/run-settled", {
          requestOffset: log.length,
          settlement: { status: "succeeded" },
        });
      else {
        nowMs += step.elapseMs;
        for (const sleep of sleeps.filter(({ until }) => until <= nowMs)) sleep.resolve();
      }
      await flush();
      await flush();
    },
  };
}

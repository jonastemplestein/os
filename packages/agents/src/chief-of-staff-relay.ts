// chief-of-staff-relay.ts — THE CHIEF OF STAFF'S RELAY TO JONAS: a second processor on
// /agents/chief-of-staff, beside the agent's own (as voice's relay sits on a call's agent). The
// same relay serves any agent that answers in a WhatsApp chat (the family chief of staff, in the
// family group). Where its messages go is kv's `whatsapp-relay<agent path>` (`relayTargetKey`): the
// relay holds no address of its own, and with no target in kv every message is recorded as
// `chief-of-staff/relay-failed`. The agent answers in prose like any other agent; this processor
// carries its words to Jonas on WhatsApp (from the agent's own account, to the target's chat) and
// keeps him abreast of the turn from the agent's lifecycle events:
//   - its final answer (`agent/web-message-sent` of a request, not written beside a script) and
//     the words a running script sends (`sendMessage`, no request offset) go out as one WhatsApp
//     message each — by email when WhatsApp is down (the laptop that lends it asleep);
//   - while an input that is owed an answer is being worked on, his chat shows "typing…";
//   - on a long turn, a new script status is sent as progress, at most one per PROGRESS_AFTER_MS;
//   - an agent that pauses (a breaker tripped) says so, rather than fall silent.
// So what reaches Jonas is this code's, not the model's to remember, and the agent spends no
// script round on a reply.
//
// Per-event consequences run blocked and are recorded (`chief-of-staff/message-relayed`, keyed by
// the event they carry): the engine delivers an event once, a crash between the send and the record
// can send it twice, and nothing older than MAX_RELAY_AGE_MS is sent at all — a processor enabled
// on a log with history, or replaying after a new version, folds that history and stays quiet.
import { z } from "zod";
import { errorCode } from "iterate/lib";
import { StreamProcessorDurableObject } from "iterate/sdk";
import {
  StreamProcessor,
  defineProcessorContract,
  type ConsumedEvent,
  type EmittedEventInput,
  type ProcessEventArgs,
  type ProcessorState,
  type ReduceArgs,
} from "iterate/stream/processor";
import { RunEventCatalog } from "iterate/stream/run";
import { parseCodemodeResponse } from "./codemode-format.ts";
import { AgentContract } from "./contract.ts";

/** Where a relay's messages go: the chats (messages to the first, the typing indicator to all), and
 *  the address a message goes to when WhatsApp is down, if any. */
export type RelayTarget = { chats: string[]; email: string | null };
/** kv key of an agent's target, by its path: JSON `{ chats, email }`. The deployment writes it; the
 *  agent writes from its own WhatsApp account. */
export const relayTargetKey = (agentPath: string) => `whatsapp-relay${agentPath}`;

/** The target kv holds for the agent at `agentPath`. With none, it throws: the relay then records
 *  `chief-of-staff/relay-failed` with this error, rather than send to anyone else. */
export async function readRelayTarget(
  kv: { get(key: string): Promise<string | null | undefined> },
  agentPath: string,
): Promise<RelayTarget> {
  const key = relayTargetKey(agentPath);
  const stored = await kv.get(key);
  if (!stored) throw new Error(`no relay target in kv ${key}`);
  return JSON.parse(stored) as RelayTarget;
}
/** The mark the chief of staff's messages carried while it wrote in Jonas's own chat (until
 *  2026-10-01): stripped if the model still writes it, never added. */
const OLD_AGENT_MARK = "🤖";
/** WhatsApp shows "typing…" for a few seconds per update: refreshed this often while owed. */
const TYPING_REFRESH_MS = 4_000;
/** One typing run's ceiling; a turn still owed after it is picked up by the next at-head pass. */
const TYPING_MAX_MS = 10 * 60_000;
/** How long Jonas has heard nothing before a new script status is sent as progress. */
const PROGRESS_AFTER_MS = 45_000;
/** An event older than this is history: folded, never sent. */
const MAX_RELAY_AGE_MS = 10 * 60_000;
/** An input still unanswered after this long shows no more typing: whatever ended its turn was
 *  something this fold does not read (a paused context, a deleted agent). */
const OWED_MAX_MS = 15 * 60_000;

/** Other people's chat messages, reactions and Monzo transactions reach an agent as context
 *  (chief-of-staff.ts writes these openings): most end in silence, so they show no typing and owe
 *  no progress.
 *  Everything else that starts a turn (a Pebble note, mail, a text, Jonas's own messages, another
 *  agent) is owed an answer. */
const isChatter = (content: string) =>
  /^\[(whatsapp · (chat|group) |whatsapp reaction · |whatsapp personal · |family reaction\]|monzo · )/.test(
    content,
  );

export const ChiefOfStaffRelayContract = defineProcessorContract({
  slug: "chief-of-staff-relay",
  version: "1",
  description:
    "Carries the chief of staff's answers to Jonas's WhatsApp chat and shows him the turn's progress, from the agent's lifecycle events on the same context.",
  stateSchema: z.object({
    /** The newest input owed an answer whose turn has not ended; null when nothing is owed. */
    owedSinceOffset: z.number().int().positive().nullable().default(null),
    /** When the oldest input of the owed turn arrived (ms). */
    turnStartedAtMs: z.number().nullable().default(null),
    /** When Jonas was last sent anything (ms). */
    lastRelayedAtMs: z.number().default(0),
  }),
  events: {
    "chief-of-staff/message-relayed": {
      description:
        "One message sent to Jonas: the agent's answer, words a script sent, a progress note or a notice that the agent is stuck; by WhatsApp, or by email when WhatsApp was down. `sourceOffset` is the agent event it carries.",
      payloadSchema: z.object({
        sourceOffset: z.number().int().positive(),
        kind: z.enum(["answer", "script-message", "progress", "stuck"]),
        channel: z.enum(["whatsapp", "email"]),
        text: z.string(),
        /** The project files the words attach (`attachmentsOf`): what was sent as files, not words. */
        attached: z.array(z.string()).optional(),
      }),
    },
    "chief-of-staff/relay-failed": {
      description: "A message that reached Jonas by neither WhatsApp nor email, and why.",
      payloadSchema: z.object({
        sourceOffset: z.number().int().positive(),
        text: z.string(),
        error: z.string(),
      }),
    },
  },
  /* The agent on this context owns its events, and the context its script runs; this relay only
   * reads them. */
  processorDeps: [AgentContract, RunEventCatalog],
  consumes: [
    "events.iterate.com/agent/context-added",
    "events.iterate.com/agent/llm-request-settled",
    "events.iterate.com/itx/run-settled",
    "events.iterate.com/agent/web-message-sent",
    "events.iterate.com/agent/summary-updated",
    "events.iterate.com/agent/paused",
    /* Consumed so the fold knows when Jonas last heard from here. */
    "chief-of-staff/message-relayed",
  ],
  emits: ["chief-of-staff/message-relayed", "chief-of-staff/relay-failed"],
});

type RelayState = ProcessorState<typeof ChiefOfStaffRelayContract>;
type RelayEvent = ConsumedEvent<typeof ChiefOfStaffRelayContract>;
type RelayEmitted = EmittedEventInput<typeof ChiefOfStaffRelayContract>;
type RelayArgs = ProcessEventArgs<RelayState, RelayEvent, RelayEmitted>;
type RelayKind = "answer" | "script-message" | "progress" | "stuck";

/** What the host injects: the two ways to Jonas, the typing indicator, and the clock. */
type RelayDeps = {
  sendWhatsApp(text: string): Promise<unknown>;
  sendEmail(text: string): Promise<unknown>;
  /** "typing…" on, or off, in Jonas's chat. */
  setTyping(typing: boolean): Promise<unknown>;
  now(): number;
  sleep(ms: number): Promise<void>;
};

export class ChiefOfStaffRelay extends StreamProcessor<RelayState, RelayEvent> {
  readonly contract = ChiefOfStaffRelayContract;
  private readonly deps: RelayDeps;

  constructor(deps: RelayDeps) {
    super();
    this.deps = deps;
  }

  /** The fold's `owedSinceOffset !== null`, as the typing run reads it between waits. */
  #owed = false;
  /** This incarnation's typing run, so an at-head pass does not start a second. */
  #typing = false;

  reduce({ state, event }: ReduceArgs<RelayState, RelayEvent>): RelayState | undefined {
    switch (event.type) {
      case "events.iterate.com/agent/context-added": {
        const { role, content, llmRequestPolicy } = event.payload;
        // only words that start a turn and are owed an answer: not context, not chatter
        if (
          role !== "user" ||
          llmRequestPolicy?.behaviour === "dont-trigger-request" ||
          isChatter(content)
        )
          return undefined;
        return {
          ...state,
          owedSinceOffset: event.offset,
          turnStartedAtMs:
            state.owedSinceOffset === null ? Date.parse(event.createdAt) : state.turnStartedAtMs,
        };
      }
      case "events.iterate.com/agent/llm-request-settled": {
        // The turn is over when a request that read the owed input ends without a script (its
        // answer, or nothing), or expires. A script, a correction or a failed attempt goes on.
        const { requestOffset, result } = event.payload;
        if (state.owedSinceOffset === null || requestOffset < state.owedSinceOffset)
          return undefined;
        const over =
          result.status === "succeeded"
            ? parseCodemodeResponse(result.text).kind === "none"
            : result.status === "cancelled" && result.reason === "expired";
        return over ? { ...state, owedSinceOffset: null, turnStartedAtMs: null } : undefined;
      }
      case "events.iterate.com/itx/run-settled": {
        // A script that returned nothing ends the turn too: the agent is given no result to answer.
        const { requestOffset, settlement } = event.payload;
        if (state.owedSinceOffset === null || requestOffset < state.owedSinceOffset)
          return undefined;
        return settlement.status === "succeeded" && settlement.result === undefined
          ? { ...state, owedSinceOffset: null, turnStartedAtMs: null }
          : undefined;
      }
      case "events.iterate.com/agent/paused":
        return state.owedSinceOffset === null
          ? undefined
          : { ...state, owedSinceOffset: null, turnStartedAtMs: null };
      case "chief-of-staff/message-relayed":
        return { ...state, lastRelayedAtMs: Date.parse(event.createdAt) };
      default:
        return undefined;
    }
  }

  processEvent(args: RelayArgs): undefined {
    const { event, state, delivery, runInBackground } = args;
    this.#owed =
      state.owedSinceOffset !== null &&
      this.deps.now() - (state.turnStartedAtMs ?? 0) < OWED_MAX_MS;
    // "typing…" while an answer is owed: one run per incarnation, started at the head, so a
    // replay shows nothing and a revived incarnation picks the turn up again.
    if (delivery.caughtUp && this.#owed && !this.#typing) {
      this.#typing = true;
      runInBackground(() => this.#typeWhileOwed());
    }
    if (!event) return;
    if (this.deps.now() - Date.parse(event.createdAt) > MAX_RELAY_AGE_MS) return;

    switch (event.type) {
      case "events.iterate.com/agent/web-message-sent": {
        const { message, llmRequestOffset, besideScript } = event.payload;
        // prose beside a script is said before its result exists: the typing indicator covers it
        if (besideScript) return;
        this.#relay(args, llmRequestOffset ? "answer" : "script-message", message);
        return;
      }
      case "events.iterate.com/agent/paused":
        this.#relay(
          args,
          "stuck",
          `I got stuck and stopped: ${event.payload.reason}. Send me any message to start me again.`,
        );
        return;
      case "events.iterate.com/agent/summary-updated": {
        // A script's status on a turn Jonas has waited on: progress, rationed.
        const { activity } = event.payload;
        const heardAtMs = Math.max(state.turnStartedAtMs ?? 0, state.lastRelayedAtMs);
        if (
          activity &&
          state.owedSinceOffset !== null &&
          Date.parse(event.createdAt) - heardAtMs >= PROGRESS_AFTER_MS
        )
          this.#relay(args, "progress", `… ${activity}`);
        return;
      }
    }
  }

  /** One message to Jonas for the event at hand, blocked: WhatsApp, else email, then the record. */
  #relay({ event, append, blockProcessorWhile }: RelayArgs, kind: RelayKind, words: string): void {
    if (!event) return;
    const text = words.trim().replace(new RegExp(`^${OLD_AGENT_MARK}\\s*`), "");
    if (!text) return;
    const sourceOffset = event.offset;
    blockProcessorWhile(async () => {
      let channel: "whatsapp" | "email" = "whatsapp";
      try {
        await this.deps.sendWhatsApp(text);
      } catch (whatsappError) {
        try {
          channel = "email";
          await this.deps.sendEmail(text);
        } catch (emailError) {
          // one cause for both (no relay target in kv) is said once
          const [viaWhatsApp, viaEmail] = [message(whatsappError), message(emailError)];
          await append({
            type: "chief-of-staff/relay-failed",
            idempotencyKey: this.idempotencyKey(`relay-failed/${String(sourceOffset)}`),
            payload: {
              sourceOffset,
              text,
              error:
                viaWhatsApp === viaEmail
                  ? viaWhatsApp
                  : `whatsapp: ${viaWhatsApp}; email: ${viaEmail}`,
            },
          });
          return;
        }
      }
      try {
        await append({
          type: "chief-of-staff/message-relayed",
          idempotencyKey: this.idempotencyKey(`relayed/${String(sourceOffset)}`),
          payload: {
            sourceOffset,
            kind,
            channel,
            text,
            ...(attachmentsOf(text).files.length > 0 && {
              attached: attachmentsOf(text).files.map(({ path }) => path),
            }),
          },
        });
      } catch (error) {
        // a redelivery's record of a message already recorded
        if (errorCode(error) !== "IDEMPOTENCY_CONFLICT") throw error;
      }
    });
  }

  /** "typing…" refreshed until the fold says nothing is owed (processEvent keeps `#owed` current),
   *  then switched off. Best effort: WhatsApp being down fails nothing. */
  async #typeWhileOwed(): Promise<void> {
    const startedAt = this.deps.now();
    try {
      while (this.#owed && this.deps.now() - startedAt < TYPING_MAX_MS) {
        await this.deps.setTyping(true).catch(() => undefined);
        await this.deps.sleep(TYPING_REFRESH_MS);
      }
    } finally {
      this.#typing = false;
      await this.deps.setTyping(false).catch(() => undefined);
    }
  }
}

const message = (error: unknown) =>
  (error instanceof Error ? error.message : String(error)).slice(0, 200);

/** Baileys' socket as `itx.whatsapp.jeeves` (the agents' own account) lends it: the two calls the
 *  relay makes. */
type WhatsApp = {
  sendMessage(jid: string, content: Record<string, unknown>): Promise<unknown>;
  sendPresenceUpdate(presence: "composing" | "paused", jid: string): Promise<unknown>;
};

/** The relay, hosted on an agent's context (worker.ts enables it): WhatsApp and email through this
 *  context's `itx`, under its rules, to the chat kv names for this agent. */
export class ChiefOfStaffRelayDurableObject extends StreamProcessorDurableObject<RelayState> {
  /** This agent's target, read once per incarnation once kv has one (`readRelayTarget`). */
  #targetRead?: RelayTarget;
  async #target(): Promise<RelayTarget> {
    if (this.#targetRead) return this.#targetRead;
    using itx = this.getItx();
    const { path } = await itx.whoami();
    return (this.#targetRead = await readRelayTarget(itx.kv, path));
  }

  async #whatsapp<T>(call: (whatsapp: WhatsApp) => Promise<T>): Promise<T> {
    // the scope is held until the call settles: `using` releases it as this function returns
    using itx = this.getItx();
    // `itx.whatsapp.jeeves` is a stub a client lends (rpc stubs), so iterate/api does not name it
    return await call((itx as unknown as { whatsapp: { jeeves: WhatsApp } }).whatsapp.jeeves);
  }

  /** An answer as the WhatsApp messages that carry it (`whatsappMessagesOf`), its attached project
   *  files resolved here: each one's content type, and a short-lived signed URL that Baileys
   *  fetches it from on the computer lending WhatsApp. */
  async #whatsappMessages(text: string, jid: string): Promise<Record<string, unknown>[]> {
    const { files, text: words } = attachmentsOf(text);
    if (files.length === 0) return [whatsappContent(text, jid)];
    using itx = this.getItx();
    const resolved: AttachedFile[] = [];
    for (const file of files) {
      const handle = itx.files.get(file.path);
      const head = await handle.head();
      resolved.push({
        ...file,
        found: head
          ? {
              contentType: head.contentType,
              url: (await handle.url({ expiresInSeconds: 600 })).url,
            }
          : null,
      });
    }
    return whatsappMessagesOf(words, resolved, jid);
  }

  processor = new ChiefOfStaffRelay({
    now: () => Date.now(),
    // a bare setTimeout is safe: every wait happens inside a background closure the host keeps alive
    sleep: (ms) => new Promise<void>((resolve) => setTimeout(resolve, ms)),
    sendWhatsApp: async (text) => {
      const jid = (await this.#target()).chats[0]!;
      for (const content of await this.#whatsappMessages(text, jid))
        await this.#whatsapp((whatsapp) => whatsapp.sendMessage(jid, content));
    },
    sendEmail: async (text) => {
      const { email } = await this.#target();
      if (!email) throw new Error("this relay has no email address to fall back to");
      const { files, text: words } = attachmentsOf(text);
      using itx = this.getItx();
      return await itx.email.send({
        to: email,
        subject: `Chief of staff: ${(words || files[0]?.caption || "a file").split("\n")[0]!.slice(0, 100)}`,
        text: `${words}\n\n(WhatsApp was not reachable, so this came by email.)`,
        ...(files.length > 0 && { attachments: files.map(({ path }) => ({ path })) }),
      });
    },
    setTyping: async (typing) => {
      for (const jid of (await this.#target()).chats)
        await this.#whatsapp((whatsapp) =>
          whatsapp.sendPresenceUpdate(typing ? "composing" : "paused", jid),
        );
    },
  });
}

/** WhatsApp's caption limit is 1,024 characters: longer words go as a message of their own. */
const CAPTION_MAX_CHARS = 1_000;

/** The project files an answer attaches, and its words without them. An attachment is Markdown's
 *  image form naming a project file by its path, `![caption](/path/to/file.png)`: what an agent
 *  writes in its final answer to send a picture, a PDF or any other file to the chat. A path may
 *  hold spaces (mail attachments keep their file names: `/integrations/gmail/…/Care Guide
 *  2024.pdf`), as it is, in `<…>`, or percent-encoded. A link to a web address is left as it is. */
export function attachmentsOf(text: string): {
  files: { path: string; caption: string }[];
  text: string;
} {
  const files: { path: string; caption: string }[] = [];
  const words = text
    .replace(
      /!\[([^\]]*)\]\((?:<(\/[^>\n]+)>|(\/[^)\n]+))\)/g,
      (_, caption: string, angled?: string, plain?: string) => {
        files.push({ path: decodedPath((angled ?? plain ?? "").trim()), caption });
        return "";
      },
    )
    .replace(/\n{3,}/g, "\n\n")
    .trim();
  return { files, text: words };
}

/** A path as an agent wrote it, percent-decoded when it was encoded (`Care%20Guide.pdf`). */
function decodedPath(path: string): string {
  if (!/%[0-9a-f]{2}/i.test(path)) return path;
  try {
    return decodeURI(path);
  } catch {
    return path;
  }
}

/** An attached file as the relay found it: where Baileys fetches it and what it is, or null when
 *  there is no such project file. */
export type AttachedFile = {
  path: string;
  caption: string;
  found: { contentType: string; url: string } | null;
};

/** How WhatsApp shows a file. Only what every WhatsApp client plays is sent as media; the rest goes
 *  as a document, which any client can open or save:
 *  - `image`: JPEG, PNG or WebP (an SVG or a HEIC is a document);
 *  - `video`: MP4 (a WebM or a MOV is a document);
 *  - `voice`: Ogg Opus, sent as a voice note, the one form in which an iPhone plays Ogg at all;
 *  - `audio`: MP3, M4A/AAC and the other audio types, as an audio file with a player. */
export type MediaKind = "image" | "video" | "voice" | "audio" | "document";
export function mediaKindOf(contentType: string, fileName: string): MediaKind {
  const type = contentType.split(";")[0]!.trim().toLowerCase();
  if (type === "image/jpeg" || type === "image/png" || type === "image/webp") return "image";
  if (type === "video/mp4") return "video";
  if (type === "audio/ogg" || type === "audio/opus" || /\.(ogg|oga|opus)$/i.test(fileName))
    return "voice";
  if (type.startsWith("audio/")) return "audio";
  return "document";
}

/** One file as the WhatsApp message that carries it. Baileys fetches `url`. A voice note and an
 *  audio file take no caption: `whatsappMessagesOf` sends their words as a message of their own. */
export function mediaContent(
  contentType: string,
  url: string,
  fileName: string,
  caption: string,
  mentions?: string[],
): Record<string, unknown> {
  const extra = { ...(caption && { caption }), ...(mentions && { mentions }) };
  const kind = mediaKindOf(contentType, fileName);
  if (kind === "image") return { image: { url }, ...extra };
  if (kind === "video") return { video: { url }, ...extra };
  // the exact type WhatsApp's own voice notes carry; `ptt` is what makes it one
  if (kind === "voice") return { audio: { url }, mimetype: "audio/ogg; codecs=opus", ptt: true };
  if (kind === "audio")
    return { audio: { url }, mimetype: contentType === "audio/mp3" ? "audio/mpeg" : contentType };
  return {
    document: { url },
    mimetype: contentType || "application/octet-stream",
    fileName,
    ...extra,
  };
}

/** An answer's words and attached files as the WhatsApp messages that carry them, in order:
 *  - one picture, video or document with words short enough: one message, the words its caption;
 *  - otherwise the words first, as a message of their own, then each file with its own caption (a
 *    voice note or an audio file has none, so its words are never lost in it);
 *  - a file that does not exist is said in words, not dropped. */
export function whatsappMessagesOf(
  words: string,
  files: AttachedFile[],
  jid: string,
): Record<string, unknown>[] {
  const name = (file: AttachedFile) => file.path.split("/").at(-1)!;
  const takesCaption = (file: AttachedFile) =>
    file.found !== null &&
    !["voice", "audio"].includes(mediaKindOf(file.found.contentType, name(file)));
  const captioned =
    files.length === 1 &&
    takesCaption(files[0]!) &&
    words.length > 0 &&
    words.length <= CAPTION_MAX_CHARS;
  const said = [
    ...(captioned ? [] : [words]),
    ...files
      .filter((file) => !file.found)
      .map((file) => `(I meant to attach ${file.path}, but there is no such file.)`),
  ]
    .filter(Boolean)
    .join("\n\n");
  return [
    ...(said ? [whatsappContent(said, jid)] : []),
    ...files.flatMap((file) => {
      if (!file.found) return [];
      const { text: caption, mentions } = whatsappContent(captioned ? words : file.caption, jid);
      return [mediaContent(file.found.contentType, file.found.url, name(file), caption, mentions)];
    }),
  ];
}

/** Explicit @international-number tokens become genuine mentions in group messages only. */
export function whatsappContent(text: string, jid: string): { text: string; mentions?: string[] } {
  if (!jid.endsWith("@g.us")) return { text };
  const mentions = [
    ...new Set(
      [...text.matchAll(/(?:^|\s)@(\d{7,15})(?![\d\w])/g)].map(
        (match) => match[1] + "@s.whatsapp.net",
      ),
    ),
  ];
  return mentions.length ? { text, mentions } : { text };
}

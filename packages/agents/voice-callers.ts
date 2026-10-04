// voice-callers.ts — who is on the line of a WhatsApp voice call, by the call's voice client (the
// `<client>` of `/agents/voice/<client>/<call>`: `whatsapp-<the other person's digits>`, whether
// the agent rang them or they rang it): the role the agent is in on that call, whose file its agent
// is given every turn (agent/standing-instructions.ts), and how the voice addresses the person
// (voice/voice-agent.ts).
//
// THIS MAP IS AN EXAMPLE, with fictional callers and numbers (Ofcom's drama range). A deployment's
// config repo holds the real map, in its own voice-callers.ts at the repo root; that copy is never
// copied from here, and this one never holds real people.

export type VoiceCaller = { name: string; address: string; roleFile: string };

const VOICE_CALLERS: Record<string, VoiceCaller> = {
  "whatsapp-447700900001": {
    name: "Alex Example",
    address: "sir",
    roleFile: "chief-of-staff.md",
  },
  "whatsapp-447700900002": {
    name: "Sam Example",
    address: "ma'am",
    roleFile: "family-chief-of-staff.md",
  },
};

/** The person on the call whose agent or voice relay lives at `path`, when it is a known one. */
export function voiceCallerAt(path: string): VoiceCaller | undefined {
  const client = /^\/agents\/voice\/([^/]+)\/[^/]+$/.exec(path)?.[1];
  return client === undefined ? undefined : VOICE_CALLERS[client];
}

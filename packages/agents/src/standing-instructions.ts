// standing-instructions.ts — which of the config repo's Markdown files an agent is given as its
// standing instructions on every turn (processor.ts reads them fresh), by the agent's path.
import { voiceCallerAt } from "../voice-callers.ts";

/** The files, in the order the agent reads them. Every agent reads AGENTS.md. An agent at
 *  `/agents/<name>` also reads its own `<name>.md` (chief-of-staff.md for /agents/chief-of-staff).
 *  The agent of a voice call with a known person (voice-callers.ts) reads the file of the role
 *  the agent is in with them, then voice-call.md, which says what a call changes about it. A file
 *  the repo does not have is left out. */
export function standingInstructionFiles(path: string): string[] {
  const name = /^\/agents\/([^/]+)$/.exec(path)?.[1];
  if (name) return ["AGENTS.md", `${name}.md`];
  const caller = voiceCallerAt(path);
  if (caller) return ["AGENTS.md", caller.roleFile, "voice-call.md"];
  return ["AGENTS.md"];
}

/** The most of a standing file a request shows: every request carries the file, so one an agent
 *  grew past the model's window would fail every request, and no script could shorten it again.
 *  chief-of-staff.md, the longest, was 29,853 characters on 2026-10-03. */
export const STANDING_FILE_MAX_CHARS = 100_000;

/** A standing file as its section reads it: whole, or cut at the limit with a line saying so. */
export function standingFileSection(file: string, text: string): string {
  const shown =
    text.length <= STANDING_FILE_MAX_CHARS
      ? text
      : `${text.slice(0, STANDING_FILE_MAX_CHARS)}\n\n[${file} is cut here: it is ${String(text.length)} characters, and a request shows at most ${String(STANDING_FILE_MAX_CHARS)} of a standing file. The rest is not in force until the file is shortened.]`;
  return `${file} (/repos/config), as it is now:\n\n${shown}`;
}

# TODO

## Detect the Sonnet-5.5-safeguard notice + the follow-on model-switch dialog

Live-observed in the `tmux-recherche` project: a user message tripped Sonnet
5.5's own safety classifier mid-turn. Two separate things happened, and the
plugin currently sees neither:

1. **A notice line *is* written to the transcript JSONL**, right after the
   triggering message, as its own event — not a hook, not `assistant`/`user`:

   ```json
   {"type": "system", "subtype": "informational", "level": "notice",
    "content": "Sonnet 5.5's safeguards stopped the response above · continuing once with that noted"}
   ```

   `TranscriptWatcher._event()` in `main.py` only inspects `d.get("type") in
   ("assistant", "user")` — see the `_poll()` loop. This `system` line is
   skipped entirely right now.

2. **After that notice, Claude Code can additionally show a full-screen
   "Model switch" TUI dialog** (switch to Sonnet 5 / stay on 5.5 / ...),
   which blocks the session waiting for a keypress. This dialog produces
   **no transcript line and no hook event** — nothing distinguishes it from
   Claude just taking a long time. Confirmed live: the pane sat on this
   dialog indefinitely; sijba kept showing the project as merely "working".

### What to build

- **Catch the notice (easy, same mechanism as the existing `is_error`
  handling in `TranscriptWatcher._event()`):** also match `d.get("type") ==
  "system"` with `subtype in ("informational", ...)` and relay
  `content` to sijba, e.g. `⚠️ {content}`, plus a `self.trace.append(...)`
  entry so it shows up in `/trace`. Cheap, no false negatives for *this*
  part — the notice is always written when a safeguard trips.

- **Detect the hang after it (harder — needs a timeout, not an event):**
  there is no signal for "a TUI dialog is open and waiting". The only
  option is elapsed-time heuristics in `PluginClient`:
  - track `last_progress_at` (bumped on every hook *and* every transcript
    line seen for the current envelope — both `on_hook` and
    `TranscriptWatcher._event` already fire during a turn, just add the
    bump there)
  - a background task that, once `self._current` (the in-flight envelope)
    has been set for longer than some threshold (say 60-90s) with no
    `last_progress_at` update, sends one sijba message like
    `⏳ keine Aktivität seit {n}s — evtl. hängt ein TUI-Dialog, prüfe mit
    "tmux attach -t sijba-<project>"`. Send it once per stall, not every
    poll (track `stall_notified` per envelope so it doesn't spam).
  - optional stretch: `tmux capture-pane -p` the last few lines when the
    stall fires and include a short excerpt in that sijba message instead
    of just telling the user to attach — `ClaudeTmux.capture()` already
    exists for this (see `main.py`).

### Why this matters here specifically

This isn't about the content of what tripped the safeguard — it's that
*any* mid-turn classifier trip or confirmation dialog currently leaves the
sijba-side user with no signal beyond "still waiting", indistinguishable
from a slow Bash command. The notice-relay + stall-timeout above closes
that gap generically (works for the model-switch dialog, but also any
other future TUI confirmation that blocks on a keypress).

### Pointers

- `main.py`: `TranscriptWatcher` class (`_poll`, `_event`), `PluginClient`
  (`_current`, `on_hook`, `_reply`), `ClaudeTmux.capture()`.
- Repro: inject a message that trips Sonnet 5.5's own safeguards (any
  phrasing the model itself flags mid-response), then `tmux attach -t
  sijba-<project>` and watch for the "Model switch" dialog; cross-check
  `~/.claude/projects/<workdir-slug>/*.jsonl` for the `system`/
  `informational` line timed right after it.

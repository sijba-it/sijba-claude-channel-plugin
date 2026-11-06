# sijba-claude-channel-plugin

Interactive Claude Code in a tmux session, wired to sijba via the official
**Channels** protocol + **hooks**. Standalone `kind: git` catalog plugin
(controller env contract: `SIJBA_URL SIJBA_TOKEN PROJECT_NAME WORK_DIR REGISTRY_FILE`).

```
sijba <-ws-> main.py <-unix socket-> channel.mjs (MCP channel, spawned by claude)
                 ^
                 +--- hook.py (Stop / Notification / PreToolUse)
```

- **in**: sijba message -> `notifications/claude/channel` -> `<channel source="sijba" chat_id=...>`
- **out**: claude calls the `reply` tool -> sijba reply (user only sees what claude replies)
- **Stop hook**: if claude finished without `reply`, last assistant text is read from the
  transcript and sent as fallback
- **Notification hook**: "⏳ claude waits for you" goes to sijba
- `SIJBA_TRUST_WORKDIR=1`: auto-confirm claude's one-time folder-trust dialog (otherwise attach and confirm once;
  its default is "No, exit", so plain Enter is never sent).
- `SIJBA_TOOL_STATUS=1`: PreToolUse hook sends `🔧 Bash: ...` progress lines
- `SIJBA_PERMISSION_RELAY=1`: tool-approval prompts go to sijba, answer `yes <id>` / `no <id>`
  (local terminal dialog stays open, first answer wins). Only enable where the sijba
  channel's senders are trusted.
- claude starts with `--continue` (last conversation of WORK_DIR) after any restart; `/reset` starts fresh.
  If a resumed claude dies within 30s, the next start is fresh once (no crash loop).
- Session is `tmux attach -t sijba-<project>`-able. Commands: `/reset /esc /screen [n] /key <keys> /attach /log [n] /trace [n] /archive /pause`.
  `/screen` shows the tmux pane, `/key` sends tmux key names (`/key 1`, `/key Down Enter`, `/key C-c`) —
  answers blocking TUI dialogs (e.g. the model-switch prompt) from sijba.

Runs claude with `--mcp-config`, `--settings` (hooks) and
`--dangerously-load-development-channels server:sijba` (custom channel, not on the
allowlist) — nothing is written into the project dir. main.py presses Enter on the
startup confirmation dialogs.

## Setup
Standalone: `python3 -m venv .venv && .venv/bin/pip install websockets`. The npm dependencies are
installed automatically on first start (controller git plugins have no install step).
Under the controller set `CLAUDE_CMD` to an absolute path (systemd PATH usually lacks `~/.local/bin`).
Needs: interactive `claude` logged in with a claude.ai account, tmux, node >= 18.
Team/Enterprise: org policy `channelsEnabled` must allow channels.

- **Transcript watcher**: tails the newest `~/.claude/projects/<workdir>/*.jsonl`. If a `reply` tool call
  *fails* (bridge offline, denied, ...) the text from the tool_use input is delivered to sijba anyway;
  tool errors are logged and kept for `/trace [n]` (last tool calls / errors). claude's own `system`
  notices (subtype `informational` or level warning/error, e.g. "safeguards stopped the response")
  are relayed as `⚠️ ...`.
- **Stall watchdog**: if a turn shows no hook and no transcript line for `SIJBA_STALL_SECS` (default 45,
  0 = off) *and* claude's spinner (`esc to interrupt`) is gone from the pane, one `⏳` message with the
  last pane lines is sent — a TUI dialog waiting for a keypress produces no event at all. Spinner
  visible = still working (long Bash command etc.), no warning. `/screen` shows the same state.
- **Login from sijba**: if claude isn't logged in (fresh install, or "Not logged in" in its status line
  after the session expired) the next sijba message - or `/login` - starts the login: theme/method
  screens get their defaults (subscription login), the OAuth link is sent to sijba, the code the user
  sends back is typed in (invalid code -> new link). Afterwards the waiting message is delivered.
- **Questions via hooks** (preferred, no screen scraping): `PermissionRequest` and `PreToolUse`
  (AskUserQuestion) run `hook.py` blocking; main.py asks in sijba and returns the decision as hook output.
  - permission (Bash/Edit/Write/MCP...): `1` allow, `2` allow + claude's suggestion (rule like `Bash(git push *)`
    or accept-edits mode), `3`/`nein` deny, any other text = deny with that text as the reason for claude.
  - AskUserQuestion: one sijba message per question; number, `1,3` for multi-select, or free text.
  - plan approval (ExitPlanMode) ignores a hook "allow": the hook only delivers the plan text, the answer
    goes to the terminal dialog via keys (see below).
  - claude shows its own dialog in parallel; answering it in the terminal (or `/esc`) kills the hook ->
    sijba gets "ℹ️ erledigt". Not answered from sijba -> the terminal dialog stays the fallback.
  - `SIJBA_PERMISSION_RELAY=1` (channel permission relay) disables the PermissionRequest hook.
- **TUI dialogs without a hook** (e.g. the model-switch prompt after a safeguard trip): while a turn runs the pane is
  checked every 3s (every 0.5s for 15s right after a safeguard notice); an open select dialog
  (`Enter to select` + numbered options) is relayed once with the screen 1:1 and `👉 Antworte mit der Zahl`;
  after a safeguard notice, notice + dialog arrive as ONE message. Reply with just the number — main.py
  moves the cursor, presses Enter and shows the screen 2s later. Plain text while a dialog is open is
  NOT injected (claude would only queue it) — you get the screen and the choices instead.
  Covered (tested live with claude 2.1.319): Bash/Edit/Write permission prompts, model switch,
  AskUserQuestion (single, multi-select with `0` = Next/Submit, several questions incl. the
  "Submit answers" page), plan approval, folder trust. Where the dialog takes free text
  ("Type something", "Tell Claude what to change") plain text is typed in as that answer.
  After an answer, a follow-up dialog is offered right away ("❓ Nächste Frage").
  Note: in auto mode (user setting) most permission prompts never appear. `SIJBA_AUTO_SELECT="Stay on"`
  (comma list of option-label substrings) picks matching options unasked and reports it.
- `claude` runs with `ENABLE_TOOL_SEARCH=false`: otherwise MCP tools are deferred, claude doesn't know
  the `reply` schema and answers in the terminal only.

## Status
Verified live (claude 2.1.319, Pro login, fake sijba server): channel event injection, `reply` tool,
Stop-hook fallback (also after killing the bridge mid-turn), Notification hook, **permission relay**
end to end (Bash `rm` in default mode -> `🔐 Freigabe nötig ... yes <id>` in sijba -> allowed, file deleted).
Unit-tested: transcript watcher delivering the text of a failed `reply` call.
Auto mode (`--permission-mode auto`) allowed file writes outside WORK_DIR and a passwordless `sudo apt-get`
without any prompt ("Allowed by auto mode") - nothing reaches sijba there because nothing is asked.
Known quirk: claude prints "server:sijba · no MCP server configured with that name" for `--mcp-config`
servers although `/mcp` shows it connected and events arrive (cosmetic).
Not verified: how sijba renders several `reply` messages per envelope (`continue: true`).

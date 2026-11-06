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

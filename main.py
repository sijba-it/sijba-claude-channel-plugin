#!/usr/bin/env python3
"""Claude Code *Channel* plugin for sijba.

Unlike sijba-claude-plugin (one-shot `claude -p` per turn), this keeps one
real interactive `claude` running in a tmux session (`sijba-<project>`, so you
can `tmux attach` any time) and talks to it through the official Channels
protocol:

    sijba  <--ws-->  main.py  <--unix socket-->  channel.mjs (MCP, spawned by claude)
                        ^
                        +--unix socket-- hook.py (Claude Code hooks)

  * inbound  : sijba message -> channel notification -> <channel> event in claude
  * outbound : claude calls the `reply` tool -> sijba reply
  * hooks    : Stop (fallback reply from transcript if claude forgot `reply`),
               Notification (claude waits for you), PreToolUse (opt-in status)
  * optional : permission relay (SIJBA_PERMISSION_RELAY=1) - approve tool use
               from sijba with "yes <id>" / "no <id>"

Env: SIJBA_URL SIJBA_TOKEN PROJECT_NAME WORK_DIR REGISTRY_FILE  (controller contract)
     CLAUDE_CMD (default claude), SIJBA_PERMISSION_RELAY=1, SIJBA_TOOL_STATUS=1,
     CLAUDE_EXTRA_ARGS (e.g. "--model sonnet"), SIJBA_STALL_SECS (default 45, 0 = off),
     SIJBA_AUTO_SELECT (comma list of option labels to pick unasked in TUI dialogs, e.g. "Stay on")
"""
from __future__ import annotations

import asyncio
import collections
import fcntl
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets

EXIT_ARCHIVED = 90
EXIT_PAUSED = 91

PLUGIN_NAME = "claude-channel"
HERE = Path(__file__).resolve().parent

SIJBA_URL = os.environ.get("SIJBA_URL", "wss://sijba.edv-bartl.at")
SIJBA_TOKEN = os.environ.get("SIJBA_TOKEN", "")
PROJECT_NAME = os.environ.get("PROJECT_NAME", "")
WORK_DIR = Path(os.environ.get("WORK_DIR", ""))
REGISTRY_FILE = os.environ.get("REGISTRY_FILE", "")
CLAUDE_CMD = os.environ.get("CLAUDE_CMD", "claude")
CLAUDE_EXTRA_ARGS = os.environ.get("CLAUDE_EXTRA_ARGS", "")
PERMISSION_RELAY = os.environ.get("SIJBA_PERMISSION_RELAY") == "1"
TOOL_STATUS = os.environ.get("SIJBA_TOOL_STATUS") == "1"
TRUST_WORKDIR = os.environ.get("SIJBA_TRUST_WORKDIR") == "1"
STALL_SECS = int(os.environ.get("SIJBA_STALL_SECS", "45"))
AUTO_SELECT = [x.strip().lower() for x in os.environ.get("SIJBA_AUTO_SELECT", "").split(",") if x.strip()]
DIALOG_OPTION_RE = re.compile(r"^\s*[│|]?\s*(❯)?\s*(\d+)\.\s+(.+?)\s*[│|]?\s*$")

for _name, _ok in (("SIJBA_TOKEN", SIJBA_TOKEN), ("PROJECT_NAME", PROJECT_NAME),
                   ("REGISTRY_FILE", REGISTRY_FILE)):
    if not _ok:
        print(f"ERROR: {_name} environment variable required", file=sys.stderr)
        sys.exit(1)
if not WORK_DIR.is_dir():
    print(f"ERROR: WORK_DIR {WORK_DIR} does not exist", file=sys.stderr)
    sys.exit(1)


def _ensure_node_deps() -> None:
    """Controller git plugins are just `git clone`d - no install step - so fetch
    the channel server's npm deps ourselves on first start."""
    if (HERE / "node_modules" / "@modelcontextprotocol" / "sdk").is_dir():
        return
    print("installing channel server dependencies (npm install)...", flush=True)
    r = subprocess.run(["npm", "install", "--omit=dev", "--no-audit", "--no-fund"], cwd=HERE)
    if r.returncode != 0:
        print("ERROR: npm install failed", file=sys.stderr)
        sys.exit(1)


_ensure_node_deps()

PLUGIN_ID = f"{PLUGIN_NAME}-{PROJECT_NAME}"
CHANNEL_ID = f"{PLUGIN_NAME}:{PROJECT_NAME}"
TMUX_SESSION = f"sijba-{PROJECT_NAME}"
RUN_DIR = HERE / ".run" / PROJECT_NAME
SOCK_PATH = f"/tmp/sijba-channel-{os.getuid()}-{PROJECT_NAME}.sock"  # unix socket path limit ~108

_LOG_BUFFER: collections.deque[str] = collections.deque(maxlen=200)
MAX_CHARS_TO_UI = 8000
PERM_REPLY_RE = re.compile(r"^\s*(y|yes|n|no)\s+([a-km-z]{5})\s*$", re.I)


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"{ts} [{PROJECT_NAME}] {msg}"
    _LOG_BUFFER.append(line)
    print(line, flush=True)


def _truncate(text: str, limit: int = MAX_CHARS_TO_UI) -> str:
    return text if len(text) <= limit else text[:limit] + f"\n…[truncated, {len(text) - limit} more chars]"


def _set_registry_status(name: str, status: str) -> None:
    path = Path(REGISTRY_FILE)
    with open(path, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            raw = f.read().strip()
            data = json.loads(raw) if raw else {"roots": [], "projects": {}}
            data.setdefault("projects", {}).setdefault(name, {})["status"] = status
            data["projects"][name]["status_changed_at"] = time.time()
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
            os.replace(tmp, path)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _strip_trigger(text: str) -> str:
    for trigger in (PROJECT_NAME, CHANNEL_ID):
        if text.lower().startswith(trigger.lower()):
            return text[len(trigger):].strip()
    return text.strip()


def _last_assistant_text(transcript_path: str) -> str:
    """Text blocks of the last assistant message after the last user *text* turn."""
    try:
        lines = Path(transcript_path).read_text().splitlines()
    except OSError:
        return ""
    parts: list[str] = []
    for raw in reversed(lines):
        try:
            d = json.loads(raw)
        except json.JSONDecodeError:
            continue
        content = (d.get("message") or {}).get("content")
        if d.get("type") == "user" and (isinstance(content, str) or
                any(b.get("type") == "text" for b in content or [])):
            break
        if d.get("type") == "assistant" and isinstance(content, list):
            texts = [b["text"] for b in content if b.get("type") == "text" and b.get("text")]
            parts = texts + parts
    return "\n\n".join(parts).strip()


# ---------------------------------------------------------------------------
# tmux-hosted interactive claude
# ---------------------------------------------------------------------------

def _tmux(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["tmux", *args], capture_output=True, text=True)


def _screen(pane: str, n: int = 40) -> str:
    """The pane as it looks (last n lines, blank-line runs collapsed) for a ``` block."""
    out: list[str] = []
    for l in pane.splitlines():
        l = l.rstrip()
        if l or (out and out[-1]):
            out.append(l)
    while out and not out[-1]:
        out.pop()
    return _truncate("\n".join(out[-n:]) or "(leerer Bildschirm)", 3500).replace("`" * 3, "´´´")


def _dialog_prompt(dialog: dict) -> str:
    choices = "\n".join(f"  {i} → {o}" for i, o in enumerate(dialog["options"], 1))
    return f"👉 Antworte mit der Zahl:\n{choices}\n(/esc bricht ab)"


def _parse_dialog(pane: str) -> dict | None:
    """A claude select dialog at the bottom of the pane: numbered options, one marked
    with ❯, and an "Enter to select" footer. Parsed bottom-up from the footer so numbered
    lists in claude's own answer above it don't get mixed in. Returns question, options, cursor."""
    lines = [l for l in pane.splitlines() if l.strip()][-30:]
    foot = next((i for i in range(len(lines) - 1, max(-1, len(lines) - 5), -1)
                 if "enter to select" in lines[i].lower()), None)
    if foot is None:
        return None
    found: dict[int, tuple[str, bool]] = {}
    first = None
    for i in range(foot - 1, -1, -1):
        m = DIALOG_OPTION_RE.match(lines[i])
        if not m:
            continue                    # wrapped option text / description lines
        n = int(m.group(2))
        if found and n != min(found) - 1:
            break
        found[n] = (m.group(3).strip(), bool(m.group(1)))
        first = i
        if n == 1:
            break
    if 1 not in found or len(found) < 2:
        return None
    opts = [found[n][0] for n in sorted(found)]
    cursor = next((n - 1 for n in sorted(found) if found[n][1]), 0)
    above = lines[max(0, first - 8):first]
    boxed = [l for l in above if l.lstrip().startswith(("│", "|"))]
    question = [l.strip(" │|╭╮╰╯─") for l in (boxed or above[-3:])]
    return {"question": [q for q in question if q], "options": opts, "cursor": cursor}


class ClaudeTmux:
    continued = False
    started_at = 0.0

    def has_session(self) -> bool:
        return _tmux("has-session", "-t", TMUX_SESSION).returncode == 0

    def _write_config(self) -> list[str]:
        RUN_DIR.mkdir(parents=True, exist_ok=True)
        env = {"SIJBA_CHANNEL_SOCK": SOCK_PATH}
        if PERMISSION_RELAY:
            env["SIJBA_PERMISSION_RELAY"] = "1"
        mcp = RUN_DIR / "mcp.json"
        mcp.write_text(json.dumps({"mcpServers": {"sijba": {
            "command": "node", "args": [str(HERE / "channel.mjs")], "env": env,
        }}}, indent=2))
        hook_cmd = f"SIJBA_CHANNEL_SOCK={shlex.quote(SOCK_PATH)} python3 {shlex.quote(str(HERE / 'hook.py'))}"
        hook = [{"hooks": [{"type": "command", "command": hook_cmd}]}]
        hooks = {"Stop": hook, "Notification": hook}
        if TOOL_STATUS:
            hooks["PreToolUse"] = hook
        settings = RUN_DIR / "settings.json"
        settings.write_text(json.dumps({
            "hooks": hooks,
            # the reply tool is how the user hears anything - never gate it
            "permissions": {"allow": ["mcp__sijba__reply"]},
        }, indent=2))
        return ["--mcp-config", str(mcp), "--settings", str(settings),
                "--dangerously-load-development-channels", "server:sijba"]

    def start(self, fresh: bool = False) -> None:
        """Resumes the last conversation of WORK_DIR (--continue) unless `fresh` (/reset),
        there is none yet, or the previous resumed claude died right away (see _watch_tmux)."""
        if self.has_session():
            _log(f"tmux session {TMUX_SESSION} already running — reusing")
            return
        no_continue = RUN_DIR / "no-continue-once"
        if no_continue.exists():
            no_continue.unlink()
            fresh = True
            _log("previous --continue start died right away - starting a fresh conversation")
        self.continued = not fresh and any(_transcript_dir().glob("*.jsonl"))
        self.started_at = time.monotonic()
        # ENABLE_TOOL_SEARCH=false: MCP tools are otherwise "deferred" (schema only
        # loaded after a ToolSearch call) and claude then answers in the terminal
        # instead of calling `reply`.
        cmd = shlex.join(["env", "ENABLE_TOOL_SEARCH=false", CLAUDE_CMD,
                          *self._write_config(), *(["--continue"] if self.continued else []),
                          *shlex.split(CLAUDE_EXTRA_ARGS)])
        r = _tmux("new-session", "-d", "-s", TMUX_SESSION, "-c", str(WORK_DIR),
                  "-x", "200", "-y", "50", cmd)
        if r.returncode != 0:
            raise RuntimeError(f"tmux new-session failed: {r.stderr.strip()}")
        _log(f"started claude in tmux session {TMUX_SESSION}" + (" (--continue)" if self.continued else " (fresh)"))

    def capture(self) -> str:
        return _tmux("capture-pane", "-p", "-J", "-t", TMUX_SESSION).stdout

    def busy(self, pane: str | None = None) -> bool:
        """claude's spinner line ("✻ Thinking… (esc to interrupt)") is shown for as long as a
        turn runs - incl. long tool calls. Dialogs say "esc to cancel" instead."""
        lines = [l for l in (pane if pane is not None else self.capture()).splitlines() if l.strip()]
        return any("esc to interrupt" in l.lower() for l in lines[-10:])

    def select(self, dialog: dict, n: int) -> None:
        """Move the ❯ cursor to option n (1-based) and confirm."""
        delta = (n - 1) - dialog["cursor"]
        self.keys(*(["Down"] * delta if delta > 0 else ["Up"] * -delta), "Enter")

    def keys(self, *keys: str) -> None:
        _tmux("send-keys", "-t", TMUX_SESSION, *keys)

    def stop(self) -> None:
        _tmux("kill-session", "-t", TMUX_SESSION)

    async def accept_startup_dialogs(self, timeout: float = 60) -> None:
        """The development-channel flag shows a confirmation dialog on every
        start ("I am using this for local development"); first run in a project
        may also ask to trust the MCP server. Both have the affirmative option
        preselected, so Enter confirms."""
        deadline = time.time() + timeout
        last = ""
        while time.time() < deadline and self.has_session():
            pane = self.capture()
            if "Yes, I trust this folder" in pane:
                # "No, exit" is preselected here - a blind Enter would quit claude.
                if TRUST_WORKDIR and pane != last:
                    _log("trusting WORK_DIR (SIJBA_TRUST_WORKDIR=1)")
                    self.keys("Down", "Enter")
                elif pane != last:
                    _log("folder-trust dialog open - attach and confirm once, or set SIJBA_TRUST_WORKDIR=1")
            elif pane != last and ("local development" in pane or "Use this MCP server" in pane):
                _log("accepting startup dialog")
                self.keys("Enter")
            elif "messages from server:sijba inject directly" in pane.replace("\n", " "):
                return
            last = pane
            await asyncio.sleep(1.5)


# ---------------------------------------------------------------------------
# Transcript (JSONL) watcher - second opinion on what claude actually did
# ---------------------------------------------------------------------------

def _transcript_dir() -> Path:
    # claude names the project dir after the cwd with every non-alphanumeric char -> "-"
    return Path.home() / ".claude" / "projects" / re.sub(r"[^A-Za-z0-9]", "-", str(WORK_DIR.resolve()))


class TranscriptWatcher:
    """Tails the newest transcript of WORK_DIR. Two jobs:
      * a `reply` tool call that *failed* (bridge offline, denied, ...) is not lost:
        the text is in the tool_use input, so we deliver it to sijba ourselves
      * a rolling trace (tool calls / errors) for `/trace`, plus a log line per error
    Also relays claude's own `system` notices (e.g. "safeguards stopped the response")
    and bumps the client's progress clock for the stall watchdog.
    """

    def __init__(self, client: "PluginClient"):
        self.client = client
        self.path: Path | None = None
        self.offset = 0
        self.reply_calls: dict[str, tuple[str, str]] = {}   # tool_use_id -> (chat_id, text)
        self.seen: set[str] = set()
        self.trace: collections.deque[str] = collections.deque(maxlen=100)

    def _newest(self) -> Path | None:
        try:
            files = list(_transcript_dir().glob("*.jsonl"))
        except OSError:
            return None
        return max(files, key=lambda p: p.stat().st_mtime, default=None)

    async def run(self) -> None:
        while True:
            await asyncio.sleep(1)
            try:
                await self._poll()
            except Exception as exc:
                _log(f"transcript watcher error: {exc}")

    async def _poll(self) -> None:
        newest = self._newest()
        if newest is None:
            return
        if newest != self.path:
            first = self.path is None
            self.path = newest
            # first sight: skip history; a later new session file: read from the start
            self.offset = newest.stat().st_size if first else 0
            return
        size = newest.stat().st_size
        if size < self.offset:
            self.offset = 0
        if size == self.offset:
            return
        with open(newest, "rb") as f:
            f.seek(self.offset)
            chunk = f.read()
        end = chunk.rfind(b"\n")
        if end < 0:
            return                      # incomplete last line - wait for the rest
        self.offset += end + 1
        self.client.last_progress_at = time.monotonic()
        for raw in chunk[:end].splitlines():
            try:
                d = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if d.get("type") in ("assistant", "user"):
                await self._event(d)
            elif d.get("type") == "system":
                await self._system(d)

    async def _system(self, d: dict) -> None:
        """Notices claude writes as their own transcript line - only the user-facing ones
        (informational / warning / error), not compact boundaries, durations, etc."""
        content = d.get("content")
        if not isinstance(content, str) or not content.strip():
            return
        if d.get("subtype") != "informational" and d.get("level") not in ("warning", "error"):
            return
        key = f"system:{d.get('uuid')}"
        if d.get("uuid"):
            if key in self.seen:
                return
            self.seen.add(key)
        self.trace.append(f"{datetime.now().strftime('%H:%M:%S')} !! {d.get('level') or d.get('subtype')}: {content[:160]}")
        env = self.client._current
        _log(f"system notice in transcript (envelope={env}): {content[:160]}")
        if env:
            asyncio.create_task(self.client.notice_then_dialog(env, content.strip()))

    async def _event(self, d: dict) -> None:
        content = (d.get("message") or {}).get("content")
        if not isinstance(content, list):
            return
        for b in content:
            bid = b.get("id") or b.get("tool_use_id")
            key = f"{b.get('type')}:{bid}"
            if bid and key in self.seen:
                continue
            if bid:
                self.seen.add(key)
                if len(self.seen) > 2000:
                    self.seen = set(list(self.seen)[-1000:])
            if b.get("type") == "tool_use":
                inp = b.get("input") or {}
                self.trace.append(f"{datetime.now().strftime('%H:%M:%S')} -> {b.get('name')} {json.dumps(inp)[:100]}")
                if b.get("name") == "mcp__sijba__reply":
                    self.reply_calls[b["id"]] = (str(inp.get("chat_id", "")), str(inp.get("text", "")))
            elif b.get("type") == "tool_result":
                res = b.get("content")
                if isinstance(res, list):
                    res = " ".join(x.get("text", "") for x in res if isinstance(x, dict))
                failed = bool(b.get("is_error"))
                call = self.reply_calls.pop(b.get("tool_use_id"), None)
                if failed:
                    self.trace.append(f"{datetime.now().strftime('%H:%M:%S')} !! error: {str(res)[:160]}")
                    _log(f"tool error in transcript: {str(res)[:160]}")
                if call and failed:
                    chat_id, text = call
                    _log(f"reply tool failed ({str(res)[:80]!r}) - delivering text from transcript, envelope={chat_id}")
                    if chat_id in self.client._envelope_channel and text:
                        self.client._replied.add(chat_id)
                        await self.client._reply(chat_id, text)


# ---------------------------------------------------------------------------
# Local socket server (channel.mjs + hook.py)
# ---------------------------------------------------------------------------

class Bridge:
    def __init__(self, client: "PluginClient"):
        self.client = client
        self.writer: asyncio.StreamWriter | None = None
        self.connected = asyncio.Event()

    async def serve(self) -> None:
        try:
            os.unlink(SOCK_PATH)
        except FileNotFoundError:
            pass
        server = await asyncio.start_unix_server(self._handle, path=SOCK_PATH)
        os.chmod(SOCK_PATH, 0o600)
        async with server:
            await server.serve_forever()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        is_bridge = False
        try:
            while line := await reader.readline():
                try:
                    m = json.loads(line)
                except json.JSONDecodeError:
                    continue
                t = m.get("type")
                if t == "hook":
                    asyncio.create_task(self.client.on_hook(m["event"]))
                    continue
                if not is_bridge:
                    is_bridge = True
                    self.writer = writer
                    self.connected.set()
                    _log("channel bridge connected")
                if t == "reply":
                    await self.client.on_claude_reply(m["chat_id"], m["text"])
                elif t == "permission_request":
                    await self.client.on_permission_request(m)
        finally:
            if is_bridge and self.writer is writer:
                self.writer = None
                self.connected.clear()
                _log("channel bridge disconnected")
            writer.close()

    async def send(self, obj: dict) -> bool:
        if self.writer is None:
            return False
        self.writer.write((json.dumps(obj) + "\n").encode())
        await self.writer.drain()
        return True


# ---------------------------------------------------------------------------
# sijba websocket client
# ---------------------------------------------------------------------------

class PluginClient:
    def __init__(self):
        self.ws = None
        self.claude = ClaudeTmux()
        self.bridge = Bridge(self)
        self.shutdown_event = asyncio.Event()
        self.exit_code = 0
        self._backoff = 5
        self._seen: set[str] = set()                  # envelope_ids already injected (dedupe redelivery)
        self._envelope_channel: dict[str, str] = {}   # envelope_id -> sijba channel
        self._current: str | None = None              # envelope currently being worked on
        self._replied: set[str] = set()               # envelopes claude answered via reply tool
        self._pending_perms: dict[str, dict] = {}
        self.last_progress_at = time.monotonic()      # last hook / transcript line seen
        self._stall_notified_at: float | None = None  # last_progress_at value we already warned about
        self._dialog_sig: tuple | None = None         # options of the TUI dialog already relayed
        self._notice_pending = False                  # notice_then_dialog() is still watching
        self.watcher = TranscriptWatcher(self)

    # -- sijba out -------------------------------------------------------
    async def _reply(self, envelope_id: str, text: str, channel_id: str | None = None) -> None:
        channel_id = channel_id or self._envelope_channel.get(envelope_id, CHANNEL_ID)
        try:
            await self.ws.send(json.dumps({
                "type": "reply", "channel": channel_id, "envelope_id": envelope_id,
                "text": _truncate(text), "continue": True,
            }))
        except Exception as exc:
            _log(f"failed to send reply: {exc}")

    # -- claude -> sijba -------------------------------------------------
    async def on_claude_reply(self, chat_id: str, text: str) -> None:
        self._replied.add(chat_id)
        _log(f"reply envelope={chat_id} {text[:80]!r}")
        await self._reply(chat_id, text)

    async def on_permission_request(self, m: dict) -> None:
        rid = m["request_id"]
        self._pending_perms[rid] = m
        env = self._current
        if not env:
            _log(f"permission request {rid} with no active envelope — left to the terminal dialog")
            return
        preview = m.get("input_preview") or m.get("description") or ""
        await self._reply(env, (
            f"🔐 Freigabe nötig: {m['tool_name']}\n{_truncate(preview, 1500)}\n\n"
            f"Antwort: `yes {rid}` oder `no {rid}`"
        ))

    async def on_hook(self, ev: dict) -> None:
        name = ev.get("hook_event_name")
        env = self._current
        self.last_progress_at = time.monotonic()
        _log(f"hook {name} (envelope={env})")
        if not env:
            return
        if name == "Stop":
            if env not in self._replied:
                text = (ev.get("last_assistant_message") or "").strip() \
                    or _last_assistant_text(ev.get("transcript_path", ""))
                if text:
                    _log(f"Stop without reply tool — falling back to transcript for envelope={env}")
                    await self._reply(env, text)
            self._current = None
            self._pending_perms.clear()
        elif name == "Notification":
            if self._pending_perms or self._dialog_sig or self._notice_pending:
                return              # the relayed 🔐 / ❓ message already says it
            await self._reply(env, f"⏳ {ev.get('message', 'Claude wartet auf dich')}")
        elif name == "PreToolUse" and TOOL_STATUS:
            tin = ev.get("tool_input") or {}
            hint = tin.get("command") or tin.get("file_path") or tin.get("pattern") or ""
            await self._reply(env, f"🔧 {ev.get('tool_name')}: {str(hint)[:120]}")

    # -- sijba in --------------------------------------------------------
    async def _handle_local_command(self, channel_id: str, envelope_id: str, text: str) -> bool:
        cmd, _, rest = text[1:].partition(" ")
        cmd = cmd.lower().strip()
        if cmd == "log":
            n = min(int(rest.strip()), 200) if rest.strip().isdigit() else 10
            await self._reply(envelope_id, "\n".join(list(_LOG_BUFFER)[-n:]) or "(no logs yet)", channel_id)
        elif cmd in ("archive", "pause"):
            status, code = ("archived", EXIT_ARCHIVED) if cmd == "archive" else ("paused", EXIT_PAUSED)
            try:
                _set_registry_status(PROJECT_NAME, status)
            except Exception as exc:
                await self._reply(envelope_id, f"Error updating registry: {exc}", channel_id)
                return True
            await self._reply(envelope_id, f"{PROJECT_NAME} {status}. Stopping.", channel_id)
            self.exit_code = code
            self._request_shutdown(kill_claude=True)
        elif cmd in ("reset", "kill"):
            self.claude.stop()
            await asyncio.sleep(1)
            self.claude.start(fresh=True)
            asyncio.create_task(self.claude.accept_startup_dialogs())
            self._current = None
            await self._reply(envelope_id, f"{PROJECT_NAME}: claude restarted, fresh conversation.", channel_id)
        elif cmd == "trace":
            n = min(int(rest.strip()), 100) if rest.strip().isdigit() else 15
            await self._reply(envelope_id, "\n".join(list(self.watcher.trace)[-n:]) or "(no tool activity yet)", channel_id)
        elif cmd == "esc":
            self.claude.keys("Escape")
            await self._reply(envelope_id, "Escape sent (interrupts the running turn).", channel_id)
        elif cmd == "screen":
            n = min(int(rest.strip()), 100) if rest.strip().isdigit() else 40
            pane = self.claude.capture()
            dialog = _parse_dialog(pane)
            state = ("❓ wartet auf deine Auswahl" if dialog else "🔄 arbeitet" if self.claude.busy(pane)
                     else "💤 bereit" if not self._current else "⏸ wartet (kein Spinner)")
            msg = f"{state}\n```\n{_screen(pane, n)}\n```"
            if dialog:
                self._dialog_sig = tuple(dialog["options"])
                msg += "\n" + _dialog_prompt(dialog)
            await self._reply(envelope_id, msg, channel_id)
        elif cmd == "key":
            # tmux key names, space separated: "1", "Enter", "Down Enter", "C-c"
            keys = rest.split()
            if not keys:
                await self._reply(envelope_id, "Usage: /key <keys>  e.g. /key 1   /key Down Enter   /key Escape", channel_id)
                return True
            self.claude.keys(*keys)
            self.last_progress_at = time.monotonic()
            await asyncio.sleep(1.5)
            await self._reply(envelope_id, f"⌨️ {' '.join(keys)}\n```\n{_screen(self.claude.capture(), 25)}\n```", channel_id)
        elif cmd == "attach":
            await self._reply(envelope_id, f"tmux attach -t {TMUX_SESSION}", channel_id)
        else:
            await self._reply(envelope_id, "Commands: /archive /pause /reset /esc /screen [n] /key <keys> /attach /log [n] /trace [n]. Anything else: plain text.", channel_id)
        return True

    async def handle_message(self, msg: dict) -> None:
        channel_id = msg["channel"]
        payload = msg["payload"]
        envelope_id = payload["envelope_id"]
        text = _strip_trigger(payload["text"])
        if envelope_id in self._seen:
            _log(f"redelivery of envelope={envelope_id} ignored")
            return
        self._seen.add(envelope_id)
        if len(self._seen) > 500:
            self._seen = set(list(self._seen)[-250:])
        self._envelope_channel[envelope_id] = channel_id

        if text.strip().isdigit() and (dialog := _parse_dialog(self.claude.capture())):
            n = int(text.strip())
            if 1 <= n <= len(dialog["options"]):
                self.claude.select(dialog, n)
                self.last_progress_at = time.monotonic()
                self._dialog_sig = None
                _log(f"dialog: user picked {n}. {dialog['options'][n - 1]}")
                await asyncio.sleep(2)
                pane = self.claude.capture()
                state = "🔄 Claude arbeitet weiter" if self.claude.busy(pane) else "Bildschirm jetzt:"
                await self._reply(envelope_id, f"✅ {n} → {dialog['options'][n - 1]}\n{state}\n```\n{_screen(pane, 20)}\n```", channel_id)
                return
        if m := PERM_REPLY_RE.match(text):
            rid = m.group(2).lower()
            if rid in self._pending_perms:
                allow = m.group(1).lower().startswith("y")
                self._pending_perms.pop(rid)
                await self.bridge.send({"type": "permission_verdict", "request_id": rid,
                                        "behavior": "allow" if allow else "deny"})
                await self._reply(envelope_id, f"{'✅ erlaubt' if allow else '⛔ abgelehnt'} ({rid})", channel_id)
                return
        if text.startswith("/"):
            if await self._handle_local_command(channel_id, envelope_id, text):
                return
        if not text:
            await self._reply(envelope_id, "", channel_id)
            return
        if dialog := _parse_dialog(pane := self.claude.capture()):
            # claude would just queue it behind the dialog - nothing would visibly happen
            await self._reply(envelope_id, (
                f"✋ Claude hängt gerade in einer Auswahl, deine Nachricht wurde NICHT weitergegeben.\n"
                f"```\n{_screen(pane, 25)}\n```\n{_dialog_prompt(dialog)}\nDanach die Nachricht nochmal schicken."), channel_id)
            return
        if not self.bridge.connected.is_set():
            try:
                await asyncio.wait_for(self.bridge.connected.wait(), timeout=90)
            except asyncio.TimeoutError:
                await self._reply(envelope_id, "Claude-Channel nicht bereit (Bridge nicht verbunden). Siehe /log.", channel_id)
                return
        self._current = envelope_id
        self._replied.discard(envelope_id)
        self.last_progress_at = time.monotonic()
        _log(f"inject envelope={envelope_id} text={text[:120]!r}")
        await self.bridge.send({"type": "inbound", "chat_id": envelope_id, "text": text})

    # -- lifecycle -------------------------------------------------------
    def _request_shutdown(self, kill_claude: bool = False) -> None:
        if kill_claude:
            self.claude.stop()
        self.shutdown_event.set()
        if self.ws is not None:
            asyncio.ensure_future(self.ws.close())

    async def connect(self) -> None:
        ws_url = SIJBA_URL.replace("https://", "wss://").replace("http://", "ws://")
        _log(f"Connecting to {ws_url} as {PLUGIN_ID}")
        self.ws = await websockets.connect(f"{ws_url}/api/plugin/ws?token={SIJBA_TOKEN}",
                                           ping_interval=30, ping_timeout=60, close_timeout=5)
        await self.ws.send(json.dumps({
            "type": "register", "plugin_id": PLUGIN_ID,
            "capabilities": ["conversation", "tools", "files", "code", "agent"],
            "channels": {CHANNEL_ID: {
                "label": f"Claude (channel): {PROJECT_NAME}",
                "hint": (f"Interactive Claude Code via Channels for '{PROJECT_NAME}' at {WORK_DIR}. "
                         f"Trigger: '{PROJECT_NAME} ...'. Commands: /archive /pause /reset /esc /screen [n] /key <keys> /attach /log [n] /trace [n]."),
                "triggers": [PROJECT_NAME, CHANNEL_ID],
                "capabilities": ["code", "analysis", "debugging", "files", "terminal"],
                "metadata": {"plugin": PLUGIN_NAME, "project": PROJECT_NAME, "working_dir": str(WORK_DIR)},
            }},
        }))
        self._backoff = 5
        async for raw in self.ws:
            msg = json.loads(raw)
            t = msg.get("type")
            if t == "registered":
                _log(f"Plugin registered: {msg['plugin_id']}")
            elif t == "channel_registered":
                _log(f"Channel registered: {msg['channel_id']} (ok={msg.get('ok')})")
            elif t == "message":
                asyncio.create_task(self._safe_handle(msg))
            elif t == "ping":
                await self.ws.send(json.dumps({"type": "pong"}))
            elif t == "error":
                _log(f"Server error: {msg.get('message')}")

    async def _safe_handle(self, msg: dict) -> None:
        try:
            await self.handle_message(msg)
        except Exception as exc:
            _log(f"handle_message error: {exc}")

    async def _watch_tmux(self) -> None:
        """claude exiting on its own = crash from the controller's point of view."""
        while not self.shutdown_event.is_set():
            await asyncio.sleep(5)
            if not self.claude.has_session():
                _log("tmux session vanished — exiting so the controller restarts us")
                if self.claude.continued and time.monotonic() - self.claude.started_at < 30:
                    # resumed conversation broke claude's start - don't crash-loop on it
                    RUN_DIR.mkdir(parents=True, exist_ok=True)
                    (RUN_DIR / "no-continue-once").touch()
                self.exit_code = self.exit_code or 1
                self._request_shutdown()

    async def _check_dialog(self, header: str = "") -> bool:
        """Relay a newly opened TUI select dialog to sijba (or auto-pick per SIJBA_AUTO_SELECT),
        screen 1:1 plus what to answer. True if a dialog is open."""
        env = self._current
        pane = self.claude.capture()
        dialog = _parse_dialog(pane)
        if not dialog:
            self._dialog_sig = None
            return False
        sig = tuple(dialog["options"])
        if not env or sig == self._dialog_sig or self._pending_perms:
            return True
        self._dialog_sig = sig
        for i, opt in enumerate(dialog["options"], 1):
            if any(a in opt.lower() for a in AUTO_SELECT):
                self.claude.select(dialog, i)
                _log(f"dialog: auto-selected {i}. {opt}")
                await self._reply(env, f"{header}🤖 Dialog automatisch beantwortet: {i} → {opt}")
                return True
        _log(f"dialog relayed: {sig}")
        await self._reply(env, f"{header}❓ Claude wartet auf deine Auswahl:\n```\n{_screen(pane, 25)}\n```\n{_dialog_prompt(dialog)}")
        return True

    async def notice_then_dialog(self, env: str, notice: str, seconds: float = 15) -> None:
        """A safeguard notice is often followed by the model-switch dialog. Watch the pane
        for a moment and send ONE message: notice + dialog + what to answer, or - if no
        dialog shows up - just the notice."""
        self._notice_pending = True
        header = f"⚠️ {notice}\n\n"
        try:
            end = time.monotonic() + seconds
            while time.monotonic() < end:
                if await self._check_dialog(header):
                    return
                await asyncio.sleep(0.5)
            await self._reply(env, f"{header}Claude macht von selbst weiter — du musst nichts tun.")
        finally:
            self._notice_pending = False

    async def _watch_dialogs(self) -> None:
        while not self.shutdown_event.is_set():
            await asyncio.sleep(3)
            if self._current and not self._notice_pending:
                try:
                    await self._check_dialog()
                except Exception as exc:
                    _log(f"dialog check error: {exc}")

    async def _watch_stall(self) -> None:
        """A TUI dialog waiting for a keypress (e.g. the model-switch prompt after a
        safeguard trip) leaves no hook and no transcript line - only silence. Warn once
        per stall when a turn has shown no progress for STALL_SECS and claude's spinner
        is gone (spinner visible = still working, e.g. a long Bash command)."""
        while not self.shutdown_event.is_set():
            await asyncio.sleep(10)
            env = self._current
            if not env or self._pending_perms:
                continue
            idle = time.monotonic() - self.last_progress_at
            if idle < STALL_SECS or self._stall_notified_at == self.last_progress_at:
                continue
            pane = self.claude.capture()
            if self.claude.busy(pane) or self._dialog_sig:
                continue                    # still working, or the dialog was already relayed
            self._stall_notified_at = self.last_progress_at
            _log(f"stall: no progress for {int(idle)}s (envelope={env})")
            await self._reply(env, (
                f"⏳ Seit {int(idle)}s passiert nichts und Claude arbeitet nicht. So sieht's aus:\n"
                f"```\n{_screen(pane, 25)}\n```\n"
                f"👉 Tasten schicken mit /key (z.B. /key Enter, /key 1, /key Escape) oder /esc zum Abbrechen."))

    async def run_forever(self) -> None:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: self._request_shutdown(kill_claude=True))
        asyncio.create_task(self.bridge.serve())
        self.claude.start()
        asyncio.create_task(self.claude.accept_startup_dialogs())
        asyncio.create_task(self._watch_tmux())
        asyncio.create_task(self._watch_dialogs())
        if STALL_SECS > 0:
            asyncio.create_task(self._watch_stall())
        asyncio.create_task(self.watcher.run())
        while not self.shutdown_event.is_set():
            try:
                await self.connect()
            except Exception as exc:
                _log(f"Connection failed: {exc}")
            if self.shutdown_event.is_set():
                break
            _log(f"Reconnecting in {self._backoff}s...")
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=self._backoff)
            except asyncio.TimeoutError:
                pass
            self._backoff = min(self._backoff * 2, 300)
        _log("Shutdown complete")


async def main() -> int:
    client = PluginClient()
    await client.run_forever()
    return client.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

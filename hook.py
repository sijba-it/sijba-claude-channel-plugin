#!/usr/bin/env python3
"""Claude Code hook -> main.py. Reads the hook JSON from stdin and forwards it over
the plugin's unix socket. Always exits 0.

Most events are fire-and-forget. A permission request (PermissionRequest, except
ExitPlanMode which ignores an "allow") and an AskUserQuestion (PreToolUse) instead
wait for main.py to get the user's answer from sijba and print it as the hook's
decision. No answer / main.py unreachable -> no output, claude's own terminal dialog
decides. claude kills this process when the dialog is answered in the terminal."""
import json
import os
import socket
import sys

try:
    payload = json.load(sys.stdin)
    ev, tool = payload.get("hook_event_name"), payload.get("tool_name")
    want_reply = (ev == "PermissionRequest" and tool != "ExitPlanMode") or \
                 (ev == "PreToolUse" and tool == "AskUserQuestion")
    s = socket.socket(socket.AF_UNIX)
    s.settimeout(2)
    s.connect(os.environ["SIJBA_CHANNEL_SOCK"])
    s.sendall((json.dumps({"type": "hook", "event": payload, "want_reply": want_reply}) + "\n").encode())
    if want_reply:
        s.settimeout(None)
        line = s.makefile("rb").readline()
        out = json.loads(line).get("output") if line else None
        if out:
            print(json.dumps(out))
    s.close()
except Exception:
    pass
sys.exit(0)

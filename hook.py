#!/usr/bin/env python3
"""Claude Code hook -> main.py. Reads the hook JSON from stdin, forwards it
over the plugin's unix socket, and always exits 0 (never blocks Claude)."""
import json
import os
import socket
import sys

try:
    payload = json.load(sys.stdin)
    s = socket.socket(socket.AF_UNIX)
    s.settimeout(2)
    s.connect(os.environ["SIJBA_CHANNEL_SOCK"])
    s.sendall((json.dumps({"type": "hook", "event": payload}) + "\n").encode())
    s.close()
except Exception:
    pass
sys.exit(0)

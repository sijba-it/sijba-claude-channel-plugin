# TODO

## Verify safeguard-notice relay + stall watchdog live

Implemented (see README: transcript watcher `system` notices, stall watchdog,
`/screen`, `/key`), only tested against a synthetic transcript so far. Next
time the model-switch dialog shows up in a channel project, check that:

- the `⚠️ ...` notice reaches sijba right after the safeguard trip,
- the `⏳ keine Aktivität ...` message arrives after ~`SIJBA_STALL_SECS` with
  the dialog visible in the pane excerpt,
- `/key <n>` / `/key Enter` picks the dialog option and the turn continues.

If the stall message fires too often on long silent Bash commands, raise
`SIJBA_STALL_SECS` or skip it while a `PreToolUse` without matching
`PostToolUse` is open (needs a PostToolUse hook in `_write_config`).

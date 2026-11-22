#!/bin/sh
# Verify MCP passes only the caller's credentials to its CLI subprocess.
test -z "${FLEET_ADMIN_TOKEN_FILE}" || exit 41
test "$(cat "$FLEET_TOKEN_FILE")" = scoped-token || exit 42
printf '%s\n' "$*" >> "${0%/*}/args.txt"
printf '{"apps":[{"name":"demo"}]}\n'

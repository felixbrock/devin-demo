#!/usr/bin/env bash
#
# Reset the Devin demo to a clean slate between takes.
#
# Drives the operator's POST /reset, which performs a full rollback:
#   - terminates any running Devin sessions
#   - closes the PRs raised for tracked issues and deletes their branches
#   - closes each processed GitHub issue and recreates it as a fresh copy
#     (same title/body/labels minus the trigger label, new issue number)
#   - clears the operator's local database
#
# Re-add the trigger label to a recreated issue to run the pipeline again.
#
# Usage:
#   ./reset.sh                 # targets http://localhost:8001
#   OPERATOR_URL=... ./reset.sh
#   ./reset.sh --yes           # skip the confirmation prompt
#
set -euo pipefail

OPERATOR_URL="${OPERATOR_URL:-http://localhost:8001}"
ASSUME_YES=0
case "${1:-}" in
  --yes|-y) ASSUME_YES=1 ;;
esac

echo "Operator: $OPERATOR_URL"

# Fail fast if the operator is not reachable.
if ! curl -fsS -o /dev/null "$OPERATOR_URL/health" 2>/dev/null; then
  echo "ERROR: operator not reachable at $OPERATOR_URL (is docker-compose up?)" >&2
  exit 1
fi

if [ "$ASSUME_YES" -ne 1 ]; then
  echo
  echo "This closes PRs, deletes fix branches, and recreates every tracked"
  echo "GitHub issue as a fresh copy, then clears local state. This cannot be undone."
  read -r -p "Proceed? [y/N] " reply
  case "$reply" in
    y|Y|yes|YES) ;;
    *) echo "Aborted."; exit 0 ;;
  esac
fi

echo
echo "Resetting..."
# Capture the body even on non-2xx (e.g. 501 when GITHUB_TOKEN is unset) so the
# operator's reason is shown rather than a bare failure.
resp="$(curl -sS -X POST "$OPERATOR_URL/reset" -H 'Content-Type: application/json')" || {
  echo "ERROR: reset request failed (could not reach operator)" >&2
  exit 1
}

# Pretty-print the actions/errors the operator reports.
python3 - "$resp" <<'PY'
import json, sys
raw = sys.argv[1]
try:
    data = json.loads(raw)
except (ValueError, IndexError):
    print("Unexpected response from operator:")
    print(raw or "(empty)")
    sys.exit(1)
if 'error' in data:
    print("Reset refused:", data['error'])
    sys.exit(1)
print(f"Status: {data.get('status')}  (issues reset: {data.get('issues_reset', 0)})")
actions = data.get('actions') or []
errors = data.get('errors') or []
if actions:
    print("\nActions:")
    for a in actions:
        print(f"  ✓ {a}")
if errors:
    print("\nErrors:")
    for e in errors:
        print(f"  ✗ {e}")
if not actions and not errors:
    print("\nNothing to roll back (no tracked issues).")
sys.exit(1 if errors else 0)
PY

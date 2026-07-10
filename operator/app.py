"""
Operator service - Core logic for managing Devin agent sessions

Receives labeled issues from the webhook receiver and, for each one:
1. Creates a Devin session via the Devin API (v1) with instructions to fix
   the issue, add tests that make the fix verifiable, and open a PR in
   Superset with a what/why/impact summary that references the tests run
2. Polls the session until it finishes and extracts the PR it created
3. Normalizes the PR description via the GitHub API (safety net)
4. Records the Devin Review on the PR. Reviews are triggered automatically:
   the repo is enrolled in Devin Review auto-review (Devin app -> Settings ->
   Devin Review -> Automatic review), so every PR is reviewed on creation.
   The operator only reads review status via the v3 Review API when
   DEVIN_ORG_ID / DEVIN_SERVICE_TOKEN are configured.

Restart behavior: Devin sessions keep running when the operator restarts; on
startup the operator re-attaches to any session still marked running and polls
it to completion. A session blocked with its PR already open counts as done
(unattended pipeline). Re-adding the trigger label to a failed issue restarts it.
POST /reset terminates running sessions, closes PRs and branches, and
recreates each tracked GitHub issue as a fresh copy (same content, new number).
"""

from flask import Flask, request, jsonify
import os
import re
import requests
from datetime import datetime
import json
import time
import threading

app = Flask(__name__)


@app.after_request
def add_cors_headers(response):
    """The dashboard is served from another origin (frontend on :3000)"""
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response


# Database connection (using SQLite for demo)
import sqlite3
DB_PATH = os.getenv('DB_PATH', '/app/data/devin_demo.db')

# Devin v1 API (sessions) - personal or service API key (apk_*)
DEVIN_API_KEY = os.getenv('DEVIN_API_KEY', '')
DEVIN_API_BASE = os.getenv('DEVIN_API_URL', 'https://api.devin.ai').rstrip('/')

# Devin v3 API (Devin Review status reads) - org id + service user credential (cog_*)
DEVIN_ORG_ID = os.getenv('DEVIN_ORG_ID', '')
DEVIN_SERVICE_TOKEN = os.getenv('DEVIN_SERVICE_TOKEN', '')

GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', '')
REPO_OWNER = os.getenv('REPO_OWNER', 'felixbrock')
REPO_NAME = os.getenv('REPO_NAME', 'superset')
# Label that triggers the pipeline; excluded when /reset recreates tickets
TARGET_LABEL = os.getenv('TARGET_LABEL', 'devin-fix')
# Label marking the tickets the dashboard should display (the issues Devin
# identified). The dashboard lists every issue carrying this label, live from
# GitHub, so it stays correct as issue numbers change across resets.
CATALOG_LABEL = os.getenv('CATALOG_LABEL', 'created by Devin')
REPO_FULL_NAME = f'{REPO_OWNER}/{REPO_NAME}'
MAX_CONCURRENT_SESSIONS = int(os.getenv('MAX_CONCURRENT_SESSIONS', '3'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '2'))
POLL_INTERVAL_SECONDS = int(os.getenv('POLL_INTERVAL_SECONDS', '60'))
SESSION_TIMEOUT_SECONDS = int(os.getenv('SESSION_TIMEOUT_SECONDS', '7200'))

# JSON Schema (Draft 7) Devin fills in as it works; lets us read the PR and
# test details off the session instead of parsing free text
SESSION_OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "pr_url": {"type": "string", "description": "URL of the pull request opened for the fix"},
        "pr_number": {"type": "integer", "description": "Number of the pull request"},
        "branch_name": {"type": "string", "description": "Branch the fix was pushed to"},
        "summary": {"type": "string", "description": "What was changed and why"},
        "impact": {"type": "string", "description": "Impact of the change on users and the codebase"},
        "tests_added": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Paths of test files added or modified to verify the fix"
        },
        "test_command": {"type": "string", "description": "Command used to run the tests"},
        "test_results": {"type": "string", "description": "Outcome of the test run"}
    },
    "required": ["pr_url", "branch_name", "summary", "tests_added", "test_results"]
}


def init_db():
    """Initialize the database with enhanced schema for state tracking"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Enhanced issues table with comprehensive state tracking
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS issues (
            id INTEGER PRIMARY KEY,
            issue_number INTEGER NOT NULL,
            issue_title TEXT NOT NULL,
            issue_body TEXT,
            issue_url TEXT NOT NULL,
            repository TEXT NOT NULL,
            issue_type TEXT DEFAULT 'security',
            status TEXT DEFAULT 'pending',
            agent_session_id TEXT,
            pr_url TEXT,
            pr_number INTEGER,
            branch_name TEXT,
            review_status TEXT,
            review_method TEXT,
            error_message TEXT,
            github_issue_state TEXT DEFAULT 'open',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP
        )
    ''')

    # Columns added after the initial schema; ignore failures on fresh DBs
    for column_def in ('issue_body TEXT', 'review_status TEXT', 'review_method TEXT',
                       'test_results TEXT', 'tests_added INTEGER'):
        try:
            cursor.execute(f'ALTER TABLE issues ADD COLUMN {column_def}')
        except sqlite3.OperationalError:
            pass

    # Enhanced agent sessions table with detailed tracking
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS agent_sessions (
            session_id TEXT PRIMARY KEY,
            issue_id INTEGER,
            status TEXT DEFAULT 'starting',
            devin_agent_id TEXT,
            devin_session_url TEXT,
            logs TEXT,
            error_details TEXT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (issue_id) REFERENCES issues(id)
        )
    ''')

    # Columns added to agent_sessions after the initial schema
    for column_def in ('messages TEXT',):
        try:
            cursor.execute(f'ALTER TABLE agent_sessions ADD COLUMN {column_def}')
        except sqlite3.OperationalError:
            pass

    # State transitions table for audit trail
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS state_transitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_id INTEGER,
            from_status TEXT,
            to_status TEXT,
            transition_type TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            metadata TEXT,
            FOREIGN KEY (issue_id) REFERENCES issues(id)
        )
    ''')

    # Append-only activity log per session (operator lifecycle events + folded
    # Devin messages) that drives the live feed on the dashboard
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS session_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            issue_id INTEGER,
            ts TEXT,
            kind TEXT,
            message TEXT
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_events_session ON session_events(session_id)')

    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_issues_number ON issues(issue_number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_status ON agent_sessions(status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_issue ON agent_sessions(issue_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transitions_issue ON state_transitions(issue_id)')

    conn.commit()
    conn.close()


def get_db():
    """Get database connection"""
    # Generous busy timeout: request handlers and session poller threads
    # write concurrently
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def record_state_transition(issue_id, from_status, to_status, transition_type='manual', metadata=None):
    """Record a state transition for audit trail"""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO state_transitions (issue_id, from_status, to_status, transition_type, metadata)
        VALUES (?, ?, ?, ?, ?)
    ''', (issue_id, from_status, to_status, transition_type, json.dumps(metadata) if metadata else None))

    conn.commit()
    conn.close()


def append_event(session_id, issue_id, kind, message):
    """Append one activity event to a session's live feed.

    kind is one of: 'milestone' (start/PR/tests/review/done), 'progress'
    (working heartbeats), 'status' (Devin status changes), 'devin' (a real
    Devin message), 'error'. Best-effort: never raise into the caller.
    """
    try:
        conn = get_db()
        conn.execute(
            'INSERT INTO session_events (session_id, issue_id, ts, kind, message) '
            'VALUES (?, ?, ?, ?, ?)',
            (session_id, issue_id, datetime.utcnow().isoformat() + 'Z', kind, (message or '')[:600])
        )
        conn.commit()
        conn.close()
    except Exception as e:  # pragma: no cover - logging only
        print(f'[EVENT] failed to append ({kind}): {e}')


def latest_session_feed(issue_db_id, limit=16):
    """Most recent activity events for an issue's latest session (chronological)."""
    if not issue_db_id:
        return []
    try:
        conn = get_db()
        cur = conn.cursor()
        row = cur.execute(
            'SELECT session_id FROM agent_sessions WHERE issue_id = ? '
            'ORDER BY started_at DESC LIMIT 1', (issue_db_id,)
        ).fetchone()
        if not row:
            conn.close()
            return []
        evs = cur.execute(
            'SELECT ts, kind, message FROM session_events WHERE session_id = ? '
            'ORDER BY id DESC LIMIT ?', (row['session_id'], limit)
        ).fetchall()
        conn.close()
        return [{'type': e['kind'], 'message': e['message'], 'timestamp': e['ts']}
                for e in reversed(evs)]
    except Exception as e:  # pragma: no cover
        print(f'[EVENT] feed read failed: {e}')
        return []


def fmt_elapsed(seconds):
    seconds = int(seconds)
    m, s = divmod(seconds, 60)
    return f'{m}m {s:02d}s' if m else f'{s}s'


# Semaphore for limiting concurrent agent sessions
session_semaphore = threading.Semaphore(MAX_CONCURRENT_SESSIONS)


def devin_v1_headers():
    return {
        'Authorization': f'Bearer {DEVIN_API_KEY}',
        'Content-Type': 'application/json'
    }


def build_session_prompt(issue_data, branch_name, attempt):
    """Build the task prompt for the Devin session"""
    retry_note = ''
    if attempt > 1:
        retry_note = (
            f"\nNote: this is attempt {attempt} at this task; a previous session "
            "did not complete. Start fresh.\n"
        )

    return f"""You are fixing a GitHub issue in the Apache Superset fork {issue_data['repository']}.
{retry_note}
Issue #{issue_data['issue_number']}: {issue_data['issue_title']}
Issue URL: {issue_data['issue_url']}

Issue description:
{issue_data.get('issue_body') or '(no description - read the issue at the URL above)'}

Follow these steps:

1. Clone {issue_data['repository']}, create a branch named `{branch_name}`, and
   investigate the issue until you understand the root cause.
2. Implement a fix that addresses the root cause. Keep the change minimal and
   consistent with the surrounding code.
3. REQUIRED: add or extend automated tests that fail without your fix and pass
   with it, so the fix is independently verifiable. Place them following the
   repository's existing test conventions.
4. Run the tests you added (plus any directly related existing tests) and make
   sure they pass. Record the exact command you used and the results.
5. Push the branch and open a pull request against the default branch of
   {issue_data['repository']}. The PR description MUST contain these sections:
   - ## Summary - what was changed
   - ## Why - the problem from issue #{issue_data['issue_number']} and why this is the right fix (link the issue)
   - ## Impact - effect on users and the codebase, including any risks
   - ## Tests - the test files you added, the command you ran, and the results
6. Fill in the structured output with the PR URL, PR number, branch name,
   summary, impact, the list of test files, the test command, and the test
   results. Keep it updated as you make progress.

Do not merge the PR. If you get blocked, make a reasonable autonomous decision
and note it in the PR description instead of waiting for input."""


def create_devin_session(issue_data, branch_name, attempt=1):
    """Create a Devin session via POST /v1/sessions"""
    payload = {
        'prompt': build_session_prompt(issue_data, branch_name, attempt),
        'title': f"Fix #{issue_data['issue_number']}: {issue_data['issue_title'][:80]} (attempt {attempt})",
        'tags': ['devin-demo', f"issue-{issue_data['issue_number']}"],
        'idempotent': True,
        'structured_output_schema': SESSION_OUTPUT_SCHEMA
    }

    response = requests.post(
        f'{DEVIN_API_BASE}/v1/sessions',
        json=payload,
        headers=devin_v1_headers(),
        timeout=30
    )
    response.raise_for_status()
    data = response.json()

    print(f"[DEVIN API] Created session {data['session_id']} for issue #{issue_data['issue_number']}: {data.get('url')}")

    return {
        'session_id': data['session_id'],
        'devin_session_url': data.get('url'),
        'status': 'starting'
    }


def send_session_message(session_id, message):
    """Send a follow-up message to a Devin session"""
    try:
        requests.post(
            f'{DEVIN_API_BASE}/v1/sessions/{session_id}/message',
            json={'message': message},
            headers=devin_v1_headers(),
            timeout=30
        ).raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[DEVIN API] Failed to message session {session_id}: {e}")


def terminate_devin_session(session_id):
    """
    Terminate a Devin session via DELETE /v1/sessions/{session_id}.
    A session that is already gone (terminated earlier, expired) counts
    as success - the goal is that it is not running.
    """
    try:
        response = requests.delete(
            f'{DEVIN_API_BASE}/v1/sessions/{session_id}',
            headers=devin_v1_headers(),
            timeout=30
        )
        if response.status_code in (400, 404, 410):
            print(f'[DEVIN API] Session {session_id} already gone ({response.status_code})')
            return True
        response.raise_for_status()
        print(f'[DEVIN API] Terminated session {session_id}')
        return True
    except requests.exceptions.RequestException as e:
        print(f'[DEVIN API] Failed to terminate session {session_id}: {e}')
        return False


def github_request(method, path, **kwargs):
    """Call the GitHub REST API; raises on HTTP errors"""
    response = requests.request(
        method,
        f'https://api.github.com{path}',
        headers={
            'Authorization': f'Bearer {GITHUB_TOKEN}',
            'Accept': 'application/vnd.github+json'
        },
        timeout=30,
        **kwargs
    )
    response.raise_for_status()
    return response


def resume_session_monitor(issue_id, session_id):
    """
    Re-attach to a Devin session that outlived its polling thread (operator
    restart). The session keeps running on Devin's side; poll it to completion
    and finalize exactly like a fresh run. No retry here - if the resumed
    session fails, re-adding the trigger label starts a new attempt.
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM issues WHERE id = ?', (issue_id,))
    issue = cursor.fetchone()
    conn.close()

    if not issue:
        print(f"[RESUME] Issue {issue_id} not found for session {session_id}")
        return

    issue_data = dict(issue)
    append_event(session_id, issue_id, 'status',
                 'Operator restarted — re-attached to the running Devin session')

    try:
        with session_semaphore:
            session_result = poll_devin_session(session_id, issue_id)

        if session_result['status'] == 'completed':
            finalize_completed_session(issue_id, issue_data, session_id, session_result)
            return

        error_msg = session_result.get('error', 'Unknown error')
    except Exception as e:
        error_msg = str(e)

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE agent_sessions
        SET status = 'failed', error_details = ?, updated_at = CURRENT_TIMESTAMP
        WHERE session_id = ?
    ''', (error_msg, session_id))
    cursor.execute('''
        UPDATE issues
        SET status = 'failed', error_message = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (f'Resumed session failed: {error_msg}', issue_id))
    conn.commit()
    conn.close()
    record_state_transition(issue_id, 'in_progress', 'failed', 'resumed_session_failed',
                            {'error': error_msg, 'session_id': session_id})
    append_event(session_id, issue_id, 'error', f'Resumed session failed: {error_msg}')


def recover_inflight_work():
    """
    Reconcile state after an operator restart. Sessions still marked running
    are re-attached and polled to completion (the Devin session itself never
    stopped). Issues stuck in 'pending' with no session lost their thread
    before a session was created - mark them failed so re-labeling restarts
    them.
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT session_id, issue_id FROM agent_sessions WHERE status IN ('starting', 'running')")
    orphaned = [dict(row) for row in cursor.fetchall()]
    resumed_issue_ids = {o['issue_id'] for o in orphaned}
    cursor.execute("SELECT id, status FROM issues WHERE status IN ('pending', 'in_progress')")
    stuck = [dict(row) for row in cursor.fetchall() if row['id'] not in resumed_issue_ids]
    conn.close()

    for o in orphaned:
        thread = threading.Thread(target=resume_session_monitor,
                                  args=(o['issue_id'], o['session_id']))
        thread.daemon = True
        thread.start()

    for issue in stuck:
        conn = get_db()
        conn.execute('''
            UPDATE issues
            SET status = 'failed',
                error_message = 'Operator restarted before a session was created; re-add the label to retry',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (issue['id'],))
        conn.commit()
        conn.close()
        record_state_transition(issue['id'], issue['status'], 'failed', 'operator_restarted')

    if orphaned or stuck:
        print(f'[STARTUP] Resumed {len(orphaned)} running session(s), '
              f'failed {len(stuck)} issue(s) that never got a session')


def poll_devin_session(session_id, issue_id):
    """
    Poll GET /v1/sessions/{session_id} until the session reaches a terminal
    state or SESSION_TIMEOUT_SECONDS elapses.

    Returns {'status': 'completed'|'failed', 'structured_output': ...,
             'pull_request': ..., 'logs': ..., 'error': ...}
    """
    deadline = time.monotonic() + SESSION_TIMEOUT_SECONDS
    started = time.monotonic()
    nudged_blocked = False
    pr_recorded = False
    seen_message_ids = set()
    last_status = None

    while time.monotonic() < deadline:
        try:
            response = requests.get(
                f'{DEVIN_API_BASE}/v1/sessions/{session_id}',
                headers=devin_v1_headers(),
                timeout=30
            )
            response.raise_for_status()
            session = response.json()
        except requests.exceptions.RequestException as e:
            print(f"[MONITOR] Poll error for {session_id}: {e}")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        status = session.get('status_enum') or session.get('status')
        log_line = f"{datetime.utcnow().isoformat()}Z status={status}"
        print(f"[MONITOR] Session {session_id} (issue {issue_id}): {status}")

        # Devin returns the full activity timeline each poll; persist the latest
        # so the dashboard can show Devin's live progress messages.
        messages_json = json.dumps(session.get('messages') or [])

        conn = get_db()
        conn.execute('''
            UPDATE agent_sessions
            SET logs = COALESCE(logs, '') || ? || char(10),
                messages = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE session_id = ?
        ''', (log_line, messages_json, session_id))
        conn.commit()
        conn.close()

        # Fold any new real Devin messages into the feed
        for m in (session.get('messages') or []):
            mid = m.get('event_id') or m.get('timestamp')
            text = (m.get('message') or '').strip()
            if mid and mid not in seen_message_ids and text:
                seen_message_ids.add(mid)
                append_event(session_id, issue_id, 'devin', text)

        # Note status changes as they happen
        if status != last_status:
            last_status = status
            pretty = {
                'working': 'Devin is working on the fix',
                'blocked': 'Devin paused for input — nudging it to continue autonomously',
                'resumed': 'Devin resumed',
            }.get(status, f'Status: {status}')
            append_event(session_id, issue_id, 'status', pretty)

        # Deterministic heartbeat so the feed always advances, even when Devin
        # is quiet between messages
        append_event(session_id, issue_id, 'progress',
                     f'Working on the fix… ({fmt_elapsed(time.monotonic() - started)} elapsed)')

        structured = session.get('structured_output') or {}
        pr_url_live = structured.get('pr_url') or (session.get('pull_request') or {}).get('url')

        # Surface the PR on the ticket as soon as it exists, not only at
        # session completion, so the dashboard links it while work continues
        if pr_url_live and not pr_recorded:
            pr_recorded = True
            parsed = parse_pr_url(pr_url_live)
            pr_number_live = structured.get('pr_number') or (parsed[2] if parsed else None)
            conn = get_db()
            conn.execute('''
                UPDATE issues SET pr_url = ?, pr_number = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND pr_url IS NULL
            ''', (pr_url_live, pr_number_live, issue_id))
            conn.commit()
            conn.close()
            append_event(session_id, issue_id, 'milestone', f'Pull request opened: {pr_url_live}')

        if status == 'finished':
            append_event(session_id, issue_id, 'milestone', 'Devin finished — collecting the pull request and results')
            return {
                'status': 'completed',
                'structured_output': structured,
                'pull_request': session.get('pull_request'),
                'logs': f'Session finished. Final status: {status}'
            }

        if status == 'expired':
            append_event(session_id, issue_id, 'error', 'Devin session expired before completing')
            return {'status': 'failed', 'error': 'Devin session expired before completing'}

        if status == 'blocked':
            # Sessions block when Devin wants human input; this pipeline is
            # unattended. If the PR is already open, the deliverable exists -
            # count the session as done. Otherwise tell it once to proceed on
            # its own judgment.
            if pr_url_live and nudged_blocked:
                append_event(session_id, issue_id, 'milestone',
                             'PR is open and Devin is waiting for input — collecting results')
                return {
                    'status': 'completed',
                    'structured_output': structured,
                    'pull_request': session.get('pull_request'),
                    'logs': f'Session blocked with PR open; treated as complete. Final status: {status}'
                }
            if not nudged_blocked:
                nudged_blocked = True
                send_session_message(
                    session_id,
                    'This session runs unattended. Make a reasonable autonomous decision, '
                    'note it in the PR description, and continue until the PR is open '
                    'and the structured output is filled in.'
                )

        time.sleep(POLL_INTERVAL_SECONDS)

    return {'status': 'failed', 'error': f'Session timed out after {SESSION_TIMEOUT_SECONDS}s'}


def parse_pr_url(pr_url):
    """Extract (owner, repo, number) from a GitHub PR URL"""
    match = re.search(r'github\.com/([^/]+)/([^/]+)/pull/(\d+)', pr_url or '')
    if not match:
        return None
    return match.group(1), match.group(2), int(match.group(3))


CVSS_RE = re.compile(r'CVSS[^\d]{0,20}(\d{1,2}\.\d)', re.IGNORECASE)


def parse_cvss(body):
    """Best-effort extraction of a CVSS score from an issue body; None if absent"""
    match = CVSS_RE.search(body or '')
    if not match:
        return None
    try:
        score = float(match.group(1))
    except ValueError:
        return None
    return score if 0 <= score <= 10 else None


def severity_for_cvss(cvss):
    """Standard NVD/FIRST CVSS v3 qualitative severity band"""
    if cvss is None:
        return None
    if cvss >= 9.0:
        return 'Critical'
    if cvss >= 7.0:
        return 'High'
    if cvss >= 4.0:
        return 'Medium'
    if cvss > 0:
        return 'Low'
    return 'None'


def build_pr_body(issue_data, structured):
    """Build the standardized PR description from the session's structured output"""
    tests = structured.get('tests_added') or []
    tests_list = '\n'.join(f'- `{t}`' for t in tests) if tests else '- (none reported)'

    return f"""## Summary

{structured.get('summary', f"Fix for issue #{issue_data['issue_number']}: {issue_data['issue_title']}")}

## Why

Resolves #{issue_data['issue_number']} ({issue_data['issue_url']}): {issue_data['issue_title']}

## Impact

{structured.get('impact', 'See summary above.')}

## Tests

{tests_list}

Command: `{structured.get('test_command', 'n/a')}`

Results: {structured.get('test_results', 'n/a')}

---
**Automated fix by Devin AI** via the devin-demo operator.
Session-created PR; description normalized by the operator.
"""


def ensure_pr_description(pr_url, issue_data, structured):
    """
    Safety net: overwrite the PR body with the standardized what/why/impact/tests
    template built from the session's structured output. Skipped when no
    GITHUB_TOKEN is configured (Devin's own PR description is kept).
    """
    if not GITHUB_TOKEN:
        print('[GITHUB] No GITHUB_TOKEN set - keeping PR description as written by Devin')
        return

    parsed = parse_pr_url(pr_url)
    if not parsed:
        print(f'[GITHUB] Could not parse PR URL: {pr_url}')
        return

    owner, repo, number = parsed
    try:
        github_request('PATCH', f'/repos/{owner}/{repo}/pulls/{number}',
                       json={'body': build_pr_body(issue_data, structured)})
        print(f'[GITHUB] Normalized description of PR #{number}')
    except requests.exceptions.RequestException as e:
        print(f'[GITHUB] Failed to update PR description: {e}')


def get_devin_review_status(pr_url):
    """Get the latest Devin Review status for a PR (v3 API only)"""
    if not (DEVIN_ORG_ID and DEVIN_SERVICE_TOKEN):
        return None
    response = requests.get(
        f'{DEVIN_API_BASE}/v3/organizations/{DEVIN_ORG_ID}/pr-reviews',
        params={'pr_url': pr_url},
        headers={'Authorization': f'Bearer {DEVIN_SERVICE_TOKEN}'},
        timeout=30
    )
    response.raise_for_status()
    return response.json()


def finalize_completed_session(issue_id, issue_data, session_id, session_result):
    """
    Record a delivered session: normalize the PR description, read the Devin
    Review status, and mark the issue and session completed.
    Raises if the session reported no PR.
    """
    structured = session_result.get('structured_output') or {}

    pr_url = structured.get('pr_url')
    if not pr_url and session_result.get('pull_request'):
        pr_url = session_result['pull_request'].get('url')

    if not pr_url:
        raise Exception('Session finished without reporting a PR')

    parsed = parse_pr_url(pr_url)
    pr_number = structured.get('pr_number') or (parsed[2] if parsed else None)

    # Safety net: enforce the what/why/impact/tests PR template
    ensure_pr_description(pr_url, issue_data, structured)

    # Reviews are auto-triggered (repo enrolled in Devin Review
    # auto-review); just record the current status if readable
    review = {'method': 'auto_review', 'status': 'pending'}
    try:
        review_data = get_devin_review_status(pr_url)
        if review_data:
            review['status'] = review_data.get('status')
    except requests.exceptions.RequestException as e:
        print(f'[DEVIN REVIEW] Could not read review status: {e}')

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE issues
        SET status = 'completed', pr_url = ?, pr_number = ?,
            review_status = ?, review_method = ?,
            test_results = ?, tests_added = ?,
            completed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (pr_url, pr_number, review.get('status'), review.get('method'),
          structured.get('test_results'),
          1 if structured.get('tests_added') else 0,
          issue_id))
    cursor.execute('''
        UPDATE agent_sessions
        SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP,
            logs = COALESCE(logs, '') || ? || char(10)
        WHERE session_id = ?
    ''', (session_result.get('logs', ''), session_id))
    conn.commit()
    conn.close()

    record_state_transition(issue_id, 'in_progress', 'completed', 'agent_completed', {
        'pr_url': pr_url,
        'pr_number': pr_number,
        'tests_added': structured.get('tests_added'),
        'test_results': structured.get('test_results'),
        'review': {'method': review.get('method'), 'status': review.get('status')}
    })

    append_event(session_id, issue_id, 'milestone',
                 f"Opened pull request #{pr_number}" if pr_number else "Opened pull request")
    if structured.get('test_results'):
        append_event(session_id, issue_id, 'milestone',
                     f"Tests: {structured.get('test_results')}")
    append_event(session_id, issue_id, 'milestone',
                 f"Devin Review: {review.get('status') or 'pending'}")
    append_event(session_id, issue_id, 'milestone',
                 "Fix complete ✓")

    print(f"[SUCCESS] Issue #{issue_data['issue_number']} completed: {pr_url}")


def process_issue_with_retry(issue_id, max_retries=MAX_RETRIES):
    """
    Process an issue end to end with retry logic on session failure:
    Devin session (fix + tests + PR) -> PR description normalization ->
    record Devin Review status (review itself is auto-triggered on PR creation)
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM issues WHERE id = ?', (issue_id,))
    issue = cursor.fetchone()
    conn.close()

    if not issue:
        print(f"[ERROR] Issue {issue_id} not found")
        return False

    issue_data = dict(issue)
    branch_name = f"devin/fix-issue-{issue_data['issue_number']}"

    for attempt in range(1, max_retries + 1):
        try:
            print(f"[PROCESS] Attempt {attempt}/{max_retries} for issue #{issue_data['issue_number']}")

            with session_semaphore:
                session_data = create_devin_session(issue_data, branch_name, attempt)

                conn = get_db()
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE issues
                    SET status = 'in_progress', agent_session_id = ?, branch_name = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (session_data['session_id'], branch_name, issue_id))
                cursor.execute('''
                    INSERT OR REPLACE INTO agent_sessions
                        (session_id, issue_id, status, devin_session_url, started_at)
                    VALUES (?, ?, 'running', ?, CURRENT_TIMESTAMP)
                ''', (session_data['session_id'], issue_id, session_data['devin_session_url']))
                conn.commit()
                conn.close()

                record_state_transition(issue_id, 'pending', 'in_progress', f'agent_started_attempt_{attempt}', {
                    'session_id': session_data['session_id'],
                    'session_url': session_data['devin_session_url'],
                    'branch_name': branch_name,
                    'attempt': attempt
                })

                sid = session_data['session_id']
                append_event(sid, issue_id, 'milestone',
                             f"Devin session created (attempt {attempt}) — cloning {issue_data['repository']} "
                             f"and reading issue #{issue_data['issue_number']}")

                session_result = poll_devin_session(sid, issue_id)

            if session_result['status'] == 'completed':
                finalize_completed_session(issue_id, issue_data,
                                           session_data['session_id'], session_result)
                return True

            # Session failed; record and retry
            error_msg = session_result.get('error', 'Unknown error')

            conn = get_db()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE agent_sessions
                SET status = 'failed', error_details = ?, updated_at = CURRENT_TIMESTAMP
                WHERE session_id = ?
            ''', (error_msg, session_data['session_id']))
            cursor.execute('''
                UPDATE issues
                SET error_message = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (f'Attempt {attempt} failed: {error_msg}', issue_id))
            conn.commit()
            conn.close()

            record_state_transition(issue_id, 'in_progress', 'failed', f'agent_failed_attempt_{attempt}', {
                'error': error_msg,
                'attempt': attempt
            })

            append_event(session_data['session_id'], issue_id, 'error',
                         f'Attempt {attempt} failed: {error_msg}'
                         + (' — retrying' if attempt < max_retries else ''))

            print(f"[RETRY] Issue #{issue_data['issue_number']} failed on attempt {attempt}")

        except Exception as e:
            print(f"[ERROR] Exception in processing issue #{issue_data['issue_number']}: {e}")
            try:
                append_event(session_data['session_id'], issue_id, 'error', f'Attempt {attempt} error: {e}')
            except Exception:
                pass

            if attempt == max_retries:
                conn = get_db()
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE issues
                    SET status = 'failed', error_message = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (f'All {max_retries} attempts failed: {str(e)}', issue_id))
                conn.commit()
                conn.close()

                record_state_transition(issue_id, 'in_progress', 'failed', 'all_retries_exhausted', {
                    'error': str(e),
                    'max_retries': max_retries
                })
                return False

    # Final failure without an exception on the last attempt
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE issues
        SET status = 'failed', updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (issue_id,))
    conn.commit()
    conn.close()
    return False


@app.route('/issues', methods=['POST'])
def create_issue():
    """
    Receive issue from webhook and create tracking record
    """
    try:
        data = request.json

        conn = get_db()
        cursor = conn.cursor()

        # Check if issue already exists (idempotent)
        cursor.execute('SELECT id, status FROM issues WHERE issue_number = ? AND repository = ?',
                       (data['issue_number'], data['repository']))
        existing = cursor.fetchone()

        if existing:
            # Re-adding the label to a failed ticket restarts it; anything
            # in flight or completed stays untouched
            if existing['status'] == 'failed':
                cursor.execute('''
                    UPDATE issues
                    SET status = 'pending', error_message = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (existing['id'],))
                conn.commit()
                conn.close()

                record_state_transition(existing['id'], 'failed', 'pending', 'relabel_restart', {
                    'github_action': data.get('action'),
                    'sender': data.get('sender')
                })

                thread = threading.Thread(target=process_issue_with_retry, args=(existing['id'],))
                thread.daemon = True
                thread.start()

                return jsonify({
                    'status': 'restarted',
                    'issue_id': existing['id'],
                    'message': 'Failed issue restarted after re-label'
                }), 200

            conn.close()
            return jsonify({
                'status': 'exists',
                'issue_id': existing['id'],
                'message': 'Issue already tracked'
            }), 200

        cursor.execute('''
            INSERT INTO issues (issue_number, issue_title, issue_body, issue_url, repository, issue_type, status)
            VALUES (?, ?, ?, ?, ?, 'security', 'pending')
        ''', (
            data['issue_number'],
            data['issue_title'],
            data.get('issue_body', ''),
            data['issue_url'],
            data['repository']
        ))

        issue_id = cursor.lastrowid
        conn.commit()
        conn.close()

        record_state_transition(issue_id, None, 'pending', 'webhook_received', {
            'github_action': data.get('action'),
            'sender': data.get('sender')
        })

        # Process in the background: Devin session -> PR -> Devin Review
        thread = threading.Thread(target=process_issue_with_retry, args=(issue_id,))
        thread.daemon = True
        thread.start()

        return jsonify({
            'status': 'created',
            'issue_id': issue_id,
            'message': 'Issue tracked and Devin session starting in background'
        }), 201

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/issues', methods=['GET'])
def list_issues():
    """List all tracked issues"""
    try:
        conn = get_db()
        cursor = conn.cursor()

        # Enrich each issue with when the agent first started working (earliest
        # session start) so the dashboard can show elapsed working time
        cursor.execute('''
            SELECT i.*,
                   (SELECT MIN(s.started_at) FROM agent_sessions s WHERE s.issue_id = i.id)
                       AS work_started_at
            FROM issues i
            ORDER BY i.created_at DESC
        ''')
        issues = cursor.fetchall()

        conn.close()

        return jsonify({
            'issues': [dict(issue) for issue in issues]
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


def fetch_labeled_issues():
    """
    Fetch every GitHub issue in the target repo carrying CATALOG_LABEL
    (open and closed), following pagination. Pull requests are excluded.
    """
    issues = []
    page = 1
    while True:
        resp = github_request(
            'GET', f'/repos/{REPO_FULL_NAME}/issues',
            params={'labels': CATALOG_LABEL, 'state': 'all', 'per_page': 100, 'page': page}
        )
        batch = resp.json()
        if not batch:
            break
        issues.extend(i for i in batch if 'pull_request' not in i)
        if len(batch) < 100:
            break
        page += 1
    return issues


def tracking_by_number():
    """Map GitHub issue number -> operator tracking record (this repo only)"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT i.id AS issue_db_id,
               i.issue_number, i.status, i.agent_session_id, i.pr_url, i.pr_number,
               i.branch_name, i.review_status, i.error_message, i.completed_at,
               i.test_results, i.tests_added,
               (SELECT MIN(s.started_at) FROM agent_sessions s WHERE s.issue_id = i.id)
                   AS work_started_at
        FROM issues i
        WHERE i.repository = ?
    ''', (REPO_FULL_NAME,))
    rows = {row['issue_number']: dict(row) for row in cursor.fetchall()}
    conn.close()
    return rows


def recent_messages(raw, limit=14):
    """Parse stored Devin session messages JSON into a compact recent feed.

    Devin's GET session response returns a `messages` array of activity events
    (type, message, timestamp) representing what the agent is doing. We keep the
    most recent `limit` so the dashboard can render a live progress log.
    """
    if not raw:
        return []
    try:
        msgs = json.loads(raw)
    except (ValueError, TypeError):
        return []
    if not isinstance(msgs, list):
        return []
    out = []
    for m in msgs[-limit:]:
        if not isinstance(m, dict):
            continue
        text = (m.get('message') or '').strip()
        if not text:
            continue
        out.append({
            'type': m.get('type'),
            'message': text[:600],
            'timestamp': m.get('timestamp'),
        })
    return out


def tests_passed_from(text):
    """Best-effort pass/fail read of Devin's free-text test_results.

    Returns True (clearly passing), False (clearly failing), or None (unknown /
    not reported). Conservative by design: it must not read a passing run that
    says "0 failed" / "no failures" as a failure, so negated failure wording is
    ignored and only a real non-zero failure count or an un-negated failure word
    yields False.
    """
    if not text:
        return None
    t = text.lower()

    # Explicit non-zero failure count, e.g. "2 failed", "1 test failure".
    m = re.search(r'(\d+)\s*(?:test\w*\s*)?(?:failed|failing|failures?|errors?)', t)
    if m and int(m.group(1)) > 0:
        return False

    negated_fail = any(p in t for p in (
        '0 failed', '0 failures', 'no failures', 'no failing', 'none failed',
        'without failures', '0 errors', 'no errors',
    ))
    has_pass = any(p in t for p in (
        'pass', 'passed', 'passing', 'all green', 'succeeded', 'success',
    ))
    has_fail = ('fail' in t or 'error' in t) and not negated_fail

    if has_fail:
        return False
    if has_pass:
        return True
    return None


@app.route('/tickets', methods=['GET'])
def list_tickets():
    """
    The dashboard's source of truth: every GitHub issue labeled CATALOG_LABEL,
    live from GitHub, enriched with the operator's work status. Independent of
    issue numbers, so it stays correct after resets recreate tickets.
    """
    if not GITHUB_TOKEN:
        return jsonify({
            'error': 'GITHUB_TOKEN must be set to list tickets by label',
            'label': CATALOG_LABEL
        }), 501

    try:
        gh_issues = fetch_labeled_issues()
    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'GitHub API error: {str(e)}'}), 502

    tracking = tracking_by_number()
    tickets = []
    for gh in gh_issues:
        number = gh['number']
        # Closed issues with no work history are reset leftovers (originals
        # replaced by a fresh copy) - hide them so tickets don't show twice
        if gh.get('state') != 'open' and number not in tracking:
            continue
        body = gh.get('body')
        cvss = parse_cvss(body)
        track = tracking.get(number, {})
        tickets.append({
            'issue_number': number,
            'issue_title': gh.get('title'),
            'issue_url': gh.get('html_url'),
            'github_state': gh.get('state'),
            'cvss': cvss,
            'severity': severity_for_cvss(cvss),
            'status': track.get('status', 'identified'),
            'agent_session_id': track.get('agent_session_id'),
            'pr_url': track.get('pr_url'),
            'pr_number': track.get('pr_number'),
            'branch_name': track.get('branch_name'),
            'review_status': track.get('review_status'),
            'error_message': track.get('error_message'),
            'test_results': track.get('test_results'),
            'tests_added': bool(track.get('tests_added')) if track.get('tests_added') is not None else None,
            'tests_passed': tests_passed_from(track.get('test_results')),
            'messages': latest_session_feed(track.get('issue_db_id')),
            'work_started_at': track.get('work_started_at'),
            'completed_at': track.get('completed_at'),
        })

    return jsonify({'repository': REPO_FULL_NAME, 'label': CATALOG_LABEL, 'tickets': tickets}), 200


@app.route('/issues/<int:issue_id>', methods=['GET'])
def get_issue(issue_id):
    """Get specific issue details"""
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM issues WHERE id = ?', (issue_id,))
        issue = cursor.fetchone()

        conn.close()

        if not issue:
            return jsonify({'error': 'Issue not found'}), 404

        return jsonify(dict(issue)), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/issues/<int:issue_id>/start', methods=['POST'])
def start_agent_session(issue_id):
    """
    Manually (re)start processing for a pending issue
    """
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM issues WHERE id = ?', (issue_id,))
        issue = cursor.fetchone()
        conn.close()

        if not issue:
            return jsonify({'error': 'Issue not found'}), 404

        if issue['status'] not in ('pending', 'failed'):
            return jsonify({'error': f'Issue not in a startable state (current: {issue["status"]})'}), 400

        thread = threading.Thread(target=process_issue_with_retry, args=(issue_id,))
        thread.daemon = True
        thread.start()

        return jsonify({
            'status': 'started',
            'message': 'Processing started in background'
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/issues/<int:issue_id>/review', methods=['GET'])
def get_issue_review(issue_id):
    """Get the latest Devin Review status for the issue's PR"""
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT pr_url FROM issues WHERE id = ?', (issue_id,))
        issue = cursor.fetchone()
        conn.close()

        if not issue:
            return jsonify({'error': 'Issue not found'}), 404
        if not issue['pr_url']:
            return jsonify({'error': 'Issue has no PR yet'}), 400

        review = get_devin_review_status(issue['pr_url'])
        if review is None:
            return jsonify({'error': 'Review API not configured (DEVIN_ORG_ID / DEVIN_SERVICE_TOKEN)'}), 501

        return jsonify(review), 200

    except requests.exceptions.RequestException as e:
        return jsonify({'error': f'Devin Review API error: {str(e)}'}), 502
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/sessions/<session_id>/complete', methods=['POST'])
def complete_session(session_id):
    """
    Mark agent session as complete with PR URL and details
    """
    try:
        data = request.json
        pr_url = data.get('pr_url')
        pr_number = data.get('pr_number')
        logs = data.get('logs')
        devin_agent_id = data.get('devin_agent_id')

        conn = get_db()
        cursor = conn.cursor()

        cursor.execute('SELECT issue_id FROM agent_sessions WHERE session_id = ?', (session_id,))
        session = cursor.fetchone()

        if not session:
            conn.close()
            return jsonify({'error': 'Session not found'}), 404

        issue_id = session['issue_id']

        cursor.execute('''
            UPDATE agent_sessions
            SET status = 'completed',
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                logs = ?,
                devin_agent_id = ?
            WHERE session_id = ?
        ''', (logs, devin_agent_id, session_id))

        cursor.execute('''
            UPDATE issues
            SET status = 'completed',
                pr_url = ?,
                pr_number = ?,
                completed_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (pr_url, pr_number, issue_id))

        # Commit before recording the transition: record_state_transition
        # opens its own connection and would deadlock against our
        # uncommitted writes
        conn.commit()
        conn.close()

        record_state_transition(issue_id, 'in_progress', 'completed', 'agent_completed', {
            'session_id': session_id,
            'pr_url': pr_url,
            'pr_number': pr_number
        })

        return jsonify({
            'status': 'completed',
            'message': 'Session marked as complete',
            'pr_url': pr_url,
            'pr_number': pr_number
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/reset', methods=['POST'])
def reset_system():
    """
    Reset the demo to its initial state:
    1. Terminate Devin sessions that are still running
    2. Close the PRs raised for tracked issues and delete their branches
    3. Close each processed GitHub issue and recreate it as a fresh copy
       (same title/body/labels minus the trigger label; the copy gets a
       new issue number, which is fine - content is what matters)
    4. Clear the local database

    Re-adding the trigger label to a fresh copy runs the pipeline again.
    Best-effort: individual GitHub/Devin failures are collected in 'errors'
    rather than aborting the reset.
    """
    if not GITHUB_TOKEN:
        return jsonify({
            'error': 'GITHUB_TOKEN must be set: reset closes PRs/issues and recreates tickets via the GitHub API'
        }), 501

    try:
        actions = []
        errors = []

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM issues')
        issues_to_reset = [dict(row) for row in cursor.fetchall()]
        cursor.execute("SELECT session_id FROM agent_sessions WHERE status IN ('starting', 'running')")
        running_sessions = [row['session_id'] for row in cursor.fetchall()]
        conn.close()

        # Kill running sessions first so nothing pushes new commits mid-reset
        for session_id in running_sessions:
            if terminate_devin_session(session_id):
                actions.append(f'Terminated Devin session {session_id}')
            else:
                errors.append(f'Could not terminate Devin session {session_id}')

        for issue in issues_to_reset:
            repo = issue['repository']
            number = issue['issue_number']

            if issue['pr_url']:
                parsed = parse_pr_url(issue['pr_url'])
                if parsed:
                    try:
                        github_request('PATCH', f'/repos/{parsed[0]}/{parsed[1]}/pulls/{parsed[2]}',
                                       json={'state': 'closed'})
                        actions.append(f'Closed PR #{parsed[2]} for issue #{number}')
                    except requests.exceptions.RequestException as e:
                        errors.append(f'Could not close PR for issue #{number}: {e}')

            if issue['branch_name']:
                try:
                    github_request('DELETE', f"/repos/{repo}/git/refs/heads/{issue['branch_name']}")
                    actions.append(f"Deleted branch {issue['branch_name']}")
                except requests.exceptions.RequestException:
                    pass  # branch may never have been pushed

            # Copy from the live issue so the recreation is faithful even if
            # the stored body is stale; fall back to the local record
            title = issue['issue_title']
            body = issue['issue_body'] or ''
            labels = []
            try:
                original = github_request('GET', f'/repos/{repo}/issues/{number}').json()
                title = original.get('title') or title
                body = original.get('body') or body
                labels = [l['name'] for l in original.get('labels', []) if l['name'] != TARGET_LABEL]
            except requests.exceptions.RequestException as e:
                errors.append(f'Could not fetch original issue #{number}, copying from local data: {e}')

            try:
                # Close the original and strip the demo labels so it drops out
                # of the dashboard catalog; the fresh copy carries them instead
                leftover_labels = [l for l in labels if l != CATALOG_LABEL]
                github_request('PATCH', f'/repos/{repo}/issues/{number}',
                               json={'state': 'closed', 'labels': leftover_labels})
                copy = github_request('POST', f'/repos/{repo}/issues',
                                      json={'title': title, 'body': body, 'labels': labels}).json()
                actions.append(f"Closed issue #{number} and recreated it as #{copy['number']}")
            except requests.exceptions.RequestException as e:
                errors.append(f'Could not close and recreate issue #{number}: {e}')

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM state_transitions')
        cursor.execute('DELETE FROM agent_sessions')
        cursor.execute('DELETE FROM issues')
        conn.commit()
        conn.close()

        return jsonify({
            'status': 'reset',
            'issues_reset': len(issues_to_reset),
            'actions': actions,
            'errors': errors
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/issues/<int:issue_id>/transitions', methods=['GET'])
def get_issue_transitions(issue_id):
    """
    Get state transition history for an issue
    """
    try:
        conn = get_db()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM state_transitions
            WHERE issue_id = ?
            ORDER BY timestamp ASC
        ''', (issue_id,))

        transitions = cursor.fetchall()
        conn.close()

        return jsonify({
            'transitions': [dict(t) for t in transitions]
        }), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/sessions/<session_id>/logs', methods=['POST'])
def update_session_logs(session_id):
    """
    Update logs for an active agent session
    """
    try:
        data = request.json
        logs = data.get('logs')

        conn = get_db()
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE agent_sessions
            SET logs = ?, updated_at = CURRENT_TIMESTAMP
            WHERE session_id = ?
        ''', (logs, session_id))

        conn.commit()
        conn.close()

        return jsonify({'status': 'updated'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200


init_db()
try:
    recover_inflight_work()
except Exception as e:
    print(f'[STARTUP] In-flight work recovery failed: {e}')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8001, debug=True)

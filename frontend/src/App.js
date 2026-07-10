import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8001';

// SQLite CURRENT_TIMESTAMP is UTC in "YYYY-MM-DD HH:MM:SS" form (no zone).
function parseUtc(ts) {
  if (!ts) return null;
  const d = new Date(ts.replace(' ', 'T') + 'Z');
  return isNaN(d.getTime()) ? null : d;
}

function formatElapsed(ms) {
  if (ms == null || ms < 0) return '—';
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${sec}s`;
  return `${sec}s`;
}

const STATUS_LABEL = {
  identified: 'Identified',
  pending: 'Queued',
  in_progress: 'In progress',
  completed: 'Completed',
  failed: 'Failed',
};

const STATUS_ORDER = { in_progress: 0, pending: 1, failed: 2, completed: 3, identified: 4 };

function App() {
  const [tickets, setTickets] = useState([]);
  const [meta, setMeta] = useState({ repository: 'felixbrock/superset', label: 'created by Devin' });
  const [now, setNow] = useState(Date.now());
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState(null);
  const [unreachable, setUnreachable] = useState(false);

  useEffect(() => {
    const fetchTickets = async () => {
      try {
        const res = await axios.get(`${API_URL}/tickets`);
        setTickets(res.data.tickets || []);
        setMeta({ repository: res.data.repository, label: res.data.label });
        setError(null);
        setUnreachable(false);
      } catch (e) {
        const msg = e.response && e.response.data && e.response.data.error;
        setError(msg || 'Cannot reach operator');
        setUnreachable(!e.response);
      } finally {
        setLoaded(true);
      }
    };
    fetchTickets();
    const poll = setInterval(fetchTickets, 4000);
    return () => clearInterval(poll);
  }, []);

  // Drive the live working-time counters.
  useEffect(() => {
    const tick = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(tick);
  }, []);

  const rows = tickets.map((t) => {
    const start = parseUtc(t.work_started_at);
    const end = parseUtc(t.completed_at);
    let workingMs = null;
    if (start) workingMs = (t.status === 'in_progress' ? now : (end ? end.getTime() : now)) - start.getTime();
    return {
      ...t,
      live: t.status === 'in_progress',
      workingMs,
      cvssSort: t.cvss == null ? -1 : t.cvss,
    };
  });

  const counts = rows.reduce((acc, r) => { acc[r.status] = (acc[r.status] || 0) + 1; return acc; }, {});
  const attempted = (counts.completed || 0) + (counts.failed || 0);
  const successRate = attempted > 0 ? Math.round(((counts.completed || 0) / attempted) * 100) : null;
  const active = rows.filter((r) => r.status === 'in_progress' || r.status === 'pending')
    .sort((a, b) => STATUS_ORDER[a.status] - STATUS_ORDER[b.status] || b.cvssSort - a.cvssSort);
  const allSorted = [...rows].sort((a, b) =>
    STATUS_ORDER[a.status] - STATUS_ORDER[b.status] || b.cvssSort - a.cvssSort);

  return (
    <div className="page">
      <div className="rule rule-a" />
      <div className="rule rule-b" />

      <header className="masthead">
        <div className="mark" aria-hidden>✻</div>
        <div className="masthead-text">
          <h1>Superset Security Operator</h1>
          <p>Autonomous issue resolution · <span className="repo">{meta.repository}</span></p>
        </div>
        <div className={`conn ${error ? 'conn-down' : 'conn-up'}`}>
          {error ? (unreachable ? 'operator offline' : 'degraded') : 'live'}
        </div>
      </header>

      {error && loaded && (
        <div className="banner">{error}</div>
      )}

      <section className="band">
        <span className="marker">01</span>
        <h2>Overview</h2>
        <div className="stats">
          <Stat n={rows.length} label="Tickets" />
          <Stat n={counts.in_progress || 0} label="In progress" accent />
          <Stat n={counts.pending || 0} label="Queued" />
          <Stat n={counts.completed || 0} label="Completed" />
          <Stat n={counts.failed || 0} label="Failed" />
          <Stat n={successRate == null ? '—' : `${successRate}%`} label="Success rate" />
        </div>
        {attempted > 0 && (
          <p className="stats-note">
            {(counts.completed || 0)} of {attempted} attempted ticket{attempted === 1 ? '' : 's'} remediated
            {' '}with a passing-test PR.
          </p>
        )}
      </section>

      <section className="band">
        <span className="marker">02</span>
        <h2>Active work</h2>
        {active.length === 0 ? (
          <p className="empty">No agent sessions running. Label a ticket{' '}
            <code>devin-fix</code> in {meta.repository} to dispatch one.</p>
        ) : (
          <div className="cards">
            {active.map((r) => (
              <article key={r.issue_number} className={`card ${r.live ? 'card-live' : ''}`}>
                <div className="card-top">
                  <a className="issue-ref" href={r.issue_url} target="_blank" rel="noreferrer">
                    #{r.issue_number}
                  </a>
                  {r.severity ? <SeverityBadge severity={r.severity} cvss={r.cvss} /> : <span className="sev sev-none">Unrated</span>}
                </div>
                <h3>{r.issue_title}</h3>
                <div className="card-meta">
                  <StatusDot status={r.status} />
                </div>
                <div className="timer">
                  <span className="timer-val">{formatElapsed(r.workingMs)}</span>
                  <span className="timer-lbl">{r.live ? 'agent working' : 'queued'}</span>
                </div>
                <LogFeed messages={r.messages} live={r.live} />
              </article>
            ))}
          </div>
        )}
      </section>

      <section className="band">
        <span className="marker">03</span>
        <h2>Tickets labeled “{meta.label}”</h2>
        {!loaded ? (
          <p className="empty">Loading…</p>
        ) : rows.length === 0 ? (
          <p className="empty">No tickets carry the “{meta.label}” label yet.</p>
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th className="c-num">Issue</th>
                  <th>Title</th>
                  <th className="c-sev">Severity</th>
                  <th className="c-status">Status</th>
                  <th className="c-time">Agent time</th>
                  <th className="c-tests">Tests</th>
                  <th className="c-review">Review</th>
                  <th className="c-pr">PR</th>
                </tr>
              </thead>
              <tbody>
                {allSorted.map((r) => (
                  <tr key={r.issue_number} className={r.live ? 'row-live' : ''}>
                    <td className="c-num">
                      <a href={r.issue_url} target="_blank" rel="noreferrer">#{r.issue_number}</a>
                    </td>
                    <td className="title-cell">
                      {r.issue_title}
                      {r.status === 'failed' && r.error_message && (
                        <div className="row-error" title={r.error_message}>{r.error_message}</div>
                      )}
                    </td>
                    <td className="c-sev">
                      {r.severity ? <SeverityBadge severity={r.severity} cvss={r.cvss} /> : <span className="unrated">—</span>}
                    </td>
                    <td className="c-status"><StatusDot status={r.status} /></td>
                    <td className="c-time">{r.workingMs != null ? formatElapsed(r.workingMs) : '—'}</td>
                    <td className="c-tests"><TestsCell r={r} /></td>
                    <td className="c-review"><ReviewCell r={r} /></td>
                    <td className="c-pr">
                      {r.pr_url ? (
                        <a href={r.pr_url} target="_blank" rel="noreferrer">#{r.pr_number || 'PR'}</a>
                      ) : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <footer className="foot">
        <span>Tickets are read live from GitHub by the “{meta.label}” label, so the
          list follows the issues even as their numbers change across resets.
          Severity is derived from the CVSS score in each issue via the NVD
          qualitative bands. Weigh the live agent working time against what
          such a fix usually costs an engineer.</span>
      </footer>
    </div>
  );
}

function Stat({ n, label, accent }) {
  return (
    <div className={`stat ${accent && n > 0 ? 'stat-accent' : ''}`}>
      <div className="stat-n">{n}</div>
      <div className="stat-l">{label}</div>
    </div>
  );
}

function SeverityBadge({ severity, cvss }) {
  return (
    <span className={`sev sev-${severity.toLowerCase()}`}>
      {severity}{cvss != null ? ` · ${cvss.toFixed(1)}` : ''}
    </span>
  );
}

function shortTime(ts) {
  const d = ts ? new Date(ts) : null;
  if (!d || isNaN(d.getTime())) return '';
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function LogFeed({ messages, live }) {
  const msgs = Array.isArray(messages) ? messages : [];
  if (msgs.length === 0) {
    return (
      <div className="logfeed">
        <div className="logfeed-head">Devin activity</div>
        <div className="logfeed-empty">{live ? 'Waiting for the first update…' : 'No activity yet.'}</div>
      </div>
    );
  }
  // Newest first, capped so the card stays compact.
  const shown = msgs.slice(-8).reverse();
  return (
    <div className="logfeed">
      <div className="logfeed-head">
        Devin activity{live && <span className="logfeed-live"> · live</span>}
      </div>
      <ul className="logfeed-list">
        {shown.map((m, i) => (
          <li key={`${m.timestamp || ''}-${i}`} className={`logline logline-${(m.type || '').includes('user') ? 'user' : 'devin'}`}>
            <span className="logline-time">{shortTime(m.timestamp)}</span>
            <span className="logline-msg">{m.message}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function TestsCell({ r }) {
  if (r.tests_passed === true) {
    return <span className="badge badge-ok" title={r.test_results || 'Tests pass'}>✓ Tests pass</span>;
  }
  if (r.tests_passed === false) {
    return <span className="badge badge-bad" title={r.test_results || 'Tests failing'}>✗ Tests fail</span>;
  }
  if (r.status === 'completed' && r.tests_added) {
    return <span className="badge badge-neutral" title={r.test_results || 'Tests added'}>Tests added</span>;
  }
  return <span className="muted">—</span>;
}

const REVIEW_MAP = {
  approved: ['ok', 'Approved'],
  passed: ['ok', 'Approved'],
  success: ['ok', 'Approved'],
  changes_requested: ['bad', 'Changes requested'],
  rejected: ['bad', 'Rejected'],
  failed: ['bad', 'Failed'],
  pending: ['warn', 'Pending'],
  in_progress: ['warn', 'Reviewing'],
  running: ['warn', 'Reviewing'],
};

function ReviewCell({ r }) {
  if (!r.review_status) return <span className="muted">—</span>;
  const key = String(r.review_status).toLowerCase();
  const [kind, label] = REVIEW_MAP[key] || ['neutral', r.review_status];
  return <span className={`badge badge-${kind}`} title={`Devin Review: ${r.review_status}`}>{label}</span>;
}

function StatusDot({ status }) {
  return (
    <span className={`status status-${status}`}>
      <span className="dot" />
      {STATUS_LABEL[status] || status}
    </span>
  );
}

export default App;

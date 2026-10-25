'use strict';
// Traffic monitor panel. Loaded after dashboard.js and shares its `selected`, `token` and `json`.
(() => {
  const $ = id => document.getElementById(id);
  let port = null, lastId = 0, busy = false, peers = new Map();
  const MAX_ROWS = 1500;
  const clock = iso => { const d = new Date(iso); return d.toLocaleTimeString([], {hour12: false}) + '.' + String(d.getMilliseconds()).padStart(3, '0'); };
  const div = (cls, text) => { const e = document.createElement('div'); e.className = cls; e.textContent = text; return e; };

  function reset() { lastId = 0; peers = new Map(); $('tap-out').replaceChildren(); }
  function side(peer) { if (!peers.has(peer)) peers.set(peer, Math.min(peers.size, 2)); return 'tap-side' + peers.get(peer); }

  function addEntry(e) {
    const out = $('tap-out');
    if (e.kind === 'event') { out.appendChild(div('tap-event', `${clock(e.time)}  [${e.level}] ${e.message}`)); return; }
    const row = document.createElement('div');
    row.className = 'tap-row ' + side(e.from) + (e.forwarded ? '' : ' tap-dropped');
    const to = e.forwarded ? e.to.join(', ') : 'NOT FORWARDED (no other client on this port)';
    row.appendChild(div('tap-head', `${clock(e.time)}  +${e.delta.toFixed(3)}s  ${e.from} → ${to}  ${e.length} B`));
    for (const f of e.frames) row.appendChild(div('tap-frame', f.summary));
    if (e.pending_bytes) row.appendChild(div('tap-frame', `…${e.pending_bytes} B of an unfinished frame buffered`));
    if ($('tap-hex').checked) {
      row.appendChild(div('tap-hex', e.hex + (e.truncated ? '  …(truncated)' : '')));
      if (/[^.]/.test(e.ascii)) row.appendChild(div('tap-ascii', e.ascii));
    }
    out.appendChild(row);
  }

  function status(data) {
    $('tap-status').textContent = data.active
      ? `Capturing port ${port}: ${Math.floor(data.seconds_remaining / 60)}m ${data.seconds_remaining % 60}s left` + (data.dropped ? ` · ${data.dropped} oldest dropped` : '')
      : `Not capturing port ${port}. Start a capture to see traffic.`;
    $('tap-start').disabled = data.active; $('tap-stop').disabled = !data.active;
  }

  async function poll() {
    if (busy) return; busy = true;
    try {
      if (port !== selected) { port = selected; reset(); }
      const data = await json(`/api/ports/${port}/tap?since=${lastId}&limit=500`);
      if (port !== selected) return;
      for (const e of data.entries) { lastId = Math.max(lastId, e.id); addEntry(e); }
      const out = $('tap-out');
      while (out.children.length > MAX_ROWS) out.firstChild.remove();
      if (data.entries.length && $('tap-scroll').checked) out.scrollTop = out.scrollHeight;
      status(data);
    } catch (err) { $('tap-status').textContent = 'Monitor unavailable: ' + err.message; }
    finally { busy = false; }
  }

  async function control(action, body) {
    try {
      await json(`/api/ports/${selected}/tap/${action}`, {method: 'POST', headers: {'X-Control-Token': token, 'Content-Type': 'application/json'}, body: JSON.stringify(body || {})});
      if (action === 'clear') reset();
      await poll();
    } catch (err) { alert('Monitor action failed: ' + err.message); }
  }

  $('tap-start').addEventListener('click', () => control('start', {minutes: Number($('tap-minutes').value)}));
  $('tap-stop').addEventListener('click', () => control('stop'));
  $('tap-clear').addEventListener('click', () => control('clear'));
  $('tap-download').addEventListener('click', () => { location.href = `/api/ports/${selected}/tap.txt`; });
  $('tap-hex').addEventListener('change', () => { reset(); poll(); });
  poll(); setInterval(poll, 1000);
})();

# Administration and scheduling

Open **http://localhost:8889** on this computer, or `http://<server-LAN-IP>:8889` on your home network. The real workspace uses `state/`. Port 8890 was an isolated browser-test copy, not the production workspace.

## Compact site list

The main page is now one alphabetical row per site: **Site, Status, Last success, Last record, Collect now, Expand**. Search and the Needs attention filter are at the top. The sidebar, cards, availability diagrams, import controls and workspace/activity pages are removed.

Expand a site to edit its settings inline, enable/disable routine collection, connect manually, control backfill or stop its active attempts. Table results are listed underneath. Recent events and technical logs stay collapsed unless opened. **Clear displayed history only hides finished events in this browser**; it does not delete jobs, logs, collection data or live status.

Last record is an actual committed record timestamp (with its record number on hover/focus), not the time range a collection checked. Measurement is the primary table if configured; otherwise the first configured table is used. Last success refers to that primary table's latest successful recent collection. Other table failures are still shown as Partial failure. A successful connection check does not invent a measurement timestamp or a data-collection success.

Connected/Downloading stages come from worker callbacks. Connection capacity is fixed at **4** in the backend and is not editable from the webpage or configuration API. The hourly/retry rules below are unchanged. Data and credentials remain in the existing state directory.

## First login

The initial page asks for a one-time setup code and your chosen administrator password (at least 12 characters). Read the code from `state/setup-code.txt` on the server. The code is deleted after setup. There is no default administrator password. Authentication credentials are never exported with settings.

Both services are running locally with **20 configurations enabled; LEE remains unconfigured and disabled**. The site list is seeded from reference folder names and matching LoggerNet backup entries. ROS has the already verified connection and serial. PakBus destinations were set to the TCP ports at the user’s request, with LDW updated to aws-loggernet-server.ddns.net:623 / destination 623 and UMR retaining its endpoint with destination 1. BRD serial 12483 comes from the reference headers. LEE has no exact backup mapping. Review settings before enabling.

## Local launch and recovery

```sh
pip install -r requirements.txt
python web.py --state state --host 0.0.0.0 --port 8889
# In a second terminal:
python scheduler.py --state state
```

Use the same absolute `--state` directory for both processes. On first use, `python web.py --state state --import-settings settings.json` can seed legacy settings; migrated sites start disabled. The existing `app.py serve --state state` also uses the new window-aware supervisor. The old `once`, `inspect`, explicit dates, and `--all` commands remain advanced manual tools and do not apply webpage scheduling restrictions. Prefer webpage jobs for routine operation.

To recover a lost password, stop the web service, then run `python web.py --state state --reset-password`. This invalidates sessions and creates a fresh one-time setup code. Restart the service normally afterward.

The source of truth is `state/admin.sqlite3`, not the original root `settings.json`. A settings mirror is written to `state/settings.json`. Configuration revisions are retained in the backend; concurrent edits require reopening the inline editor rather than silently overwriting one another. Download data is stored in `state/collected/` by default, separately from reference files.

## Collection policy

| Sites | Default availability in Brisbane time |
|---|---|
| ROS, MRM, WFC, BRD, JRD | Unrestricted; connection failures still retry |
| FDF, FDU | Minutes 00–30 each hour |
| Others | Minutes 00–15 each hour |

**Automatic collection is aligned to the start of every hour for every enabled site.** Attempts are eligible from `:00` until `:10`, in each site's timezone. A successful table waits for the next hour. Failures retry on one-minute boundaries while time remains; an attempt already running is never overlapped. No automatic attempt continues beyond `:10`. An expired waiting automatic request is skipped, then rescheduled at the next hour. Restarting during the ten-minute period catches up; restarting afterward waits for the next hour.

The physical online windows above remain editable and can shorten automatic availability if a site powers off sooner. Four connections can run at once, and one at a time per shared gateway (normally host:port). All sites are scheduled at `:00`; with more than four sites, some must wait for one of the four connections to become free. That waiting list is what the activity view calls the queue. Sites using one radio/router can be assigned the same gateway group.

**Connect now** is a manual identity/clock/table check, then disconnects. **Collect now** manually downloads recent records. Both work outside the automatic timetable and physical-window assumptions, and can be used while routine collection is disabled if the connection details are valid. They take priority over waiting automatic requests, but never exceed four concurrent connections or overlap another connection to the same site/gateway. They do not interrupt jobs already running. LEE still requires configuration before either action is available.

Recent collection uses a separate watermark per table, initially the previous hour, then resumes with an overlap. Connection setup has a hard **20-second maximum**, including PakBus response/identity verification. Only one handshake is sent; the library reconnect/ping retry loop is removed. A manual connection request is not retried automatically. After successful connection, downloads retain their separate 60-second maximum, shortened by the automatic/physical closing deadline. Scheduled one-minute retries until :10 remain separate from retries inside a connection attempt. The site editor exposes retry spacing (60 seconds) and automatic closing minute (10); all current sites have those values. The legacy collection-interval field is retained for compatibility, but automatic cadence is now fixed to the hourly clock.

Historical work must be enabled with Backfill. It waits for a recent baseline and yields to eligible current work. Defaults cap each site to 1,000 received records and 90 seconds per hour, in slices of at most 45 seconds; overlaps conservatively count against the record allowance. All limits are editable.

Backfill seeks the first available retained records, then limits each slice's accepted date coverage to at most one day after that first record. Empty periods are skipped by the next successful ranged query rather than issuing thousands of empty daily requests. Partial slices checkpoint committed timestamps; complete slices advance verified coverage. Historical checkpoints never advance recent-data watermarks. New data always retains priority. No full-history restart is required after a dropped connection.

Every worker has its own hard deadline, including if the scheduler disappears. Restart recovery reserves unfinished workers' site/capacity until their recorded deadline expires, preventing duplicate connections. A single supervisor lock prevents multiple schedulers using the same state. The original collector's site lock and transactional journal continue to protect downloaded records.

Pause history cancels queued historical work and requests cancellation of a running historical job. Cancel from Activity stops an individual job; committed records remain. Configuration changes cancel obsolete queued snapshots; already running work keeps its original configuration until completion or explicit cancellation.

## Output and boundaries

Collection data uses the existing SQLite journal and compatible TOA5 export. Removing a site configuration does not delete its files, schemas, or journal. Renaming a site code starts files under the new code; old files remain. Changing the output directory does not move existing journals or files; perform a deliberate migration while stopped.

**Do not run the supplied preprocessing scripts against production paths yet.** Their hard-coded source paths and byte-offset history state still require the separate compatibility work described in the planning conversation. Array-field collection remains explicitly unsupported by the existing exporter until its mappings are validated. The admin interface can test identity/table metadata without assuming those tables are collectable.

The web server uses plain HTTP for trusted home-LAN use, with a hashed administrator password, CSRF tokens, HttpOnly/SameSite cookies and login throttling. Do not port-forward 8889 to the public Internet; use a private VPN for access away from home. Backups should include the entire stopped state directory, which contains credentials, configuration, jobs and checkpoints. Use SQLite's backup API for live backups. Exports and revision responses redact PakBus security codes.

## DietPi installation

Copy the Python files, `templates/`, `static/`, `deploy/`, requirements, and the state you intend to migrate into `/opt/campbell-collector`. Do not copy the temporary browser-test workspace. Stop local services before copying state; copy the full data directories, not just a live SQLite file.

```sh
apt-get update
apt-get install -y python3 python3-venv tzdata
useradd --system --home-dir /var/lib/campbell-collector --shell /usr/sbin/nologin campbell
install -d -m 700 -o campbell -g campbell /var/lib/campbell-collector
cd /opt/campbell-collector
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

If migrating the prepared `state/`, copy its contents into `/var/lib/campbell-collector`, give ownership to `campbell`, then update the saved output directory in the webpage to `/var/lib/campbell-collector/collected` before enabling anything. Alternatively initialise a fresh configuration with the import-settings option while running as `campbell`. Windows absolute output paths must not be carried into active Linux settings.

```sh
cp deploy/campbell-admin.service deploy/campbell-collector.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now campbell-admin campbell-collector
journalctl -u campbell-admin -u campbell-collector -f
```

Read the setup code on the Pi if performing fresh administrator setup. Keep the source directory readable but use `/var/lib/campbell-collector` for all runtime writes. The service configuration permits writes only there. Physical DietPi deployment has not been performed from this laptop session.

## Progress checkpoints

The implementation, tests and operational state are ordinary files. Closing this chat or reaching an AI usage limit does not erase them. See `CHECKPOINT.md` for completed work, verification evidence and remaining deployment/fleet tasks. No AI service is needed for normal operation after installation.

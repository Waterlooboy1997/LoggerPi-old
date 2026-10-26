# Administration and scheduling

Open **http://localhost:8889** on this computer, or `http://<server-LAN-IP>:8889` on your home network. The real workspace uses `state/`. Port 8890 was an isolated browser-test copy, not the production workspace.

## Compact site list

The main page is now one alphabetical row per site: **Site, Status, Last success, Last record, Collect now, Expand**. Search and the Needs attention filter are at the top. The sidebar, cards, availability diagrams, import controls and workspace/activity pages are removed.

Expand a site to edit its settings inline, enable/disable routine collection, connect manually, download its history or stop its active attempts. Table results are listed underneath. Recent events and technical logs stay collapsed unless opened. **Clear displayed history only hides finished events in this browser**; it does not delete jobs, logs, collection data or live status.

Last record is an actual committed record timestamp (with its record number on hover/focus), not the time range a collection checked. Measurement is the primary table if configured; otherwise the first configured table is used. Last success refers to that primary table's latest successful recent collection. Other table failures are still shown as Partial failure. A successful connection check does not invent a measurement timestamp or a data-collection success.

Connected/Downloading stages come from worker callbacks. Connection capacity is fixed at **4** in the backend and is not editable from the webpage or configuration API. Data and credentials remain in the existing state directory.

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

Use the same absolute `--state` directory for both processes. On first use, `python web.py --state state --import-settings settings.json` can seed legacy settings; migrated sites start disabled. The existing `app.py serve --state state` also uses the same hourly supervisor. The old `once`, `inspect`, explicit dates, and `--all` commands remain advanced manual tools and do not apply webpage scheduling restrictions. Prefer webpage jobs for routine operation.

To recover a lost password, stop the web service, then run `python web.py --state state --reset-password`. This invalidates sessions and creates a fresh one-time setup code. Restart the service normally afterward.

The source of truth is `state/admin.sqlite3`, not the original root `settings.json`. A settings mirror is written to `state/settings.json`. Configuration revisions are retained in the backend; concurrent edits require reopening the inline editor rather than silently overwriting one another. Download data is stored in `state/collected/` by default, separately from reference files.

## Collection policy

**Automatic collection starts at the top of every hour** (`:00` in each site's timezone) for every enabled site. Each site has two settings:

- **Attempts each hour** (default 10)
- **Seconds between attempts** (default 60, 10–3600): how long to wait after a failed attempt before trying again

A successful collection waits for the next hour. Failed attempts retry until one succeeds or the hour's attempts are used up. Every attempt must be able to start within the hour, so (attempts − 1) × seconds between them must be under 3600. Automatic work for an hour never runs into the next hour. A request still waiting for a free connection when the hour ends is skipped. If the scheduler restarts (or a site is enabled) partway through an hour, it only starts that hour's first attempt while attempts × spacing from `:00` has not yet passed; otherwise it waits for the next hour. An attempt already running is never overlapped.

Four connections can run at once, and one at a time per shared gateway (normally host:port). With more than four sites, some wait for a free connection. Sites using one radio/router can be assigned the same gateway group.

**Connect now** is a manual identity/clock/table check, then disconnects. **Collect now** manually downloads recent records. Both can be used at any time, including while routine collection is disabled, if the connection details are valid. They take priority over waiting automatic requests, but never exceed four concurrent connections or overlap another connection to the same site/gateway. They do not interrupt jobs already running.

Recent collection uses a separate watermark per table, initially the previous hour, then resumes with an overlap. Connection setup has a hard **20-second maximum**, including PakBus response/identity verification. Only one handshake is sent. After a successful connection, an automatic or manual recent download has a 60-second maximum, which is also cut short by the end of the hour for automatic attempts.

**Download history** is a manual, one-off action. It downloads everything the logger still holds from before the point where recent collection started, in one continuous connection, until it finishes or you press **Stop history download**. There are no hourly record/time budgets and nothing else disconnects it, apart from a 24-hour safety limit for a hung worker. While it runs, that site's hourly collection waits (one connection per site), and it uses one of the four connection slots. Run **Collect now** (or let one hourly collection succeed) first, because history needs to know where recent data starts. Every received batch is checkpointed, so a stopped or dropped download resumes where it left off. Once history is complete the button is disabled.

### Adding and removing sites

Settings saves can't add or remove sites. **Add a site…** (below the list) opens a form; to confirm, you type the new site code. New sites always start with routine collection off. Use Connect now, then turn routine collection on.

**Remove site** is in a collapsed section at the bottom of each expanded site. It requires routine collection to be off and no queued or running work, and you must type the site code. Removing a site deletes only its settings. Its files, schemas and journal are kept.

### Removed settings

The online-from/until minutes, the hourly closing minute, "almost always online", the physical PakBus source/destination addresses, the legacy collection interval and the history budget settings (records/seconds per hour, job seconds) no longer exist. Old values are dropped automatically the next time settings are loaded and saved. The number of attempts each hour is derived from the old closing minute ÷ retry spacing (10 for the current `:10`/60 s setup). Packet timeout, the 60-second download limit and overlap keep their defaults but are no longer shown in the editor.

Every worker has its own hard deadline, including if the scheduler disappears. Restart recovery reserves unfinished workers' site/capacity until their recorded deadline expires, preventing duplicate connections. A single supervisor lock prevents multiple schedulers using the same state. The original collector's site lock and transactional journal continue to protect downloaded records. Configuration changes cancel obsolete queued snapshots; running work keeps its original configuration until completion or explicit cancellation.

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

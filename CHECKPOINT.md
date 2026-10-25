## Latest connection change — stop after 20 seconds

Connection setup now has a dedicated 20-second worker watchdog, separate from download time limits. The connection adapter sends one PakBus hello and never reconnects/resends the handshake. A missing logger produces a failed job with "Connection failed after 20 seconds". A failed manual connection is shown as Failed rather than Retrying. The scheduled hourly one-minute retries remain unchanged; manual attempts are one-shot.

Validation: an accepting but nonresponsive local TCP endpoint failed after 20.32 seconds, with exactly one TCP connection and one hello frame. Live ROS identity/metadata connection succeeded in 6.56 seconds. Test coverage is in test_connection.py; all results are in validation/connection-tests.log. No data was deleted.

## Latest UI change — compact site list

The dashboard is now a compact alphabetical list with Site, live Status, primary-table Last success, actual Last record timestamp, Collect now and Expand. Settings edit inline under each row. Timelines, cards, branding, sidebar, import controls, workspace settings and the separate activity page were removed. Technical logs and recent events are collapsed; Clear displayed history hides finished events in browser storage only. Capacity is fixed at 4 in control.py and scheduler.py.

Worker progress callbacks report Connecting / Connected / Downloading; API summaries use indexed job history and actual record rows, independently of the 100-event display limit. Partial failures stay visible. 40 automated tests pass (validation/list-tests.log). Browser QA on an isolated preview verified inline save, unsaved input and expansion preservation during polling, attention filtering, clear-history without deleting jobs, 21-row desktop fit at 1366×900, and phone layout at 390×844 without horizontal overflow.

The existing 20 enabled sites, LEE's disabled/unconfigured state, authentication, collection data, :00–:10 hourly attempt rule and one-minute retries are preserved. No active collections were present when the web/scheduler processes were restarted. A consistent pre-switch admin database backup was saved under state/backups/. Physical Pi deployment and array-field/preprocessor compatibility work remain outstanding as noted below.

## Latest operational change — hourly collection and manual controls

Supersedes the earlier disabled-site and two-connection notes below. There are now 20 enabled sites; LEE remains disabled and unconfigured. Capacity is **4**. Every enabled site is scheduled on the hour, with failures retried every minute until **:10**, then deferred to the next hour. Manual **Connect now** and **Collect now** bypass the automatic timetable and can be used on valid disabled configurations. Automatic jobs are tagged with their scheduled hour and expire at the retry-period deadline; manual requests remain independent. Physical availability windows still constrain automatic work.

LDW: aws-loggernet-server.ddns.net:623, PakBus 623. UMR: existing endpoint, PakBus 1. Other configured PakBus destinations match TCP ports. BRD serial: 12483 from reference headers. No historical backfills are enabled. The web and scheduler services were restarted with these changes. Timing/concurrency tests: see validation/hourly-tests.log.

# Project checkpoint — 25 October 2026

## Current state

The port 8889 administration webpage and window-aware scheduler are implemented and running locally. Open http://localhost:8889. The browser has been switched from the isolated port 8890 preview to the real setup page.

Administrator setup is intentionally pending. The user chooses the password; the initial one-time code is in `state/setup-code.txt`. No default password has been set. After setup the code file is deleted.

There are 21 prepared site configurations and **zero enabled sites** in `state/admin.sqlite3`. The main workspace has no collection jobs. Reference files and live OneDrive paths were not changed. No fleet-wide backfill was started.

## Implemented

- Site editor, cloning/removal, connection/routing details, editable online windows and budgets.
- LoggerNet backup preview with disabled imports and unresolved settings explicitly marked. Port numbers are not assumed to equal PakBus destination addresses.
- Searchable site cards, hourly window diagrams, capacity status, activity/log view, collect/test/backfill/pause/cancel controls.
- Admin password setup/login, CSRF protection, redacted credential exports, versioned configuration and server-side revision restore.
- Two-service architecture: web.py (Waitress/Flask) and scheduler.py; worker.py performs bounded work in separate processes.
- Recent-data priority, shared gateway limits, two concurrent workers by default, window guards, capped retries, record/time-limited backfills, independent recent/history cursors and deadline watchdogs.
- Restart recovery, scheduler/site locks, durable SQLite queue/checkpoints, indexed scheduling queries.
- Existing app.py serve delegates to the new supervisor; advanced once/inspect/date commands remain available.
- DietPi systemd units and ADMIN.md installation/handover instructions.

## Availability defaults

- ROS, MRM, WFC, BRD, JRD: unrestricted, almost always online.
- FDF and FDU: minutes 00–30 of each hour.
- Others: minutes 00–15.
- Brisbane time; windowed attempts start ten seconds after opening and stop twenty seconds before closing.
- M2M was confirmed by the user to mean MRM.

## Verification evidence

- `python -m unittest -v`: **30 tests pass**, including the original ROS tests, authentication, revision/secrets handling, concurrent edits, twenty-site queue progress, shared gateways, retry limits, cancellation, closing windows, restart reservations, backfill budgets, daily slice boundaries and separate checkpoints.
- Output: `validation/admin-tests.log`.
- Live bounded ROS recent job succeeded through the new scheduler: one record at 2026-10-25 03:59:00. Evidence is under `validation/live-admin/`; this is separate from the real workspace.
- Browser QA logged into a disposable copy, searched ROS, edited a label and verified that it saved. Screenshot: `validation/admin-preview.png`.
- Temporary preview on port 8890 was stopped. Real port 8889 and the disabled-site scheduler were restarted with the final code.
- Responsive CSS is included; phone viewport/device behaviour has not been independently verified. Browser QA used the desktop in-app browser.

## Remaining deployment/fleet work

1. User completes administrator setup on 8889.
2. Review unresolved PakBus destinations, serials, folder mappings and table support before enabling sites. ROS is already verified. LEE has no exact backup mapping.
3. Physical DietPi deployment has not been performed; follow ADMIN.md, migrate full state while stopped and correct Windows output paths for Linux.
4. Validate one additional always-online site and a restricted-window site before fleet rollout. No unreviewed endpoints were probed.
5. The separate preprocessing/merger compatibility stage is still outstanding: hard-coded production paths and byte-offset history state need isolated tests before integration. Do not run those scripts against live paths.
6. Array-field export remains explicitly unsupported pending validation of naming/storage mappings. The web editor does not remove that collector limitation.

Local processes are ordinary manually launched Python processes, not installed Windows startup services. If stopped, relaunch `python web.py --state state` and `python scheduler.py --state state` from this project. See ADMIN.md for safe state backup, password reset and migration. No AI credits or open chat are required for those programs to operate.

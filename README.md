# Campbell collector for DietPi

> **New administration interface:** Open http://localhost:8889. See [ADMIN.md](ADMIN.md) for setup, site editing, availability windows, job controls and the updated two-service DietPi deployment. `app.py serve` now delegates to the window-aware supervisor. Legacy one-shot commands below remain advanced manual tools and bypass dashboard windows.

A small Python service, with JSON settings and a local SQLite journal. The supplied ROS source files are never modified by the default configuration. Python 3.9 or newer is required.

## Quick start

From this directory:

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python -m unittest -v
python app.py inspect --site ROS
python app.py once --site ROS
```

Windows activation is `.venv\Scripts\Activate.ps1`. The dependency versions match the installed working library (PyCampbellCR1000 0.4 and pylink 0.3.3). DietPi installation and hardware operation still need validation on your Pi. The collector does not change the underlying library. It corrects two observed decoding quirks at export: one-character metadata returned as integer ASCII codes, and reserved FP2 non-finite encodings.

`settings.json` starts with ROS TCP/PakBus 364, expected serial 13437, hourly collection, five-minute retries, Brisbane time, ten-second packet timeout, and a five-minute maximum run. The outer process enforces the maximum even if the library retries internally. Keep any PakBus security code out of source control if you add one.

Commands:

```sh
python app.py once --site ROS --all --output validation/backfill --max-run-seconds 3600
python app.py once --site ROS --start "2026-10-20 00:00:00" --end "2026-10-25 00:00:00"
python validate.py ROS/ROS_Measurement.dat validation/backfill/ROS_Measurement.dat --report validation/backfill/comparison.json
python app.py serve
```

All dates are logger-local wall time, without a UTC offset. `--all` asks for the logger's retained history, not history that has already been overwritten. Use `--max-run-seconds 3600` (or longer) for slow full downloads; the service retains its shorter normal limit. Completed batches survive a timeout; subsequent normal collections resume from committed progress with an overlapping window. Retrying `--all` revisits the retained range and deduplicates it. First normal collection, without imported data, reads the previous hour. `--output` chooses an independent data/journal directory.

`inspect` verifies serial number and saves current metadata without reading measurement records. Clock drift is measured against Brisbane time and logged; clocks are never adjusted. Ensure the Pi's system time is synchronized. The configured model is CR300 because that is the model in the ROS reference header; OS strings alone can say CR310 for this logger family.

## Files and recovery

Each output directory contains:

- `ROS_Measurement.dat`: the current generation, with four TOA5 headers and CRLF line endings.
- `ROS_Measurement.generation-N.dat`: previous table/program or record-reset generations.
- `ROS_Measurement.schemas.json`: schema history, including logger storage types and program identity.
- `ROS.metadata.json`: most recently observed identity, clock drift, and table definitions.
- `collector.sqlite3`: accepted records, original decoded values (`records.raw`), conflicts, schemas and committed progress.

IEEE4 values use seven significant digits, IEEE8 uses sixteen, with halfway cases rounded away from zero as observed in LoggerNet output; FP2 values retain their decoded precision; numeric sentinels remain numeric. Non-finite values are quoted `NAN`, `INF`, or `-INF`. Strings and timestamps are quoted. Arrays are rejected explicitly until a site's required TOA5 array naming has been validated. No units or values are invented to fill missing fields.

Records are committed by batch before progress advances. TOA5 files are regenerated to temporary files, flushed, then atomically replaced. A failed export is repaired on the next successful run, including an empty result. The SQLite journal is the authoritative state after takeover; do not delete it or independently edit exported files. Back up the directory while stopped, or use SQLite's backup API while running; copying only a live database file can miss its WAL.

A repeated key with a different formatted value is retained in `conflicts`, logged, and leaves the accepted value unchanged. Raw decoded values are retained in SQLite, rather than in a second CSV. Schema/program changes and record-number decreases at later timestamps create generations. A backwards clock jump is not automatically merged; investigate the reported collection-range error. Hardware replacement must be explicitly confirmed by updating `expected_serial`.

## Taking ownership of main files

1. Validate in the default `validation` directory first. Review metadata, formatted samples, and `comparison.json`. A report with zero overlap does not establish historical value agreement.
2. Stop LoggerNet output and disable any merger that writes the same canonical files. This collector requires exclusive ownership; its site lock cannot coordinate with unrelated programs.
3. Create an empty production directory. Copy the existing `ROS_Measurement.dat` there, retain a separate backup, and set `output_directory` to that directory. Do not copy the validation database into this directory.
4. Run `once`. Existing files with exactly matching TOA5 headers are imported transactionally before collection. A mismatching header stops takeover: review the current logger metadata and archive/migrate the old file explicitly rather than changing its headers blindly.
5. Verify output, then enable the service. After takeover, external manual/eagleio merges need a future journal-aware importer; the old merger must not write these files.

Do not point development runs at the supplied `ROS` directory. The initial serial check prevents accepting the other logger that appeared in the first CSV.

## DietPi systemd setup

Copy this project (excluding ROS test data and validation output) to `/opt/campbell-collector`. On DietPi, run as root or with sudo:

```sh
apt-get update
apt-get install -y python3 python3-venv tzdata
useradd --system --home-dir /var/lib/campbell-collector --shell /usr/sbin/nologin campbell
install -d -o campbell -g campbell /var/lib/campbell-collector
cd /opt/campbell-collector
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Set `output_directory` in `/opt/campbell-collector/settings.json` to `/var/lib/campbell-collector` (or adjust both this path and `ReadWritePaths` in the service). Keep the project readable by `campbell`; keep settings containing credentials restricted to root and that group. Perform the takeover steps above if importing main files.

```sh
cp deploy/campbell-collector.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now campbell-collector
journalctl -u campbell-collector -f
```

Use `systemctl stop campbell-collector` before manual work in the production directory. Settings are read at startup; restart after editing. Add another object to `sites` with a unique safe site name, correct serial, host/port, PakBus destination/source addresses, tables, timezone, intervals and delays. The default source is 2050, matching the working library; optional `pakbus_physical_address` and `pakbus_source_physical_address` override physical routing addresses if required. Each site gets a separate process, timeout and retry schedule; an offline site does not block another. Delays and intervals are seconds. Retried jobs wait the configured retry interval after completion; successful jobs wait the collection interval. This is an interval scheduler, not alignment to the wall-clock hour.

## Validation

`python -m unittest -v` runs without contacting a logger. It checks the supplied ROS CSV against an exact expected TOA5 line and tests journal recovery, import, conflicting duplicates, repeated historical downloads across resets, generation files, locks, identity rejection, independent workers, timeouts and service retry scheduling. `validate.py` compares textual values in overlapping timestamp/record keys and reports header differences, coverage, typical interval, record-number gaps and resets. Timestamp-gap candidates can reflect legitimate historical interval changes and are reported separately. Differences are reported, never corrected to match the reference.

# ROS validation — 25 October 2026

Implemented the collector in `app.py`, with `settings.json`, tests, comparison utility, deployment service, and setup instructions in `README.md`.

## Verified live

- ROS at TCP port 364, PakBus destination 364, serial 13437.
- CR300, OS CR310.Std.10.07, program CPU:P25_OPUS_ROS_V5.cr300, program signature 3640.
- Retrieved 61,870 unique records, from 2023-08-14 15:59:50 through 2026-10-25 01:59:00.
- All 57,949 unique reference records are present and match exactly, including the complete text of each data line and all four headers.
- Recovered 3,921 records absent from the supplied reference. The reference has 57,952 rows, including three identical repeated rows across two keys; the collector deduplicates these.
- No gaps in the downloaded record-number sequence and no record resets in the retained ROS dataset.
- No conflicting duplicate records in the collection journal; SQLite integrity check passed.
- A subsequent normal collection received two overlapping records, added no duplicates, and retained the same 61,870-record total.
- Logger clock was about five seconds behind the computer. No clock changes were made.

The long historical transfer encountered two missing PakBus responses, surfaced by library 0.4 as `KeyError: RecData`. Both runs resumed from committed SQLite checkpoints. No historical records were discarded or replaced to match the reference. The service automatically schedules retries on these failures.

## Formatting findings

In addition to the four TOA5 headers, timestamp/record naming, quoting and float precision, live validation found three details handled by this implementation:

- Library 0.4 returns the one-character unit `V` as integer ASCII code 86.
- Reserved FP2 non-finite bit patterns are returned as ordinary numbers; export recognizes those patterns while preserving decoded inputs in SQLite. Ordinary numeric sentinels such as -7999 remain numeric.
- LoggerNet rounds IEEE halfway cases away from zero; Python's default significant-digit formatting uses a different tie rule. The observed value 1.4453125 must export as 1.445313.

Decoded values and observed table definitions are retained for inspection. The formatter also round-trips every supplied reference row exactly.

## Automated checks and artifacts

13 tests passed, covering output precision/quoting, the full reference text roundtrip, import, duplicate conflicts, transaction rollback, export recovery, table changes, resets and historical routing, locks, serial checks, network interruption/resume, worker isolation, timeouts and scheduled retries.

- Output: `validation/backfill/ROS_Measurement.dat`
- Journal and raw decoded values: `validation/backfill/collector.sqlite3`
- Comparison: `validation/backfill/comparison.json`
- Current schema history: `validation/backfill/ROS_Measurement.schemas.json`
- Input CSV converted using live metadata: `validation/ROS_sample.dat`
- Test output: `validation/tests.log`
- Incremental collection log: `validation/incremental.log`

The original supplied ROS files were left untouched. Default settings still target an isolated validation directory. DietPi deployment files are provided, but the service has not been installed or tested on the physical Pi. Follow the production takeover steps in `README.md`; the existing merger must not independently write files owned by the collector.

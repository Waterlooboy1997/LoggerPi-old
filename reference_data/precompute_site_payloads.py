#!/usr/bin/env python3

#command for local PC: python "C:\Users\jc321293\Desktop\Github\RTWQ-data-display\precompute_site_payloads.py" --base-path "C:\Users\jc321293\Desktop\Github\RTWQ-data-display" --output-dir "C:\Users\jc321293\Desktop\Github\RTWQ-data-display\processed" --retention-days 60 --log-level DEBUG

"""Precompute measurement payloads for all configured sites."""
from __future__ import annotations

import argparse
import csv
import errno
import json
import logging
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

import time

import pandas as pd

from config import CONFIG
from toa5_slim import update_history_slim
from utilities import MAX_ROWS_PER_FILE, read_toa5_header

LOCK_FILENAME = ".precompute_site_payloads.lock"
SUMMARY_WINDOW_HOURS = 48
NNO3_ALIASES = ("NNO3", "OPUS_NNO3", "NICO-NNO3", "NICO_NNO3")
TSSEQ_ALIASES = ("OPUS_TSSEQ",)
WATER_LEVEL_ALIASES = ("WATERLEVEL", "WATERLEVEL_M", "WATERLEVEL_MM")
ATOMIC_REPLACE_MAX_ATTEMPTS = 6
ATOMIC_REPLACE_BACKOFF_SECONDS = 0.05
WINDOWS_LOCK_WINERRORS = {32, 33}


@dataclass
class SummaryRecord:
    site: str
    label: str
    latest_timestamp: Optional[str]
    time_since: str
    latest_values: Dict[str, Optional[float]]
    max_values: Dict[str, Optional[float]]
    water_level_trend: str
    status: str = "ok"
    message: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        metrics: Dict[str, Dict[str, Optional[float]]] = {}
        for key in sorted(set(self.latest_values.keys()) | set(self.max_values.keys())):
            metrics[key] = {
                "latest": self.latest_values.get(key),
                "max": self.max_values.get(key),
            }

        payload: Dict[str, object] = {
            "site": self.site,
            "label": self.label,
            "status": self.status,
            "latest_timestamp": self.latest_timestamp,
            "time_since": self.time_since,
            "latest": self.latest_values,
            "max_48h": self.max_values,
            "metrics": metrics,
            "water_level_trend": self.water_level_trend,
            "trend": self.water_level_trend,
        }
        if self.message:
            payload["message"] = self.message
        return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Precompute site measurement payloads so the web app can serve static JSON."
    )
    parser.add_argument(
        "--base-path",
        type=Path,
        default=Path(CONFIG.get("BASE_PATH", ".")),
        help="Root directory containing <site>/<site>_Measurement.dat files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("processed"),
        help="Directory where processed JSON files should be written.",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=90,
        help="Number of days of measurements to retain in the precomputed payloads.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    parser.add_argument(
        "--interval-minutes",
        type=float,
        default=0,
        help=(
            "If greater than zero, keep the process alive and rerun preprocessing "
            "after the specified number of minutes."
        ),
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def acquire_lock(lock_path: Path) -> Optional[int]:
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        logging.debug("Lock acquired at %s", lock_path)
        return fd
    except FileExistsError:
        logging.error("Lock file %s exists; another instance may be running.", lock_path)
        return None


def release_lock(fd: Optional[int], lock_path: Path) -> None:
    if fd is None:
        return
    try:
        os.close(fd)
    finally:
        try:
            lock_path.unlink(missing_ok=True)
        except Exception:
            logging.warning("Failed to remove lock file at %s", lock_path)


def read_header_row(path: Path) -> List[str]:
    try:
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle)
            row = next(reader, [])
            return [cell.strip().strip('"') for cell in row]
    except Exception as exc:
        logging.warning("Unable to read header row from %s: %s", path, exc)
        return []


def _find_column(columns: List[str], target_upper: str) -> Optional[str]:
    for column in columns:
        if isinstance(column, str) and column.upper() == target_upper:
            return column
    return None


def _format_timedelta(delta: pd.Timedelta) -> str:
    minutes = int(delta.total_seconds() // 60)
    if minutes <= 0:
        return "Just now"
    if minutes < 60:
        return f"{minutes} minute{'s' if minutes != 1 else ''} ago"
    hours, rem = divmod(minutes, 60)
    if minutes < 24 * 60:
        return f"{hours} hour{'s' if hours != 1 else ''} and {rem} minute{'s' if rem != 1 else ''} ago"
    days, rem = divmod(minutes, 24 * 60)
    hours = rem // 60
    return f"{days} day{'s' if days != 1 else ''} and {hours} hour{'s' if hours != 1 else ''} ago"


def _safe_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, str) and value.strip() == "":
            return None
        if isinstance(value, str) and value.strip().upper() == "NAN":
            return None
        result = float(value)
        if pd.isna(result):
            return None
        return result
    except Exception:
        return None


def _load_summary_frame(dat_path: Path, columns: List[str]) -> pd.DataFrame:
    nno3_col = next((c for c in columns if c.upper() in NNO3_ALIASES), None)
    tsseq_col = next((c for c in columns if c.upper() in TSSEQ_ALIASES), None)
    water_col = next((c for c in columns if c.upper() in WATER_LEVEL_ALIASES), None)

    time_col = next((c for c in columns if c.upper() == "TIMESTAMP"), None)
    selected_cols = [col for col in [time_col, nno3_col, tsseq_col, water_col] if col is not None]

    df_typed = pd.read_csv(
        dat_path,
        header=None,
        names=columns,
        skiprows=4,
        quotechar='"',
        na_values=["NAN", "NaN", "nan", "", " "],
        keep_default_na=True,
        engine="python",
        usecols=selected_cols,
    )

    if len(df_typed) > MAX_ROWS_PER_FILE:
        df_typed = df_typed.tail(MAX_ROWS_PER_FILE).reset_index(drop=True)
    else:
        df_typed = df_typed.reset_index(drop=True)

    for col in [nno3_col, tsseq_col, water_col]:
        if col and col in df_typed.columns:
            df_typed[col] = pd.to_numeric(df_typed[col], errors="coerce")

    if time_col and time_col in df_typed.columns:
        df_typed["_TIME"] = pd.to_datetime(df_typed[time_col], errors="coerce")
    else:
        df_typed["_TIME"] = pd.NaT

    return df_typed


def _collect_retained_rows(
    dat_path: Path,
    columns: List[str],
    filtered_idx: Set[int],
    typed_len: int,
) -> List[Dict[str, str]]:
    retained_rows: List[Dict[str, str]] = []
    start_idx = 0

    total_rows = 0
    with dat_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        for _ in range(4):
            next(reader, None)
        for _ in reader:
            total_rows += 1

    if total_rows > typed_len:
        start_idx = total_rows - typed_len

    with dat_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, fieldnames=columns)
        for _ in range(4):
            next(reader, None)

        for raw_idx, row in enumerate(reader):
            typed_idx = raw_idx - start_idx
            if typed_idx < 0 or typed_idx not in filtered_idx:
                continue

            retained_rows.append(
                {col: "" if row.get(col) is None else str(row.get(col)) for col in columns}
            )

    return retained_rows


def write_json_atomic(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
        fh.flush()
        os.fsync(fh.fileno())

    def _is_lock_related_error(exc: BaseException) -> bool:
        if isinstance(exc, PermissionError):
            return True
        if not isinstance(exc, OSError):
            return False
        if exc.errno in {errno.EACCES, errno.EPERM}:
            return True
        return getattr(exc, "winerror", None) in WINDOWS_LOCK_WINERRORS

    delay_seconds = ATOMIC_REPLACE_BACKOFF_SECONDS
    try:
        for attempt in range(1, ATOMIC_REPLACE_MAX_ATTEMPTS + 1):
            try:
                temp_path.replace(path)
                return
            except Exception as exc:
                if not _is_lock_related_error(exc):
                    raise
                if attempt >= ATOMIC_REPLACE_MAX_ATTEMPTS:
                    raise
                logging.warning(
                    "Atomic replace retry %d/%d for %s due to lock collision.",
                    attempt,
                    ATOMIC_REPLACE_MAX_ATTEMPTS,
                    path,
                )
                time.sleep(delay_seconds)
                delay_seconds *= 2
    except Exception:
        try:
            temp_path.unlink(missing_ok=True)
        except Exception as cleanup_exc:
            logging.warning("Failed to clean up temp file %s: %s", temp_path, cleanup_exc)
        raise


def _apply_public_reading(record: SummaryRecord, base_path: Path, now: pd.Timestamp) -> None:
    """Prefer Public telemetry for HOME freshness and latest water level only."""
    public_path = base_path / record.site / f"{record.site}_Public.dat"
    try:
        with public_path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.reader(handle, strict=True)
            header = [next(reader, None) for _ in range(4)]
            if header[0] is None:  # Empty export: retain the Measurement summary.
                return
            if any(row is None for row in header) or header[0][0].strip() != "TOA5":
                raise ValueError("Expected a four-line TOA5 header")
            columns = [cell.strip().upper() for cell in header[1]]
            time_index = columns.index("TIMESTAMP")
            water_index = columns.index("WATERLEVEL") if "WATERLEVEL" in columns else None
            latest_time = None
            latest_water = None
            for row in reader:
                if not row:
                    continue
                if len(row) != len(columns):
                    raise ValueError("Public row does not match its column header")
                timestamp = pd.to_datetime(row[time_index], errors="coerce")
                if pd.isna(timestamp):
                    continue
                if latest_time is None or timestamp >= latest_time:
                    latest_time = timestamp
                    latest_water = _safe_float(row[water_index]) if water_index is not None else None
            if latest_time is None:
                return
            # Compute before modifying the record so a bad export falls back entirely.
            time_since = _format_timedelta(now - latest_time)
            latest_iso = latest_time.isoformat()
    except FileNotFoundError:
        return
    except Exception as exc:
        logging.warning("Unable to read Public telemetry from %s: %s", public_path, exc)
        return

    record.latest_timestamp = latest_iso
    record.time_since = time_since
    if latest_water is not None and math.isfinite(latest_water):
        record.latest_values["WATERLEVEL"] = latest_water


def process_site(
    site: str,
    base_path: Path,
    output_dir: Path,
    retention_cutoff: pd.Timestamp,
    summary_cutoff: pd.Timestamp,
    now: pd.Timestamp,
) -> SummaryRecord:
    record = _process_measurement_site(
        site, base_path, output_dir, retention_cutoff, summary_cutoff, now
    )
    _apply_public_reading(record, base_path, now)
    return record


def _process_measurement_site(
    site: str,
    base_path: Path,
    output_dir: Path,
    retention_cutoff: pd.Timestamp,
    summary_cutoff: pd.Timestamp,
    now: pd.Timestamp,
) -> SummaryRecord:
    site_dir = base_path / site
    dat_path = site_dir / f"{site}_Measurement.dat"
    label = CONFIG.get("SITE_LABELS", {}).get(site, site)

    if not dat_path.exists():
        logging.error("Measurement file missing for site %s at %s", site, dat_path)
        return SummaryRecord(
            site=site,
            label=label,
            latest_timestamp=None,
            time_since="No data",
            latest_values={"NNO3": None, "OPUS_TSSEQ": None, "WATERLEVEL": None},
            max_values={"NNO3": None, "OPUS_TSSEQ": None, "WATERLEVEL": None},
            water_level_trend="No data",
            status="missing",
            message="Measurement file not found.",
        )

    logging.info("Processing %s", dat_path)

    header = read_header_row(dat_path)
    columns, units, processing, _ = read_toa5_header(str(dat_path))
    df_typed = _load_summary_frame(dat_path, columns)

    retention_mask = df_typed["_TIME"] >= retention_cutoff
    retention_mask &= df_typed["_TIME"].notna()
    filtered_idx = set(df_typed.index[retention_mask].tolist())

    if filtered_idx:
        data_rows = _collect_retained_rows(dat_path, columns, filtered_idx, len(df_typed))
    else:
        if df_typed.empty:
            logging.warning("No measurement rows found for %s", site)
        data_rows = []

    latest_time = df_typed["_TIME"].max()
    latest_idx = set()
    if pd.notna(latest_time):
        latest_mask = df_typed["_TIME"] >= latest_time - pd.Timedelta(days=5)
        latest_idx = set(df_typed.index[latest_mask].tolist())
    latest_rows = (
        _collect_retained_rows(dat_path, columns, latest_idx, len(df_typed))
        if latest_idx else []
    )

    payload = {
        "file": dat_path.name,
        "header": header,
        "columns": columns,
        "units": units,
        "processing": processing,
        "data": data_rows,
        "recent_cutoff": retention_cutoff.isoformat(),
        "latest_measurement_timestamp": latest_time.isoformat() if pd.notna(latest_time) else None,
        "latest_window": {"columns": columns, "data": latest_rows},
    }

    site_output_dir = output_dir / site
    output_path = site_output_dir / "measurement_recent.json"
    write_json_atomic(output_path, payload)
    logging.info("Wrote %s", output_path)

    df_sorted = df_typed.dropna(subset=["_TIME"]).sort_values("_TIME")
    if df_sorted.empty:
        return SummaryRecord(
            site=site,
            label=label,
            latest_timestamp=None,
            time_since="No data",
            latest_values={"NNO3": None, "OPUS_TSSEQ": None, "WATERLEVEL": None},
            max_values={"NNO3": None, "OPUS_TSSEQ": None, "WATERLEVEL": None},
            water_level_trend="No data",
            status="empty",
            message="No timestamped measurements available.",
        )

    latest_row = df_sorted.iloc[-1]
    latest_time = pd.to_datetime(latest_row["_TIME"], errors="coerce")
    if pd.isna(latest_time):
        time_since = "Invalid timestamp"
        latest_iso = None
    else:
        time_since = _format_timedelta(now - latest_time)
        latest_iso = latest_time.isoformat()

    nno3_col = (
        _find_column(columns, "NNO3")
        or _find_column(columns, "OPUS_NNO3")
        or _find_column(columns, "NICO-NNO3")
        or _find_column(columns, "NICO_NNO3")
    )
    tsseq_col = _find_column(columns, "OPUS_TSSEQ")
    water_col = _find_column(columns, "WATERLEVEL") or _find_column(columns, "WATERLEVEL_M") or _find_column(columns, "WATERLEVEL_MM")

    latest_values = {
        "NNO3": _safe_float(latest_row.get(nno3_col)) if nno3_col else None,
        "OPUS_TSSEQ": _safe_float(latest_row.get(tsseq_col)) if tsseq_col else None,
        "WATERLEVEL": _safe_float(latest_row.get(water_col)) if water_col else None,
    }

    recent = df_sorted[df_sorted["_TIME"] >= summary_cutoff]

    max_values = {
        "NNO3": _safe_float(recent[nno3_col].max(skipna=True)) if (nno3_col and nno3_col in recent.columns and not recent.empty) else None,
        "OPUS_TSSEQ": _safe_float(recent[tsseq_col].max(skipna=True)) if (tsseq_col and tsseq_col in recent.columns and not recent.empty) else None,
        "WATERLEVEL": _safe_float(recent[water_col].max(skipna=True)) if (water_col and water_col in recent.columns and not recent.empty) else None,
    }

    trend = "No data"
    if water_col and water_col in recent.columns and not recent.empty:
        recent_valid = recent[water_col].dropna()
        if not recent_valid.empty:
            first_val = _safe_float(recent_valid.iloc[0])
            last_val = _safe_float(recent_valid.iloc[-1])
            if first_val is not None and last_val is not None:
                if last_val > first_val:
                    trend = "Rising"
                elif last_val < first_val:
                    trend = "Falling"
                else:
                    trend = "Stable"
            else:
                trend = "Insufficient data"
        else:
            trend = "No data"

    return SummaryRecord(
        site=site,
        label=label,
        latest_timestamp=latest_iso,
        time_since=time_since,
        latest_values=latest_values,
        max_values=max_values,
        water_level_trend=trend,
    )


def run_once(
    base_path: Path,
    output_dir: Path,
    retention_days: int,
) -> int:
    lock_path = output_dir / LOCK_FILENAME

    lock_fd = acquire_lock(lock_path)
    if lock_fd is None:
        return 2

    try:
        if not base_path.exists():
            logging.error("Base path %s does not exist", base_path)
            return 1

        now = pd.Timestamp.now()
        retention_cutoff = now - pd.Timedelta(days=retention_days)
        summary_cutoff = now - pd.Timedelta(hours=SUMMARY_WINDOW_HOURS)

        summary_records: List[SummaryRecord] = []
        had_error = False

        for site in CONFIG.get("SITES", []):
            try:
                site_dir = base_path / site
                source_dat_path = site_dir / f"{site}_Measurement.dat"
                slim_dat_path = site_dir / f"{site}_Historic_slim.dat"
                state_path = site_dir / f"{site}_Historic_slim.state.json"
                eagle_csv_path = site_dir / f"{site}_EagleIO.csv"
                if not eagle_csv_path.exists():
                    eagle_csv_path = None

                slim_stats = update_history_slim(
                    site_id=site,
                    measurement_path=source_dat_path,
                    eagle_csv_path=eagle_csv_path,
                    slim_path=slim_dat_path,
                    state_path=state_path,
                )
                if slim_stats.rebuilt or slim_stats.rows_appended > 0:
                    logging.info("Updated slim history for %s at %s", site, slim_dat_path)

                record = process_site(
                    site=site,
                    base_path=base_path,
                    output_dir=output_dir,
                    retention_cutoff=retention_cutoff,
                    summary_cutoff=summary_cutoff,
                    now=now,
                )
                summary_records.append(record)
                if record.status in {"error", "missing"}:
                    had_error = True
            except Exception as exc:
                logging.exception("Failed to process site %s: %s", site, exc)
                summary_records.append(
                    SummaryRecord(
                        site=site,
                        label=CONFIG.get("SITE_LABELS", {}).get(site, site),
                        latest_timestamp=None,
                        time_since="Error",
                        latest_values={"NNO3": None, "OPUS_TSSEQ": None, "WATERLEVEL": None},
                        max_values={"NNO3": None, "OPUS_TSSEQ": None, "WATERLEVEL": None},
                        water_level_trend="Error",
                        status="error",
                        message=str(exc),
                    )
                )
                had_error = True

                # Measurement/history failures must not hide valid Public telemetry.
                _apply_public_reading(summary_records[-1], base_path, now)

        summary_payload = {
            "generated_at": now.isoformat(),
            "window_hours": SUMMARY_WINDOW_HOURS,
            "retention_days": retention_days,
            "sites": [record.to_dict() for record in summary_records],
        }
        summary_path = output_dir / "home_summary.json"
        try:
            write_json_atomic(summary_path, summary_payload)
        except Exception as exc:
            logging.error("Failed to write summary payload at %s: %s", summary_path, exc)
            return 1
        logging.info("Wrote %s", summary_path)

        if had_error:
            logging.warning("Completed with warnings/errors.")
            return 1

        logging.info("Precomputation finished successfully.")
        return 0
    finally:
        release_lock(lock_fd, lock_path)


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    if args.interval_minutes < 0:
        logging.error("--interval-minutes must be zero or positive.")
        return 1

    base_path = args.base_path.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    logging.info("Base path: %s", base_path)
    logging.info("Output directory: %s", output_dir)

    interval_seconds = args.interval_minutes * 60
    run_forever = interval_seconds > 0
    iteration = 0
    last_exit_code = 0

    try:
        while True:
            iteration += 1
            logging.info("Starting preprocessing run #%d", iteration)
            start = time.monotonic()
            last_exit_code = run_once(
                base_path=base_path,
                output_dir=output_dir,
                retention_days=args.retention_days,
            )

            if not run_forever:
                return last_exit_code

            elapsed = time.monotonic() - start
            sleep_duration = max(0.0, interval_seconds - elapsed)
            if sleep_duration > 0:
                logging.info(
                    "Sleeping for %.0f seconds before next preprocessing run.",
                    sleep_duration,
                )
                time.sleep(sleep_duration)
            else:
                logging.info(
                    "Interval elapsed during processing; starting next run immediately."
                )
    except KeyboardInterrupt:
        logging.info("Received interrupt; exiting after %d run(s).", iteration)
        return last_exit_code


if __name__ == "__main__":
    sys.exit(main())

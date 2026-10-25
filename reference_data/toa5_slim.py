from __future__ import annotations

import csv
import json
import logging
import math
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from itertools import zip_longest
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

SLIM_COLUMNS = ["TIMESTAMP", "RECORD", "waterLevel", "NNO3", "TSS"]
SLIM_UNITS = ["TS", "RN", "metres H2O", "mg/L", "mg/L"]
SLIM_PROCESSING = ["", "", "Smp", "Smp", "Smp"]
DEDUP_WINDOW_SIZE = 2000
DEDUP_FULL_REBUILD_THRESHOLD = 200_000
DERIVED_COLUMNS = ["Q_m3s", "Flux_kg", "FYFlux_kg"]


@dataclass
class SlimState:
    logger_offset_bytes: int = 0
    eagle_backfill_done: bool = False
    last_slim_ts: str = ""
    last_seen_timestamps: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SlimUpdateStats:
    site: str | None
    selected_output_columns: list[str]
    source_rows_scanned: int
    rows_appended: int
    rows_appended_logger: int
    rows_appended_eagle: int
    rows_skipped_dedup: int
    final_offset: int
    missing_required_fields: list[str]
    rebuilt: bool
    latest_timestamp: str | None


def _is_nan_like(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float):
        return math.isnan(value)
    text = str(value).strip()
    return text == "" or text.lower() == "nan"


def _read_csv_line(raw_line: str) -> list[str]:
    if not raw_line:
        return []
    return next(csv.reader([raw_line]))


def _normalise_ts(value: Any) -> str:
    return "" if value is None else str(value).strip().strip('"')


def _parse_timestamp(value: Any) -> datetime | None:
    ts = _normalise_ts(value)
    if not ts:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue
    return None


def _find_index(columns: list[str], *names: str) -> int | None:
    index = {name: i for i, name in enumerate(columns)}
    for name in names:
        if name in index:
            return index[name]
    return None


def _load_state(state_path: Path) -> SlimState:
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError, TypeError):
        return SlimState()

    if not isinstance(payload, dict):
        return SlimState()

    logger_offset = payload.get("logger_offset_bytes")
    eagle_done = payload.get("eagle_backfill_done")
    last_ts = payload.get("last_slim_ts")
    last_seen = payload.get("last_seen_timestamps")

    return SlimState(
        logger_offset_bytes=logger_offset if isinstance(logger_offset, int) and logger_offset >= 0 else 0,
        eagle_backfill_done=bool(eagle_done),
        last_slim_ts=last_ts if isinstance(last_ts, str) else "",
        last_seen_timestamps=[str(ts) for ts in (last_seen or []) if str(ts).strip()][-DEDUP_WINDOW_SIZE:],
    )


def _write_state_atomic(state_path: Path, state: SlimState) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "logger_offset_bytes": state.logger_offset_bytes,
        "eagle_backfill_done": state.eagle_backfill_done,
        "last_slim_ts": state.last_slim_ts,
        "last_seen_timestamps": state.last_seen_timestamps[-DEDUP_WINDOW_SIZE:],
    }
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=state_path.parent,
        prefix=f".{state_path.name}.",
        suffix=".tmp",
        delete=False,
    ) as tmp:
        json.dump(payload, tmp, sort_keys=True, separators=(",", ":"))
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    os.replace(tmp_path, state_path)


def _read_source_headers(source_dat_path: Path) -> tuple[str, list[str]]:
    with source_dat_path.open("r", encoding="utf-8", newline="") as src:
        line1 = src.readline().rstrip("\r\n")
        line2 = _read_csv_line(src.readline().rstrip("\r\n"))
    return line1, line2


def _resolve_logger_columns(columns: list[str]) -> dict[str, int | None]:
    return {
        "TIMESTAMP": _find_index(columns, "TIMESTAMP"),
        "RECORD": _find_index(columns, "RECORD"),
        "waterLevel": _find_index(columns, "waterLevel"),
        "OPUS_NNO3": _find_index(columns, "OPUS_NNO3"),
        "NICO": _find_index(columns, "NICO_NNO3", "NICO-NNO3"),
        "OPUS_TSSEQ": _find_index(columns, "OPUS_TSSEQ", "OPUS_TSSeq"),
    }


def _build_logger_projection(row: list[str], cols: dict[str, int | None]) -> list[str]:
    def get(name: str) -> str:
        idx = cols.get(name)
        if idx is None or idx >= len(row):
            return ""
        return row[idx]

    timestamp = get("TIMESTAMP")
    record = get("RECORD")
    water_level = get("waterLevel")

    opus_nno3 = get("OPUS_NNO3")
    nico_nno3 = get("NICO")
    nno3 = opus_nno3 if not _is_nan_like(opus_nno3) else nico_nno3

    tss = get("OPUS_TSSEQ")

    return [timestamp, record, water_level, nno3, tss]


def _extract_timestamp_from_row(row: list[str], columns: list[str]) -> str:
    if not columns:
        return ""
    idx = _find_index(columns, "TIMESTAMP", "timestamp", "DateTime", "datetime")
    if idx is None or idx >= len(row):
        return ""
    return _normalise_ts(row[idx])


def _extract_eagle_fields(record: dict[str, str]) -> tuple[str, str, str, str]:
    timestamp = ""
    for key in ("TIMESTAMP", "Timestamp", "timestamp", "DateTime", "datetime"):
        if key in record and str(record[key]).strip():
            timestamp = _normalise_ts(record[key])
            break

    water = ""
    nitrate_nico = ""
    nitrate_fallback = ""
    tss = ""

    def _normalise_name(name: str) -> str:
        return name.lower().replace(" ", "")

    def is_nico_nitrate_key(name: str) -> bool:
        key_l = _normalise_name(name)
        tail = key_l.split("/")[-1]
        return ("nico" in key_l and "nno3" in key_l) or ("nico" in tail and "nno3" in tail)

    def is_any_nitrate_key(name: str) -> bool:
        key_l = _normalise_name(name)
        tail = key_l.split("/")[-1]
        return "nno3" in key_l or "nno3" in tail

    for key, value in record.items():
        key_l = key.lower()
        key_tail = key_l.split("/")[-1].strip()
        key_tail = key_tail.replace("measurement -", "").strip()
        value_str = "" if value is None else str(value)

        if not water and (key_l == "waterlevel" or key_tail == "waterlevel"):
            water = value_str
        elif is_nico_nitrate_key(key) and not nitrate_nico:
            if not _is_nan_like(value_str):
                nitrate_nico = value_str
        elif is_any_nitrate_key(key) and not nitrate_fallback:
            if not _is_nan_like(value_str):
                nitrate_fallback = value_str
        elif not tss and (
            key in {"OPUS_TSSEQ", "OPUS_TSSeq", "OPUS_TSS", "TSS"}
            or key_tail in {"opus_tsseq", "opus tsseq", "opus_tss", "opus tss", "tss"}
        ):
            if not _is_nan_like(value_str):
                tss = value_str

        if key_l in {"path", "tag", "name", "topic"} and isinstance(value, str):
            path_l = value.lower()
            candidate = record.get("value") or record.get("VALUE")
            if candidate is None or _is_nan_like(candidate):
                continue
            candidate_str = str(candidate)
            if "waterlevel" in path_l and not water:
                water = candidate_str
            if ("nico_nno3" in path_l or "nico-nno3" in path_l) and not nitrate_nico:
                nitrate_nico = candidate_str
            elif ("nno3" in path_l) and not nitrate_fallback:
                nitrate_fallback = candidate_str
            if ("opus_tsseq" in path_l or "opus_tss" in path_l) and not tss:
                tss = candidate_str

    nitrate = nitrate_nico if nitrate_nico else nitrate_fallback
    return timestamp, water, nitrate, tss


def _has_timestamp_header(row: list[str] | None) -> bool:
    if not row:
        return False
    for key in row:
        if str(key).strip().strip('"').lower() in {"timestamp", "datetime", "date", "time"}:
            return True
    return False


def _iter_eagle_records(eagle_csv_path: Path):
    with eagle_csv_path.open("r", encoding="utf-8", newline="") as src:
        reader = csv.reader(src)
        first = next(reader, None)
        if first is None:
            return

        second = next(reader, None)

        if _has_timestamp_header(first):
            header = first
            pending_rows = [second] if second is not None else []
        elif _has_timestamp_header(second):
            header = second
            third = next(reader, None)
            pending_rows = []
            if third is not None:
                first_cell = str(third[0]).strip().strip('"').lower() if third else ""
                if first_cell != "units":
                    pending_rows.append(third)
        else:
            header = first
            pending_rows = [second] if second is not None else []

        def to_record(row: list[str]) -> dict[str, str]:
            return {
                str(k): "" if v is None else str(v)
                for k, v in zip_longest(header, row, fillvalue="")
                if str(k).strip()
            }

        for row in pending_rows:
            if row is None:
                continue
            yield to_record(row)

        for row in reader:
            if not row:
                continue
            yield to_record(row)


def _write_headers(out_path: Path, line1_raw: str) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as out:
        out.write(f"{line1_raw}\n")
        writer = csv.writer(out, lineterminator="\n")
        writer.writerow(SLIM_COLUMNS)
        writer.writerow(SLIM_UNITS)
        writer.writerow(SLIM_PROCESSING)


def _read_slim_tail_timestamps(slim_dat_path: Path, limit: int = DEDUP_WINDOW_SIZE) -> list[str]:
    if not slim_dat_path.exists():
        return []
    timestamps: list[str] = []
    with slim_dat_path.open("r", encoding="utf-8", newline="") as src:
        reader = csv.reader(src)
        for _ in range(4):
            next(reader, None)
        for row in reader:
            if not row:
                continue
            ts = _normalise_ts(row[0] if row else "")
            if not ts:
                continue
            timestamps.append(ts)
    return timestamps[-limit:]


def _earliest_slim_timestamp(slim_dat_path: Path) -> str:
    if not slim_dat_path.exists():
        return ""
    with slim_dat_path.open("r", encoding="utf-8", newline="") as src:
        reader = csv.reader(src)
        for _ in range(4):
            next(reader, None)
        for row in reader:
            if not row:
                continue
            ts = _normalise_ts(row[0])
            if ts:
                return ts
    return ""


def _first_logger_timestamp(source_dat_path: Path, timestamp_idx: int | None) -> str:
    if timestamp_idx is None:
        return ""
    with source_dat_path.open("r", encoding="utf-8", newline="") as src:
        reader = csv.reader(src)
        for _ in range(4):
            next(reader, None)
        for row in reader:
            if not row:
                continue
            if timestamp_idx < len(row):
                ts = _normalise_ts(row[timestamp_idx])
                if ts:
                    return ts
    return ""


def _ts_in_window(ts: str, window: set[str]) -> bool:
    return bool(ts) and ts in window


def _append_projected_rows(
    slim_dat_path: Path,
    rows: list[list[str]],
    dedup_window: list[str],
    *,
    state: SlimState,
    dedup: bool = True,
) -> tuple[int, int, str]:
    skipped = 0
    appended = 0
    latest = state.last_slim_ts
    window = set(dedup_window)

    with slim_dat_path.open("a", encoding="utf-8", newline="") as out:
        writer = csv.writer(out, lineterminator="\n")
        for row in rows:
            ts = _normalise_ts(row[0])
            if dedup and _ts_in_window(ts, window):
                skipped += 1
                continue
            writer.writerow(row)
            appended += 1
            if ts:
                dedup_window.append(ts)
                if len(dedup_window) > DEDUP_WINDOW_SIZE:
                    dedup_window[:] = dedup_window[-DEDUP_WINDOW_SIZE:]
                window = set(dedup_window)
                latest = ts

    return appended, skipped, latest


def _logger_rows_from_offset(source_dat_path: Path, cols: dict[str, int | None], start_offset: int) -> tuple[list[list[str]], int, int]:
    source_size = source_dat_path.stat().st_size
    if start_offset >= source_size:
        return [], source_size, 0

    scanned = 0
    rows: list[list[str]] = []
    with source_dat_path.open("rb") as src:
        src.seek(start_offset)
        chunk = src.read()

    text = chunk.decode("utf-8", errors="ignore")
    for raw in text.splitlines():
        if not raw.strip():
            continue
        parsed = _read_csv_line(raw)
        if not parsed:
            continue
        scanned += 1
        rows.append(_build_logger_projection(parsed, cols))

    return rows, source_size, scanned


def _logger_rows_full(source_dat_path: Path, cols: dict[str, int | None]) -> tuple[list[list[str]], int]:
    rows: list[list[str]] = []
    scanned = 0
    with source_dat_path.open("r", encoding="utf-8", newline="") as src:
        reader = csv.reader(src)
        for _ in range(4):
            next(reader, None)
        for row in reader:
            if not row:
                continue
            scanned += 1
            rows.append(_build_logger_projection(row, cols))
    return rows, scanned


def _eagle_rows_full(eagle_csv_path: Path | None) -> tuple[list[list[str]], int]:
    if eagle_csv_path is None or not eagle_csv_path.exists():
        return [], 0

    rows: list[list[str]] = []
    scanned = 0
    for record in _iter_eagle_records(eagle_csv_path):
        scanned += 1
        ts, water, nitrate, tss = _extract_eagle_fields(record)
        if not ts:
            continue
        rows.append([ts, "", water, nitrate, tss])
    return rows, scanned


def _eagle_rows_older_than(eagle_csv_path: Path | None, cutoff_ts: str) -> tuple[list[list[str]], int]:
    rows, scanned = _eagle_rows_full(eagle_csv_path)
    cutoff = _parse_timestamp(cutoff_ts)
    if cutoff is None:
        return [], scanned

    older: list[list[str]] = []
    for row in rows:
        row_ts = _parse_timestamp(row[0])
        if row_ts is not None and row_ts < cutoff:
            older.append(row)
    return older, scanned


def _prepare_initial_dedup(logger_rows: list[list[str]], eagle_rows: list[list[str]]) -> list[str]:
    total_rows = len(logger_rows) + len(eagle_rows)
    if total_rows > DEDUP_FULL_REBUILD_THRESHOLD:
        return []

    seen: list[str] = []
    known: set[str] = set()
    for row in eagle_rows + logger_rows:
        ts = _normalise_ts(row[0])
        if not ts or ts in known:
            continue
        known.add(ts)
        seen.append(ts)
        if len(seen) > DEDUP_WINDOW_SIZE:
            seen = seen[-DEDUP_WINDOW_SIZE:]
    return seen


def get_financial_year_label(timestamp: Any) -> str | None:
    dt = _parse_timestamp(timestamp)
    if dt is None:
        return None
    year = dt.year + 1 if dt.month >= 7 else dt.year
    return f"FY{year}"


def stage_to_discharge(stage_m: float | None, site_cfg: dict[str, Any] | None) -> float | None:
    if stage_m is None or site_cfg is None:
        return None
    equation = site_cfg.get("equation", {}) if isinstance(site_cfg, dict) else {}
    if equation.get("type") != "poly":
        return None

    min_stage = equation.get("min_stage")
    max_stage = equation.get("max_stage")
    if isinstance(min_stage, (int, float)) and stage_m < min_stage:
        return None
    if isinstance(max_stage, (int, float)) and stage_m > max_stage:
        return None

    coeffs = equation.get("coeffs") or []
    discharge = 0.0
    for power, coeff in enumerate(coeffs):
        discharge += float(coeff) * (stage_m ** power)

    units = str(site_cfg.get("discharge_units", "m3/s"))
    if units == "m3/s":
        return discharge
    if units == "L/s":
        return discharge / 1000.0
    if units == "ML/d":
        return discharge * 1_000_000.0 / 86_400.0
    return discharge


def discharge_to_interval_volume(discharge_m3s: float | None, dt_seconds: float | None) -> float | None:
    if discharge_m3s is None or dt_seconds is None:
        return None
    return discharge_m3s * dt_seconds


def compute_derived(row: dict[str, Any], prev_state: dict[str, Any], site_cfg: dict[str, Any] | None) -> dict[str, Any]:
    """Placeholder for future discharge/flux derivation support."""
    _ = (row, prev_state, site_cfg)
    return {}


def update_history_slim(
    site_id: str,
    measurement_path: Path,
    eagle_csv_path: Path | None,
    slim_path: Path,
    state_path: Path,
) -> SlimUpdateStats:
    line1_raw, header_cols = _read_source_headers(measurement_path)
    logger_cols = _resolve_logger_columns(header_cols)

    missing = [name for name in ("TIMESTAMP", "RECORD") if logger_cols.get(name) is None]
    source_col_presence = {
        "waterLevel": logger_cols.get("waterLevel") is not None,
        "OPUS_NNO3": logger_cols.get("OPUS_NNO3") is not None,
        "NICO_NNO3": logger_cols.get("NICO") is not None,
        "OPUS_TSSEQ": logger_cols.get("OPUS_TSSEQ") is not None,
    }

    state = _load_state(state_path)
    rebuilt = not slim_path.exists()

    source_rows_scanned = 0
    rows_appended = 0
    rows_appended_logger = 0
    rows_appended_eagle = 0
    rows_skipped_dedup = 0

    dedup_window = state.last_seen_timestamps or _read_slim_tail_timestamps(slim_path)

    if rebuilt:
        _write_headers(slim_path, line1_raw)
        eagle_rows, eagle_scanned = _eagle_rows_full(eagle_csv_path)
        logger_rows, logger_scanned = _logger_rows_full(measurement_path, logger_cols)
        source_rows_scanned += eagle_scanned + logger_scanned

        rows_to_write = eagle_rows + logger_rows

        appended, skipped, latest = _append_projected_rows(
            slim_path,
            rows_to_write,
            dedup_window,
            state=state,
            dedup=False,
        )
        rows_appended += appended
        rows_skipped_dedup += skipped
        rows_appended_eagle = len(eagle_rows)
        rows_appended_logger = len(logger_rows)

        state.logger_offset_bytes = measurement_path.stat().st_size
        state.eagle_backfill_done = True
        state.last_slim_ts = latest
        state.last_seen_timestamps = dedup_window[-DEDUP_WINDOW_SIZE:]
    else:
        if not state.eagle_backfill_done:
            earliest_slim = _earliest_slim_timestamp(slim_path)
            first_logger_ts = _first_logger_timestamp(measurement_path, logger_cols.get("TIMESTAMP"))
            cutoff = earliest_slim or first_logger_ts
            eagle_rows, eagle_scanned = _eagle_rows_older_than(eagle_csv_path, cutoff)
            source_rows_scanned += eagle_scanned
            appended, skipped, latest = _append_projected_rows(
                slim_path,
                eagle_rows,
                dedup_window,
                state=state,
            )
            rows_appended += appended
            rows_appended_eagle += appended
            rows_skipped_dedup += skipped
            if latest:
                state.last_slim_ts = latest
            state.eagle_backfill_done = True

        logger_rows, new_offset, scanned_logger = _logger_rows_from_offset(
            measurement_path,
            logger_cols,
            state.logger_offset_bytes,
        )
        source_rows_scanned += scanned_logger

        appended, skipped, latest = _append_projected_rows(
            slim_path,
            logger_rows,
            dedup_window,
            state=state,
        )
        rows_appended += appended
        rows_appended_logger += appended
        rows_skipped_dedup += skipped
        state.logger_offset_bytes = new_offset
        if latest:
            state.last_slim_ts = latest
        state.last_seen_timestamps = dedup_window[-DEDUP_WINDOW_SIZE:]

    _write_state_atomic(state_path, state)

    log_payload = {
        "event": "history_slim_update",
        "site": site_id,
        "source_columns": source_col_presence,
        "rows_appended_logger": rows_appended_logger,
        "rows_appended_eagle": rows_appended_eagle,
        "rows_skipped_dedup": rows_skipped_dedup,
        "latest_timestamp": state.last_slim_ts,
        "rebuilt": rebuilt,
    }
    LOGGER.info("%s", json.dumps(log_payload, sort_keys=True))

    return SlimUpdateStats(
        site=site_id,
        selected_output_columns=SLIM_COLUMNS,
        source_rows_scanned=source_rows_scanned,
        rows_appended=rows_appended,
        rows_appended_logger=rows_appended_logger,
        rows_appended_eagle=rows_appended_eagle,
        rows_skipped_dedup=rows_skipped_dedup,
        final_offset=state.logger_offset_bytes,
        missing_required_fields=missing,
        rebuilt=rebuilt,
        latest_timestamp=state.last_slim_ts or None,
    )


def update_slim_toa5(
    source_dat_path: Path,
    slim_dat_path: Path,
    state_path: Path,
    keep_cols: list[str] | None = None,
    *,
    site: str | None = None,
) -> SlimUpdateStats:
    """Backwards-compatible wrapper used by existing code and tests."""
    _ = keep_cols
    return update_history_slim(
        site_id=site or source_dat_path.stem,
        measurement_path=source_dat_path,
        eagle_csv_path=None,
        slim_path=slim_dat_path,
        state_path=state_path,
    )

import os
import csv
import pandas as pd
import numpy as np
from functools import lru_cache
from typing import Dict, List, Tuple
from config import CONFIG

SENTINEL_NA = {-7999, -7999.0}
NA_STRINGS = ["NAN", "NaN", "nan", "", " "]
MAX_ROWS_PER_FILE = 50_000
CHUNK_SIZE = 10_000

# -----------------------------
# UTILITIES
# -----------------------------
def read_toa5_header(path: str) -> Tuple[List[str], List[str], List[str], Dict[str, str]]:
    """
    Reads the first 4 header rows from a TOA5 data file.
    """
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        rows: List[List[str]] = []
        for _ in range(4):
            try:
                rows.append(next(reader))
            except StopIteration:
                rows.append([])

        def clean_row(r: List[str]) -> List[str]:
            return [c.strip().strip('"') for c in r]

        l1, l2, l3, l4 = map(clean_row, rows)

    meta: Dict[str, str] = {}
    if not l2:
        l2 = ["TIMESTAMP"]

    return l2, l3, l4, meta


def build_na_values() -> List[str]:
    """Builds a list of values to treat as NA, including quoted variants."""
    na = set(NA_STRINGS)
    na |= {f'"{s}"' for s in NA_STRINGS}
    return list(na)


def load_recent_rows(path: str, nrows: int = CHUNK_SIZE) -> pd.DataFrame:
    """
    Load only the last ~nrows rows from a large file using chunked reading.
    Useful for quick loading without parsing the whole file.
    """
    last_chunk = None
    total_rows = 0

    for chunk in pd.read_csv(
        path,
        header=None,
        skiprows=4,  # skip metadata header
        chunksize=CHUNK_SIZE,
        quotechar='"',
        na_values=build_na_values(),
        keep_default_na=True,
        engine="python"
    ):
        last_chunk = chunk
        total_rows += len(chunk)

    if last_chunk is None:
        return pd.DataFrame()

    # If file is smaller than nrows, reload fully
    if total_rows <= nrows:
        return pd.read_csv(
            path,
            header=None,
            skiprows=4,
            quotechar='"',
            na_values=build_na_values(),
            keep_default_na=True,
            engine="python"
        )

    return last_chunk.reset_index(drop=True)


@lru_cache(maxsize=20)
def _load_toa5_cached(path: str, mtime: float, quick: bool) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Internal cached function to load TOA5 data efficiently.
    quick=True -> only last ~50k rows
    quick=False -> full file (up to MAX_ROWS_PER_FILE)
    """
    col_names, col_units, col_proc, meta = read_toa5_header(path)

    if quick:
        df = load_recent_rows(path, nrows=CHUNK_SIZE)
        df.columns = col_names[: len(df.columns)]
    else:
        df = pd.read_csv(
            path,
            header=None,
            names=col_names,
            skiprows=4,
            quotechar='"',
            na_values=build_na_values(),
            keep_default_na=True,
            engine="python"
        ).copy()

    # Trim oversized files
    if len(df) > MAX_ROWS_PER_FILE:
        df = df.tail(MAX_ROWS_PER_FILE).reset_index(drop=True)

    # Handle time + numeric cleanup
    time_col = None
    for c in df.columns:
        cu = c.upper() if isinstance(c, str) else c
        if cu == "TIMESTAMP":
            time_col = c
        elif cu in ["NNO3", "OPUS_NNO3", "NICO-NNO3", "NICO_NNO3", "OPUS_TSSEQ", "WATERLEVEL", "OPUS_TSSEQ"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            if pd.api.types.is_numeric_dtype(df[c]):
                df.loc[df[c].isin(SENTINEL_NA), c] = np.nan

    if time_col is not None:
        df["_TIME"] = pd.to_datetime(df[time_col], errors="coerce")
    else:
        df["_TIME"] = pd.NaT

    return df, meta


def load_toa5(path: str, quick: bool = False) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Public function to load a TOA5 file with caching.
    - quick=True: only loads the last ~50k rows
    - quick=False: loads the full file (capped at MAX_ROWS_PER_FILE)
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    mtime = os.path.getmtime(path)
    return _load_toa5_cached(path, mtime, quick)

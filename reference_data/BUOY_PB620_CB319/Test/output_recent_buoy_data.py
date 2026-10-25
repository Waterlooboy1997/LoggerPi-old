import struct
import pandas as pd
from pathlib import Path

# === CONFIGURATION ===
input_file = Path("Kalamia_lagoon_buoy_do_test_Measurement.dat")

# === FUNCTIONS ===
def convert_pakbus_float(high_reg, low_reg):
    """Convert two 16-bit PakBus registers into a float32 (little-endian)."""
    if pd.isna(high_reg) or pd.isna(low_reg):
        return float('nan')
    try:
        hr = int(float(high_reg)) & 0xFFFF
        lr = int(float(low_reg)) & 0xFFFF
        raw_bytes = bytes([
            (hr >> 8) & 0xFF, hr & 0xFF,
            (lr >> 8) & 0xFF, lr & 0xFF
        ])
        return struct.unpack('<f', raw_bytes)[0]
    except Exception:
        return float('nan')

# === READ FILE ===
# Skip the first metadata line only (headers are on the 2nd line)
df = pd.read_csv(input_file, skiprows=1, low_memory=False)

# Clean header names
df.columns = [c.strip().replace('"', '') for c in df.columns]

# Parse timestamp safely
df["TIMESTAMP"] = pd.to_datetime(df["TMSTAMP"], errors="coerce")

# === DERIVED VARIABLES ===
df["WaterTemp_C"] = df.apply(lambda r: convert_pakbus_float(r["RawRegs(1)"], r["RawRegs(2)"]), axis=1)
df["DO_Percent"]  = df.apply(lambda r: convert_pakbus_float(r["RawRegs(3)"], r["RawRegs(4)"]), axis=1)
df["DO_mgL"]      = df.apply(lambda r: convert_pakbus_float(r["RawRegs(5)"], r["RawRegs(6)"]), axis=1)

# === SELECT LAST 5 ROWS & FORMAT ===
cols = ["TIMESTAMP", "DO_Percent", "DO_mgL", "WaterTemp_C"]
last5 = df[cols].tail(5).copy()

# Round numeric columns for display
for c in ["DO_Percent", "DO_mgL", "WaterTemp_C"]:
    last5[c] = last5[c].astype(float).round(2)

# Rename columns for professional display
last5 = last5.rename(columns={
    "TIMESTAMP": "Timestamp",
    "DO_Percent": "DO (%)",
    "DO_mgL": "DO (mg/L)",
    "WaterTemp_C": "Water Temp (°C)"
})

# Print as a formatted table
print(last5.to_string(index=False))

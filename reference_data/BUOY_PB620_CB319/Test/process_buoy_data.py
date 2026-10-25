import struct
import pandas as pd
from pathlib import Path

# === CONFIGURATION ===
input_file = Path("Kalamia_lagoon_buoy_do_test_Measurement.dat")
output_file = Path("output.dat")

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


def calculate_tds(raw_volt_tds, water_temp):
    """Compute TDS from voltage and water temperature."""
    try:
        rv = float(raw_volt_tds)
        wt = float(water_temp)
    except (TypeError, ValueError):
        return float('nan')

    compensation_coefficient = 1 + 0.02 * (wt - 25)
    compensation_voltage = rv / compensation_coefficient
    tds_value = (133.42 * compensation_voltage**3
                 - 255.86 * compensation_voltage**2
                 + 857.39 * compensation_voltage) * 0.5
    return tds_value


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
df["TDS"]         = df.apply(lambda r: calculate_tds(r["RawVoltTDS"], r["WaterTemp_C"]), axis=1)

# === WRITE OUTPUT ===
df.to_csv(output_file, index=False)
print(f"Processed {len(df)} rows → saved to {output_file}")

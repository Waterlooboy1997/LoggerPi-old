from pathlib import Path


CONFIG = {
    #"BASE_PATH": r"C:\Users\jc321293\Desktop\Github\RTWQ-data-display",
    #"PROCESSED_PATH": r"C:\Users\jc321293\Desktop\Github\RTWQ-data-display\processed",
    "BASE_PATH": r"C:\Users\JC321293\James Cook University\TWE - Catchment to Reef - General\Data Aquisition Server",
    "PROCESSED_PATH": r"C:\Users\JC321293\James Cook University\TWE - Catchment to Reef - General\Data Aquisition Server\processed",
    "SITES": ["ROS", "BEH", "DIX", "MRM", "UMR", "URO", "BSL", "BSU", "UBD", "FDF", "FDU", "JRD", "WCZ", "MBR", "BRD","LDW","WFC"],
    "RESAMPLE_RULE": "1D",
    "MEASUREMENT_LONG_SPAN_DAYS": 30,
    "DEFAULT_SITE": "HOME",
    "DEFAULT_TAB": "table"
}

CONFIG.setdefault(
    "CORA_CACHE_PATH",
    str(Path(CONFIG["PROCESSED_PATH"]) / "cora_cache.json"),
)
CONFIG.setdefault("CORA_WORKER_URL", "http://127.0.0.1:8765")
CONFIG.setdefault("CORA_REFRESH_COOLDOWN_SECONDS", 300)


SITE_RATING_CURVES = {
    # Example shape for future discharge/flux calculations (kept disabled by default).
    # "FDU": {
    #     "stage_col": "waterLevel",
    #     "stage_units": "m",
    #     "discharge_units": "m3/s",
    #     "equation": {
    #         "type": "poly",
    #         "coeffs": [0.0, 0.0, 0.0, 0.0],
    #         "min_stage": 0.0,
    #         "max_stage": 10.0,
    #     },
    # },
}

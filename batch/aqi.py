import numpy as np
import pandas as pd

# AQI values at each breakpoint
AQI_POINTS = [0, 50, 100, 200, 300, 400, 500]

# CPCB concentration breakpoints (the top value of each band is approximate)
BREAKPOINTS = {
    "PM2.5": [0, 30, 60, 90, 120, 250, 380],
    "PM10":  [0, 50, 100, 250, 350, 430, 510],
    "NO2":   [0, 40, 80, 180, 280, 400, 520],
    "SO2":   [0, 40, 80, 380, 800, 1600, 2400],
    "CO":    [0, 1, 2, 10, 17, 34, 51],
    "O3":    [0, 50, 100, 168, 208, 748, 1000],
    "NH3":   [0, 200, 400, 800, 1200, 1800, 2400],
}
POLLUTANTS = list(BREAKPOINTS)


def sub_index(values, pollutant):
    """Convert concentrations to a 0-500 sub-index (NaN stays NaN)."""
    arr = values.to_numpy(dtype=float)
    return np.interp(arr, BREAKPOINTS[pollutant], AQI_POINTS)


def compute_aqi(df):
    """AQI = max sub-index. CPCB rule: need >=3 pollutants, one of them PM2.5 or PM10."""
    sub = pd.DataFrame({p: sub_index(df[p], p) for p in POLLUTANTS}, index=df.index)
    enough = (sub.notna().sum(axis=1) >= 3) & sub[["PM2.5", "PM10"]].notna().any(axis=1)
    aqi = sub.max(axis=1).where(enough).round()
    main = sub.fillna(-1).idxmax(axis=1).where(aqi.notna())
    return aqi, main


def aqi_bucket(aqi):
    bins = [-np.inf, 50, 100, 200, 300, 400, np.inf]
    labels = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]
    return pd.cut(aqi, bins=bins, labels=labels)
LEVEL_NAMES = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]


def aqi_single(readings):
    """AQI for one reading dict. Returns (aqi, main_pollutant) or (None, None)."""
    sub = {p: float(np.interp(v, BREAKPOINTS[p], AQI_POINTS))
           for p, v in readings.items() if p in BREAKPOINTS and v is not None}
    if len(sub) < 3 or not ("PM2.5" in sub or "PM10" in sub):
        return None, None
    main = max(sub, key=sub.get)
    return round(sub[main]), main


def aqi_level(aqi):
    """0=Good ... 5=Severe"""
    for i, limit in enumerate([50, 100, 200, 300, 400]):
        if aqi <= limit:
            return i
    return 5
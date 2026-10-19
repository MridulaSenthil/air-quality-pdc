import numpy as np
from aqi import POLLUTANTS, compute_aqi, aqi_bucket


def process_station(df):
    """Clean one station's data and compute AQI. Independent of all other stations."""
    df = df.sort_values("Date").copy()

    for p in POLLUTANTS:                      # negative concentrations are sensor errors
        df.loc[df[p] < 0, p] = np.nan

    df[POLLUTANTS] = df[POLLUTANTS].interpolate(
        method="linear", limit=3, limit_area="inside"   # fill short gaps only
    )

    aqi, main = compute_aqi(df)
    df["AQI_calc"] = aqi
    df["Main_Pollutant"] = main
    df["AQI_Bucket_calc"] = aqi_bucket(df["AQI_calc"]).astype("object")
    return df
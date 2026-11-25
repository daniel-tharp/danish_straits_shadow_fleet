import os
import pandas as pd
import numpy as np

# Verify pyarrow is available for Parquet export (if missing, run: !pip install pyarrow)
import pyarrow

os.makedirs("data_parquet", exist_ok=True)

df_os = pd.read_csv("opensanctions_vessels.csv", dtype=str, low_memory=False)

# 1. Isolate VESSEL entities only (excludes 2,859 ORGANIZATION rows)
vessels = df_os[df_os["type"] == "VESSEL"].copy()

# 2. Strip literal 'IMO' prefix -> clean 7-digit string
vessels["imo_clean"] = vessels["imo"].str.replace("IMO", "", regex=False).str.strip()
vessels.loc[~vessels["imo_clean"].str.match(r"^\d{7}$", na=False), "imo_clean"] = np.nan

def join_unique_semicolon(series):
    vals = sorted(set(t.strip() for x in series.dropna() for t in str(x).split(";") if t.strip()))
    return ";".join(vals)

# 3. Aggregate to 1 row per unique vessel IMO
watchlist_imo = (
    vessels.dropna(subset=["imo_clean"])
    .groupby("imo_clean", as_index=False)
    .agg(
        os_name=("caption", "first"),
        os_risks=("risk", join_unique_semicolon),
        os_datasets=("datasets", join_unique_semicolon),
        os_countries=("countries", join_unique_semicolon),
        os_flags=("flag", join_unique_semicolon),
        os_mmsis=("mmsi", join_unique_semicolon),
        os_aliases=("aliases", join_unique_semicolon),
    )
)

watchlist_imo["is_sanctioned"] = watchlist_imo["os_risks"].str.contains(r"\bsanction\b", regex=True)
watchlist_imo["is_known_shadow"] = watchlist_imo["os_risks"].str.contains("mare.shadow", regex=False)
watchlist_imo["is_dark_fleet_known"] = watchlist_imo["is_sanctioned"] | watchlist_imo["is_known_shadow"]
watchlist_imo["is_detained_psc"] = watchlist_imo["os_risks"].str.contains("mare.detained", regex=False)
watchlist_imo["is_paris_banned"] = watchlist_imo["os_datasets"].str.contains("paris_mou_banned", regex=False)

# 4. Build exploded MMSI fallback lookup (catches sanctioned ships broadcasting blank/invalid IMOs)
mmsi_rows = vessels.dropna(subset=["mmsi"]).copy()
mmsi_rows["mmsi_clean"] = mmsi_rows["mmsi"].str.split(";")
mmsi_rows = mmsi_rows.explode("mmsi_clean")
mmsi_rows["mmsi_clean"] = mmsi_rows["mmsi_clean"].str.strip()
mmsi_rows = mmsi_rows[mmsi_rows["mmsi_clean"].str.match(r"^\d{9}$", na=False)]

watchlist_mmsi = (
    mmsi_rows.groupby("mmsi_clean", as_index=False)
    .agg(
        os_mmsi_risks=("risk", join_unique_semicolon),
        os_mmsi_datasets=("datasets", join_unique_semicolon),
        os_mmsi_imo=("imo_clean", "first"),
    )
)
watchlist_mmsi["mmsi_is_dark_fleet_known"] = (
    watchlist_mmsi["os_mmsi_risks"].str.contains(r"\bsanction\b", regex=True)
    | watchlist_mmsi["os_mmsi_risks"].str.contains("mare.shadow", regex=False)
)

watchlist_imo.to_parquet("data_parquet/watchlist_imo.parquet", index=False)
watchlist_mmsi.to_parquet("data_parquet/watchlist_mmsi.parquet", index=False)

print(f"Saved watchlist_imo.parquet  : {len(watchlist_imo):,} unique IMOs ({watchlist_imo['is_dark_fleet_known'].sum():,} sanctioned/shadow)")
print(f"Saved watchlist_mmsi.parquet : {len(watchlist_mmsi):,} unique MMSIs ({watchlist_mmsi['mmsi_is_dark_fleet_known'].sum():,} sanctioned/shadow)")

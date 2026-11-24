# Generated from: 02_run_event_study_pipeline.ipynb
# Converted at: 2026-11-24T19:24:18.405Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

"""
run_event_study_pipeline.py
24-Month Stratified Event Study (1 Week/Month: Days 10-16, Jul 2024 - Jun 2026)
- Streams daily DMA AIS zips -> 1-min Tanker/Pilot Parquet -> auto-deletes raw zips
- Runs cKDTree Great Belt Pilotage & Tanker-to-Tanker Bunkering month-by-month (low RAM)
- Fully resumable: skips any month already written to es_results/
"""

import os
import gc
import glob
import time
import zipfile
import requests
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

# ==============================================================================
# CONFIGURATION
# ==============================================================================
START_MONTH = "2024-07"   # 7 pre-Wave-1 mos (Jul 24 - Jan 25)
END_MONTH   = "2026-06"   # 8 Wave-1 mos (Feb-Sep 25) + 9 Post-Wave-2 mos (Oct 25 - Jun 26)
DAYS_OF_MONTH = list(range(10, 17))  # Fixed mid-month rule: Days 10-16

POST_DIR = "data_parquet"            # Directory containing OpenSanctions watchlist parquets
TMP_ZIP_DIR = "tmp_es_zips"
ES_PARQUET_DIR = "data_parquet_es"
ES_RESULTS_DIR = "es_results"

DELETE_ZIP_AFTER_ETL = True          # Keeps disk footprint under ~3 GB total

URL_TEMPLATES = [
    "http://aisdata.ais.dk/{year}/aisdk-{date}.zip",
    "https://web.ais.dk/aisdata/aisdk-{date}.zip",
    "http://web.ais.dk/aisdata/aisdk-{date}.zip",
    "http://aisdata.ais.dk/aisdk-{date}.zip",
]

USE_COLS = [
    "# Timestamp", "MMSI", "Latitude", "Longitude", "SOG", "COG",
    "IMO", "Name", "Ship type", "Length", "Draught", "Destination"
]

STAR_MARINE_IMOS = {
    "9388728", "9388807", "9313486", "9290438", "9411185",
    "9258480", "9489285", "9489297", "9297321", "9285861"
}

os.makedirs(TMP_ZIP_DIR, exist_ok=True)
os.makedirs(ES_PARQUET_DIR, exist_ok=True)
os.makedirs(ES_RESULTS_DIR, exist_ok=True)


# ==============================================================================
# HELPERS
# ==============================================================================
def load_watchlists():
    dark_imos, dark_mmsis = set(), set()
    for wf in glob.glob(os.path.join(POST_DIR, "*watchlist*.parquet")):
        wdf = pd.read_parquet(wf)
        flag_cols = [c for c in wdf.columns if any(k in c.lower() for k in ("dark", "sanction", "shadow"))]
        if not flag_cols:
            continue
        mask = pd.Series(False, index=wdf.index)
        for fc in flag_cols:
            mask |= wdf[fc].fillna(False).astype(bool)
        for ic in [c for c in wdf.columns if "imo" in c.lower() and c not in flag_cols]:
            dark_imos |= set(wdf.loc[mask, ic].dropna().astype(str).str.replace(r"\.0$", "", regex=True))
        for mc in [c for c in wdf.columns if "mmsi" in c.lower() and c not in flag_cols]:
            dark_mmsis |= set(wdf.loc[mask, mc].dropna().astype(str).str.replace(r"\.0$", "", regex=True))
    return dark_imos, dark_mmsis


def resolve_dma_url(date_str):
    year = date_str[:4]
    for tmpl in URL_TEMPLATES:
        url = tmpl.format(year=year, date=date_str)
        try:
            r = requests.head(url, timeout=15, allow_redirects=True)
            if r.status_code == 200:
                return url
        except requests.RequestException:
            continue
    return None


def download_file(url, dest_path, retries=3):
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 5_000_000:
        return True
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=90) as r:
                r.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            return True
        except Exception as e:
            print(f"    [WARN] Download attempt {attempt} failed ({e}); retrying...")
            time.sleep(5 * attempt)
    return False


def clean_imo(series):
    s = series.astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    return np.where(s.str.fullmatch(r"\d{7}"), s, None)


def first_valid(s):
    s = s.dropna()
    s = s[s.astype(str).str.strip() != ""]
    return s.mode().iloc[0] if not s.empty else None


def join_dests(s):
    vals = sorted(set(str(x).strip() for x in s.dropna() if str(x).strip() not in ("", "nan", "None")))
    return ";".join(vals[:5])


# ==============================================================================
# STEP 1: DAILY ETL FOR ONE MONTH
# ==============================================================================
def etl_month(ym_str):
    month_dir = os.path.join(ES_PARQUET_DIR, ym_str)
    t_dir = os.path.join(month_dir, "tankers")
    p_dir = os.path.join(month_dir, "pilots")
    os.makedirs(t_dir, exist_ok=True)
    os.makedirs(p_dir, exist_ok=True)

    dates = [f"{ym_str}-{d:02d}" for d in DAYS_OF_MONTH]
    ok_days = 0

    for d_str in dates:
        out_t = os.path.join(t_dir, f"{d_str}.parquet")
        out_p = os.path.join(p_dir, f"{d_str}.parquet")
        if os.path.exists(out_t) and os.path.exists(out_p):
            ok_days += 1
            continue

        url = resolve_dma_url(d_str)
        if not url:
            print(f"  [MISS] No daily zip found for {d_str} (skipping day).")
            continue

        zip_path = os.path.join(TMP_ZIP_DIR, f"aisdk-{d_str}.zip")
        if not download_file(url, zip_path):
            print(f"  [FAIL] Could not download {d_str}.")
            continue

        t0 = time.time()
        t_parts, p_parts = [], []
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                csv_name = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]
                with zf.open(csv_name) as f:
                    for chunk in pd.read_csv(f, usecols=lambda c: c in USE_COLS, chunksize=500_000, low_memory=False):
                        st = chunk["Ship type"].astype(str)
                        sub = chunk[st.isin(["Tanker", "Pilot"])].copy()
                        if sub.empty:
                            continue

                        sub["timestamp"] = pd.to_datetime(
                            sub["# Timestamp"], format="%d/%m/%Y %H:%M:%S", errors="coerce"
                        ).dt.floor("min")
                        sub = sub.dropna(subset=["timestamp", "MMSI", "Latitude", "Longitude"])
                        sub = sub[sub["Latitude"].between(53.5, 59.5) & sub["Longitude"].between(5.0, 16.0)]
                        sub["mmsi"] = sub["MMSI"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
                        sub = sub.drop_duplicates(subset=["mmsi", "timestamp"], keep="first")

                        sub["block_id"] = ym_str
                        sub["imo_clean"] = clean_imo(sub["IMO"]) if "IMO" in sub.columns else None
                        for num_c in ["Latitude", "Longitude", "SOG", "COG", "Length", "Draught"]:
                            if num_c in sub.columns:
                                sub[num_c] = pd.to_numeric(sub[num_c], errors="coerce")

                        sub = sub.drop(columns=["# Timestamp", "MMSI", "IMO"], errors="ignore")
                        t_sub = sub[sub["Ship type"] == "Tanker"]
                        p_sub = sub[sub["Ship type"] == "Pilot"]
                        if not t_sub.empty:
                            t_parts.append(t_sub)
                        if not p_sub.empty:
                            p_parts.append(p_sub)

            if t_parts:
                pd.concat(t_parts, ignore_index=True).drop_duplicates(["mmsi", "timestamp"]).to_parquet(out_t, index=False)
            if p_parts:
                pd.concat(p_parts, ignore_index=True).drop_duplicates(["mmsi", "timestamp"]).to_parquet(out_p, index=False)
            ok_days += 1
            print(f"    [ETL] {d_str} done in {time.time()-t0:.1f}s")
        except Exception as e:
            print(f"    [ERR] Corrupt/failed parse on {d_str}: {e}")
        finally:
            if DELETE_ZIP_AFTER_ETL and os.path.exists(zip_path):
                os.remove(zip_path)

    return ok_days, t_dir, p_dir


# ==============================================================================
# STEP 2: MONTH-BY-MONTH ANALYTICS (PILOTAGE + BUNKERING)
# ==============================================================================
def analyze_month(ym_str, t_dir, p_dir, dark_imos, dark_mmsis):
    panel_out = os.path.join(ES_RESULTS_DIR, f"panel_{ym_str}.parquet")
    rt_out    = os.path.join(ES_RESULTS_DIR, f"route_t_{ym_str}.parquet")
    bunk_out  = os.path.join(ES_RESULTS_DIR, f"bunkering_{ym_str}.parquet")

    df_t = pd.read_parquet(t_dir)
    df_p = pd.read_parquet(p_dir)
    df_t["timestamp"] = pd.to_datetime(df_t["timestamp"])
    df_p["timestamp"] = pd.to_datetime(df_p["timestamp"])

    df_t["is_slow"] = df_t["SOG"] <= 1.5
    df_t["is_skagen_slow"] = (
        df_t["is_slow"] & df_t["Latitude"].between(57.55, 57.95) & df_t["Longitude"].between(10.45, 11.10)
    )

    panel = (
        df_t.groupby(["block_id", "mmsi"], as_index=False)
        .agg(
            imo_clean=("imo_clean", first_valid),
            name=("Name", first_valid),
            max_draught=("Draught", "max"),
            length=("Length", "median"),
            ping_count=("timestamp", "count"),
            slow_mins=("is_slow", "sum"),
            skagen_slow_mins=("is_skagen_slow", "sum"),
            destinations=("Destination", join_dests),
        )
    )
    panel["year_month"] = ym_str
    panel["is_deep_draught"] = (panel["max_draught"].fillna(0) >= 11.0).astype(int)
    panel["stopped_60m"] = (panel["slow_mins"] >= 60).astype(int)
    panel["skagen_stopped_60m"] = (panel["skagen_slow_mins"] >= 60).astype(int)
    panel["mid"] = panel["mmsi"].str.slice(0, 3)
    panel["ever_dark"] = (panel["imo_clean"].isin(dark_imos) | panel["mmsi"].isin(dark_mmsis)).astype(int)
    panel["is_star_marine"] = panel["imo_clean"].isin(STAR_MARINE_IMOS).astype(int)
    panel["ever_shadow_all"] = ((panel["ever_dark"] == 1) | (panel["is_star_marine"] == 1)).astype(int)
    panel["vessel_id"] = panel["imo_clean"].fillna("MMSI:" + panel["mmsi"])

    # --- A. Great Belt Pilotage Engine ---
    b_start, b_end = df_t["timestamp"].min(), df_t["timestamp"].max()
    deep_mmsis = set(panel.loc[panel["is_deep_draught"] == 1, "mmsi"])
    df_deep = df_t[df_t["mmsi"].isin(deep_mmsis)]

    in_gb = (
        df_deep["Latitude"].between(55.10, 55.65) &
        df_deep["Longitude"].between(10.60, 11.35) &
        (df_deep["SOG"] >= 2.0)
    )
    gb_stats = (
        df_deep[in_gb].sort_values("timestamp")
        .groupby(["block_id", "mmsi"], as_index=False)
        .agg(
            gb_pings_count=("timestamp", "count"),
            gb_mid_ts=("timestamp", "median"),
            gb_first_lat=("Latitude", "first"),
            gb_last_lat=("Latitude", "last"),
            gb_max_draught=("Draught", "max"),
        )
    )
    gb_stats = gb_stats[gb_stats["gb_pings_count"] >= 15].copy()

    if not gb_stats.empty:
        gb_stats["lat_delta"] = gb_stats["gb_last_lat"] - gb_stats["gb_first_lat"]
        gb_stats["direction"] = np.where(gb_stats["lat_delta"] > 0.05, "Northbound (Baltic Exit)",
                                np.where(gb_stats["lat_delta"] < -0.05, "Southbound (Baltic Entry)", "Ambiguous/Local"))
        gb_stats["gb_is_deep"] = gb_stats["gb_max_draught"] >= 11.0
        gb_stats["is_clean_interior_transit"] = (
            (gb_stats["gb_mid_ts"] >= b_start + pd.Timedelta(hours=18)) &
            (gb_stats["gb_mid_ts"] <= b_end - pd.Timedelta(hours=18))
        )

        m_lat = 111_320.0
        m_lon = 111_320.0 * np.cos(np.deg2rad(56.0))
        dk_p = df_p[
            df_p["mmsi"].str.startswith(("219", "220")) |
            df_p["Name"].fillna("").str.contains("DANPILOT|LODS", case=False, regex=True)
        ].copy()
        dk_p["x_m"] = dk_p["Longitude"] * m_lon
        dk_p["y_m"] = dk_p["Latitude"] * m_lat
        dp_by_ts = {ts: g[["x_m", "y_m"]].to_numpy() for ts, g in dk_p.groupby("timestamp", sort=False)}

        rt_pings = df_deep[df_deep["mmsi"].isin(set(gb_stats["mmsi"]))].copy()
        rt_pings["x_m"] = rt_pings["Longitude"] * m_lon
        rt_pings["y_m"] = rt_pings["Latitude"] * m_lat

        prox_rows = []
        for ts, tg in rt_pings.groupby("timestamp", sort=False):
            p_xy = dp_by_ts.get(ts)
            if p_xy is None or len(p_xy) == 0:
                continue
            dists, _ = cKDTree(p_xy).query(tg[["x_m", "y_m"]].to_numpy(), k=1)
            mask = dists <= 1500.0
            if np.any(mask):
                for m, d in zip(tg["mmsi"].to_numpy()[mask], dists[mask]):
                    prox_rows.append((m, float(d)))

        route_t = gb_stats.merge(panel, on=["block_id", "mmsi"], how="left")
        if prox_rows:
            p_agg = (
                pd.DataFrame(prox_rows, columns=["mmsi", "dist_dk_m"])
                .groupby("mmsi", as_index=False)
                .agg(
                    min_dk_pilot_dist_m=("dist_dk_m", "min"),
                    mins_dk_within_600m=("dist_dk_m", lambda s: int((s <= 600.0).sum())),
                )
            )
            route_t = route_t.merge(p_agg, on="mmsi", how="left")
        else:
            route_t["min_dk_pilot_dist_m"] = np.inf
            route_t["mins_dk_within_600m"] = 0

        route_t["min_dk_pilot_dist_m"] = route_t["min_dk_pilot_dist_m"].fillna(np.inf)
        route_t["mins_dk_within_600m"] = route_t["mins_dk_within_600m"].fillna(0).astype(int)
        route_t["took_dk_pilot"] = ((route_t["min_dk_pilot_dist_m"] <= 450.0) & (route_t["mins_dk_within_600m"] >= 2)).astype(int)
        route_t["refused_dk_pilot"] = (1 - route_t["took_dk_pilot"]).astype(int)
        route_t.to_parquet(rt_out, index=False)

    # --- B. All-to-All Tanker Bunkering Scan ---
    slow_t = df_t[df_t["is_slow"]].copy()
    mb_lat = 111_320.0
    mb_lon = 111_320.0 * np.cos(np.deg2rad(56.5))
    slow_t["x_m"] = slow_t["Longitude"] * mb_lon
    slow_t["y_m"] = slow_t["Latitude"] * mb_lat

    tt_rows = []
    for ts, g in slow_t.groupby("timestamp", sort=False):
        if len(g) < 2:
            continue
        pairs = cKDTree(g[["x_m", "y_m"]].to_numpy()).query_pairs(r=250.0, output_type="ndarray")
        if len(pairs) == 0:
            continue
        mmsis = g["mmsi"].to_numpy()
        lats = g["Latitude"].to_numpy()
        lons = g["Longitude"].to_numpy()
        for i, j in pairs:
            m1, m2 = mmsis[i], mmsis[j]
            if m1 == m2:
                continue
            if m1 > m2:
                m1, m2 = m2, m1
            tt_rows.append((ym_str, m1, m2, float(lats[i]), float(lons[i])))

    if tt_rows:
        tt_enc = (
            pd.DataFrame(tt_rows, columns=["year_month", "mmsi_1", "mmsi_2", "lat", "lon"])
            .groupby(["year_month", "mmsi_1", "mmsi_2"], as_index=False)
            .agg(overlap_mins=("lat", "count"), lat=("lat", "median"), lon=("lon", "median"))
        )
        tt_enc = tt_enc[tt_enc["overlap_mins"] >= 60].copy()
        tt_enc["in_skagen"] = (tt_enc["lat"].between(57.55, 57.95) & tt_enc["lon"].between(10.45, 11.10)).astype(int)
        m_meta = panel[["mmsi", "imo_clean", "name", "length", "ever_dark", "is_star_marine", "ever_shadow_all"]]
        tt_enc = tt_enc.merge(m_meta.add_suffix("_1"), on="mmsi_1", how="left").merge(m_meta.add_suffix("_2"), on="mmsi_2", how="left")
        tt_enc["involves_dark"] = ((tt_enc["ever_dark_1"] == 1) | (tt_enc["ever_dark_2"] == 1)).astype(int)
        tt_enc.to_parquet(bunk_out, index=False)

        b_keys = pd.concat([
            tt_enc[["mmsi_1", "in_skagen"]].rename(columns={"mmsi_1": "mmsi"}),
            tt_enc[["mmsi_2", "in_skagen"]].rename(columns={"mmsi_2": "mmsi"}),
        ], ignore_index=True)
        b_flag = b_keys.groupby("mmsi", as_index=False).agg(bunkered_in_dk=("in_skagen", lambda s: 1), bunkered_at_skagen=("in_skagen", "max"))
        panel = panel.merge(b_flag, on="mmsi", how="left")
    else:
        panel["bunkered_in_dk"] = 0
        panel["bunkered_at_skagen"] = 0

    panel["bunkered_in_dk"] = panel["bunkered_in_dk"].fillna(0).astype(int)
    panel["bunkered_at_skagen"] = panel["bunkered_at_skagen"].fillna(0).astype(int)
    panel.to_parquet(panel_out, index=False)

    # Free RAM cleanly before next month
    del df_t, df_p, df_deep, slow_t
    gc.collect()


# ==============================================================================
# MAIN LOOP
# ==============================================================================
if __name__ == "__main__":
    t_start = time.time()
    dark_imos, dark_mmsis = load_watchlists()
    print(f"Loaded fixed 2026 watchlist -> IMOs: {len(dark_imos):,} | MMSIs: {len(dark_mmsis):,}")

    months = [str(m) for m in pd.period_range(START_MONTH, END_MONTH, freq="M")]
    print(f"Starting 24-month Event Study ({months[0]} -> {months[-1]}, {len(months)*7} days)")

    for idx, ym in enumerate(months, 1):
        panel_out = os.path.join(ES_RESULTS_DIR, f"panel_{ym}.parquet")
        if os.path.exists(panel_out):
            print(f"[{idx:02d}/{len(months)}] {ym} already analyzed -> skipping.")
            continue

        print(f"\n[{idx:02d}/{len(months)}] Processing {ym} (days {DAYS_OF_MONTH[0]}-{DAYS_OF_MONTH[-1]})...")
        ok_days, t_dir, p_dir = etl_month(ym)
        if ok_days < 4:
            print(f"  [WARN] Only {ok_days}/7 days available for {ym}; skipping analytics.")
            continue
        analyze_month(ym, t_dir, p_dir, dark_imos, dark_mmsis)
        print(f"  [DONE] {ym} saved to {ES_RESULTS_DIR}/ ({ok_days}/7 days)")

    # Concatenate all monthly outputs into unified Event-Study master tables
    for prefix in ["panel", "route_t", "bunkering"]:
        files = sorted(glob.glob(os.path.join(ES_RESULTS_DIR, f"{prefix}_*.parquet")))
        if files:
            df_all = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
            out_path = os.path.join(ES_RESULTS_DIR, f"MASTER_ES_{prefix}.parquet")
            df_all.to_parquet(out_path, index=False)
            print(f"Wrote {out_path} -> {len(df_all):,} rows")

    print(f"\n=== ALL DONE IN {(time.time()-t_start)/60:.1f} MINUTES ===")
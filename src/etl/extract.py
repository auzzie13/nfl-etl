import os
import nfl_data_py as nfl
import pandas as pd
from datetime import date

RAW_DIR = "raw/"

def extract_pbp_years(years: list[int]):
    # Optionally cache
    # nfl.cache_pbp(years, downcast=True)
    print("Fetching pbp for", years)
    pbp_df = nfl.import_pbp_data(years=years, columns=None, downcast=True)
    out_path = f"{RAW_DIR}pbp_{years[0]}.parquet"
    pbp_df.to_parquet(out_path, index=False)
    print("Saved:", out_path)
    return out_path

def extract_rosters(years: list[int]):
    rosters = nfl.import_seasonal_rosters(years)
    rosters.to_parquet(f"{RAW_DIR}rosters_{years[0]}.parquet", index=False)
    return f"{RAW_DIR}rosters_{years[0]}.parquet"

def extract_injuries(years: list[int]):
    inj = nfl.import_injuries(years)
    inj.to_parquet(f"{RAW_DIR}injuries_{years[0]}.parquet", index=False)
    return f"{RAW_DIR}injuries_{years[0]}.parquet"

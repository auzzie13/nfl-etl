# src/etl/load.py
import os
import sys
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# --- CONFIG (use environment variable in production) ---
DB_URL = os.getenv("DB_URL", "mysql+pymysql://nfl_user:nfl_pass@127.0.0.1:3307/nfl_dw")
engine = create_engine(DB_URL, pool_pre_ping=True)

TRANSFORMED_DIR = "transformed/"

# ---------- helper ----------
def ensure_file(path):
    if not os.path.exists(path):
        raise FileNotFoundError(path)

def write_staging(df: pd.DataFrame, table_name: str, if_exists="replace", chunksize=1000):
    """Write a dataframe to a database staging table. Use chunksize for large tables."""
    print(f"Writing {len(df):,} rows to {table_name} (if_exists={if_exists})")
    df.to_sql(table_name, engine, if_exists=if_exists, index=False, method="multi", chunksize=chunksize)
    print(f"Wrote staging table: {table_name}")

# ---------- upsert SQLs ----------
def upsert_players_from_stg():
    sql = """
    INSERT INTO dim_player (player_id, first_name, last_name, full_name, position, birth_date)
    SELECT player_id, first_name, last_name, full_name, position, birth_date
    FROM stg_players
    ON DUPLICATE KEY UPDATE
      first_name = VALUES(first_name),
      last_name = VALUES(last_name),
      full_name = VALUES(full_name),
      position = VALUES(position),
      birth_date = VALUES(birth_date);
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("Upserted dim_player from stg_players")

def upsert_player_games_from_stg():
    sql = """
    INSERT INTO fact_player_game (
      game_id, player_id, team_id, opponent_team_id, date_key,
      pass_attempts, pass_completions, pass_yards, pass_tds,
      rush_attempts, rush_yards, rush_tds,
      receptions, rec_yards, rec_tds,
      fumbles, fantasy_points
    )
    SELECT
      game_id, player_id, team_id, opponent_team_id, date_key,
      pass_attempts, pass_completions, pass_yards, pass_tds,
      rush_attempts, rush_yards, rush_tds,
      receptions, rec_yards, rec_tds,
      fumbles, fantasy_points
    FROM stg_player_game
    ON DUPLICATE KEY UPDATE
      pass_attempts = VALUES(pass_attempts),
      pass_completions = VALUES(pass_completions),
      pass_yards = VALUES(pass_yards),
      pass_tds = VALUES(pass_tds),
      rush_attempts = VALUES(rush_attempts),
      rush_yards = VALUES(rush_yards),
      rush_tds = VALUES(rush_tds),
      receptions = VALUES(receptions),
      rec_yards = VALUES(rec_yards),
      rec_tds = VALUES(rec_tds),
      fumbles = VALUES(fumbles),
      fantasy_points = VALUES(fantasy_points);
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("Upserted fact_player_game from stg_player_game")

def update_meta_run_state(pipeline_name: str, last_game_date: str | None = None):
    sql = """
    INSERT INTO meta_run_state (pipeline_name, last_run_at, last_game_date)
    VALUES (:pipeline, NOW(), :last_game_date)
    ON DUPLICATE KEY UPDATE
      last_run_at = NOW(),
      last_game_date = VALUES(last_game_date);
    """
    with engine.begin() as conn:
        conn.execute(text(sql), {"pipeline": pipeline_name, "last_game_date": last_game_date})
    print("Updated meta_run_state")

# ---------- main ----------
def run_load(year: int, skip_injuries=False):
    # paths
    players_path = os.path.join(TRANSFORMED_DIR, "dim_players.parquet")
    player_game_path = os.path.join(TRANSFORMED_DIR, "fact_player_game.parquet")
    # sanity check files
    ensure_file(players_path)
    ensure_file(player_game_path)

    players_df = pd.read_parquet(players_path)
    pg_df = pd.read_parquet(player_game_path)

    # optional: compute last_game_date from pg_df (if you have date_key or game_date)
    # If using date_key as YYYYMMDD integer:
    if "date_key" in pg_df.columns and len(pg_df) > 0:
        max_date_key = int(pg_df["date_key"].max())
        last_game_date = f"{str(max_date_key)[:4]}-{str(max_date_key)[4:6]}-{str(max_date_key)[6:]}"
    else:
        last_game_date = None

    # Write staging
    write_staging(players_df, "stg_players")
    write_staging(pg_df, "stg_player_game")

    # Upsert into production tables
    upsert_players_from_stg()
    upsert_player_games_from_stg()

    # Update meta
    update_meta_run_state("pbp", last_game_date)

    print("Load complete")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=False, help="season year", default=None)
    parser.add_argument("--skip-injuries", action="store_true")
    args = parser.parse_args()

    # no year argument required because transform produced files in transformed/
    try:
        run_load(args.year or 0, skip_injuries=args.skip_injuries)
    except FileNotFoundError as e:
        print("File missing:", e)
        sys.exit(2)
    except SQLAlchemyError as e:
        print("Database error:", e)
        sys.exit(3)

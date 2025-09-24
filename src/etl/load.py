# src/etl/load.py
import os
import sys
import argparse
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional

# --- CONFIG (use environment variable in production) ---
DB_URL = os.getenv("DB_URL", "mysql+pymysql://nfl_user:nfl_pass@127.0.0.1:3307/nfl_dw")
engine = create_engine(DB_URL, pool_pre_ping=True)

TRANSFORMED_DIR = "transformed/"

# ---------- HELPER FUNCTIONS ----------
def ensure_file(path: str) -> None:
    """Check if file exists, raise error if not."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Required file not found: {path}")

def write_staging(df: pd.DataFrame, table_name: str, if_exists: str = "replace", chunksize: int = 1000) -> None:
    """Write a dataframe to a database staging table."""
    print(f"Writing {len(df):,} rows to {table_name} (if_exists={if_exists})")
    try:
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, method="multi", chunksize=chunksize)
        print(f"✅ Successfully wrote to staging table: {table_name}")
    except Exception as e:
        print(f"❌ Error writing to {table_name}: {e}")
        raise

# ---------- DATA PREPARATION FUNCTIONS ----------
def prepare_players_for_staging(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare players data for staging table with proper data types."""
    df_clean = df.copy()
    
    # Ensure player_id is string (matches our VARCHAR schema)
    df_clean['player_id'] = df_clean['player_id'].astype(str)
    
    # Clean up any null values in critical fields
    df_clean = df_clean.dropna(subset=['player_id'])
    df_clean['first_name'] = df_clean['first_name'].fillna('')
    df_clean['last_name'] = df_clean['last_name'].fillna('')
    df_clean['full_name'] = df_clean['full_name'].fillna('')
    df_clean['position'] = df_clean['position'].fillna('UNK')
    
    # Ensure we only have the columns that exist in staging table
    staging_cols = ['player_id', 'first_name', 'last_name', 'full_name', 'position', 'birth_date']
    available_cols = [col for col in staging_cols if col in df_clean.columns]
    df_clean = df_clean[available_cols]
    
    print(f"✅ Prepared {len(df_clean)} player records for staging")
    return df_clean

def prepare_player_game_for_staging(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare player game data for staging table with proper data types."""
    df_clean = df.copy()
    
    # Ensure IDs are strings (matches our VARCHAR schema)
    df_clean['game_id'] = df_clean['game_id'].astype(str)
    df_clean['player_id'] = df_clean['player_id'].astype(str)
    
    # Remove records with null keys
    df_clean = df_clean.dropna(subset=['game_id', 'player_id'])
    
    # Rename columns to match staging table
    column_mapping = {
        'completions': 'completions',  # Already correct
        'pass_completions': 'completions',  # In case it comes with this name
    }
    df_clean = df_clean.rename(columns=column_mapping)
    
    # Ensure all numeric columns exist and are properly typed
    numeric_columns = [
        'pass_attempts', 'completions', 'pass_yards', 'pass_tds', 'interception',
        'rush_attempts', 'rush_yards', 'rush_tds', 
        'receptions', 'rec_yards', 'rec_tds', 'total_tds', 'fumbles'
    ]
    
    for col in numeric_columns:
        if col not in df_clean.columns:
            df_clean[col] = 0
        else:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0).astype(int)
    
    # Select only the columns that exist in staging table
    staging_cols = ['game_id', 'player_id'] + numeric_columns
    available_cols = [col for col in staging_cols if col in df_clean.columns]
    df_clean = df_clean[available_cols]
    
    print(f"✅ Prepared {len(df_clean)} player game records for staging")
    return df_clean

def prepare_injuries_for_staging(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare injury data for staging table."""
    if df.empty:
        return df
    
    df_clean = df.copy()
    
    # Ensure player_id is string
    df_clean['player_id'] = df_clean['player_id'].astype(str)
    
    # Clean up data types
    if 'season' in df_clean.columns:
        df_clean['season'] = pd.to_numeric(df_clean['season'], errors='coerce')
    if 'week' in df_clean.columns:
        df_clean['week'] = pd.to_numeric(df_clean['week'], errors='coerce')
    
    # Remove rows with null keys
    df_clean = df_clean.dropna(subset=['player_id'])
    
    print(f"✅ Prepared {len(df_clean)} injury records for staging")
    return df_clean

# ---------- UPSERT FUNCTIONS ----------
def upsert_players_from_stg() -> None:
    """Upsert players from staging to dimension table."""
    sql = """
    INSERT INTO dim_player (
        player_id, first_name, last_name, full_name, position, birth_date, active
    )
    SELECT 
        player_id, first_name, last_name, full_name, position, birth_date, 1 as active
    FROM stg_players
    WHERE player_id IS NOT NULL
    ON DUPLICATE KEY UPDATE
        first_name = VALUES(first_name),
        last_name = VALUES(last_name),
        full_name = VALUES(full_name),
        position = VALUES(position),
        birth_date = VALUES(birth_date),
        active = VALUES(active),
        updated_at = CURRENT_TIMESTAMP;
    """
    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            print(f"✅ Upserted players into dim_player (rows affected: {result.rowcount})")
    except Exception as e:
        print(f"❌ Error upserting players: {e}")
        raise

def upsert_player_games_from_stg() -> None:
    """Upsert player game stats from staging to fact table."""
    sql = """
    INSERT INTO fact_player_game (
        game_id, player_id, pass_attempts, pass_completions, pass_yards, pass_tds, interception,
        rush_attempts, rush_yards, rush_tds, receptions, rec_yards, rec_tds, total_tds, fumbles
    )
    SELECT
        game_id, player_id, 
        COALESCE(pass_attempts, 0),
        COALESCE(completions, 0) as pass_completions,
        COALESCE(pass_yards, 0),
        COALESCE(pass_tds, 0),
        COALESCE(interception, 0),
        COALESCE(rush_attempts, 0),
        COALESCE(rush_yards, 0),
        COALESCE(rush_tds, 0),
        COALESCE(receptions, 0),
        COALESCE(rec_yards, 0),
        COALESCE(rec_tds, 0),
        COALESCE(total_tds, 0),
        COALESCE(fumbles, 0)
    FROM stg_player_game
    WHERE game_id IS NOT NULL AND player_id IS NOT NULL
    ON DUPLICATE KEY UPDATE
        pass_attempts = VALUES(pass_attempts),
        pass_completions = VALUES(pass_completions),
        pass_yards = VALUES(pass_yards),
        pass_tds = VALUES(pass_tds),
        interception = VALUES(interception),
        rush_attempts = VALUES(rush_attempts),
        rush_yards = VALUES(rush_yards),
        rush_tds = VALUES(rush_tds),
        receptions = VALUES(receptions),
        rec_yards = VALUES(rec_yards),
        rec_tds = VALUES(rec_tds),
        total_tds = VALUES(total_tds),
        fumbles = VALUES(fumbles),
        updated_at = CURRENT_TIMESTAMP;
    """
    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            print(f"✅ Upserted player game stats into fact_player_game (rows affected: {result.rowcount})")
    except Exception as e:
        print(f"❌ Error upserting player game stats: {e}")
        raise

def upsert_injuries_from_stg() -> None:
    """Upsert injury data from staging to fact table."""
    sql = """
    INSERT IGNORE INTO fact_player_injury (
        player_id, season, week, injury_type, status
    )
    SELECT
        player_id, season, week, injury_type, status
    FROM stg_injuries
    WHERE player_id IS NOT NULL;
    """
    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
            print(f"✅ Inserted injury records into fact_player_injury (rows affected: {result.rowcount})")
    except Exception as e:
        print(f"❌ Error inserting injuries: {e}")
        raise

def update_meta_run_state(pipeline_name: str, last_game_date: Optional[str] = None) -> None:
    """Update pipeline metadata."""
    sql = """
    INSERT INTO meta_run_state (pipeline_name, last_run_at, last_game_date)
    VALUES (:pipeline, NOW(), :last_game_date)
    ON DUPLICATE KEY UPDATE
        last_run_at = NOW(),
        last_game_date = VALUES(last_game_date),
        updated_at = CURRENT_TIMESTAMP;
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(sql), {"pipeline": pipeline_name, "last_game_date": last_game_date})
            print(f"✅ Updated meta_run_state for pipeline: {pipeline_name}")
    except Exception as e:
        print(f"❌ Error updating meta_run_state: {e}")
        raise

# ---------- MAIN LOAD FUNCTION ----------
def run_load(year: int, skip_injuries: bool = False) -> None:
    """Run the complete load process."""
    print(f"🚀 Starting load process for year {year}")
    print("=" * 50)
    
    # Define file paths
    players_path = os.path.join(TRANSFORMED_DIR, "dim_players.parquet")
    player_game_path = os.path.join(TRANSFORMED_DIR, "fact_player_game.parquet")
    injuries_path = os.path.join(TRANSFORMED_DIR, "fact_player_injury.parquet")
    
    # Check required files
    ensure_file(players_path)
    ensure_file(player_game_path)
    
    try:
        # Load and prepare data
        print("📥 Loading transformed data...")
        players_df = pd.read_parquet(players_path)
        pg_df = pd.read_parquet(player_game_path)
        
        print(f"   Players: {len(players_df):,} records")
        print(f"   Player games: {len(pg_df):,} records")
        
        # Prepare data for staging
        print("🔧 Preparing data for staging...")
        players_df = prepare_players_for_staging(players_df)
        pg_df = prepare_player_game_for_staging(pg_df)
        
        # Calculate last game date for metadata
        last_game_date = None
        if "date_key" in pg_df.columns and len(pg_df) > 0:
            try:
                max_date_key = int(pg_df["date_key"].max())
                last_game_date = f"{str(max_date_key)[:4]}-{str(max_date_key)[4:6]}-{str(max_date_key)[6:]}"
            except:
                pass
        
        # Write to staging tables
        print("💾 Writing to staging tables...")
        write_staging(players_df, "stg_players")
        write_staging(pg_df, "stg_player_game")
        
        # Handle injuries if file exists and not skipped
        if not skip_injuries and os.path.exists(injuries_path):
            try:
                injuries_df = pd.read_parquet(injuries_path)
                if not injuries_df.empty:
                    print(f"   Injuries: {len(injuries_df):,} records")
                    injuries_df = prepare_injuries_for_staging(injuries_df)
                    write_staging(injuries_df, "stg_injuries")
            except Exception as e:
                print(f"⚠️  Warning: Could not process injuries: {e}")
        
        # Upsert into production tables
        print("🔄 Upserting into production tables...")
        upsert_players_from_stg()
        upsert_player_games_from_stg()
        
        if not skip_injuries and os.path.exists(injuries_path):
            try:
                upsert_injuries_from_stg()
            except Exception as e:
                print(f"⚠️  Warning: Could not upsert injuries: {e}")
        
        # Update pipeline metadata
        print("📝 Updating metadata...")
        update_meta_run_state("nfl_pbp_load", last_game_date)
        
        print("\n🎉 Load process completed successfully!")
        
        # Show summary
        with engine.connect() as conn:
            player_count = conn.execute(text("SELECT COUNT(*) FROM dim_player")).fetchone()[0]
            game_count = conn.execute(text("SELECT COUNT(*) FROM fact_player_game")).fetchone()[0]
            print(f"📊 Database now contains:")
            print(f"   Players: {player_count:,}")
            print(f"   Player-game records: {game_count:,}")
        
    except Exception as e:
        print(f"❌ Load process failed: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load NFL data into warehouse")
    parser.add_argument("--year", type=int, help="Season year", default=2025)
    parser.add_argument("--skip-injuries", action="store_true", help="Skip injury data loading")
    args = parser.parse_args()

    try:
        run_load(args.year, skip_injuries=args.skip_injuries)
    except FileNotFoundError as e:
        print(f"❌ File missing: {e}")
        print("💡 Make sure to run transform.py first to generate the required files")
        sys.exit(2)
    except SQLAlchemyError as e:
        print(f"❌ Database error: {e}")
        print("💡 Check your database connection and ensure schema is created")
        sys.exit(3)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)
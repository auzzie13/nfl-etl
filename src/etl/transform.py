import os
import pandas as pd
from typing import List, Dict

# --- CONFIG ---
RAW_DIR = "raw/"
TRANSFORMED_DIR = "transformed/"

os.makedirs(TRANSFORMED_DIR, exist_ok=True)


# --- PLAYER DIMENSION ---
def transform_players(roster_parquet_path: str) -> pd.DataFrame:
    """Transform roster data into player dimension table."""
    df = pd.read_parquet(roster_parquet_path)
    players = df[['player_id', 'first_name', 'last_name', 'position', 'birth_date']].drop_duplicates()
    players['full_name'] = players['first_name'] + ' ' + players['last_name']
    return players


def save_players(df: pd.DataFrame) -> str:
    """Save player dimension to parquet file."""
    out_path = os.path.join(TRANSFORMED_DIR, 'dim_players.parquet')
    df.to_parquet(out_path, index=False)
    print(f"Saved: {out_path}")
    return out_path


# --- PLAYER-GAME FACT TABLE ---
def extract_passing_stats(pbp: pd.DataFrame) -> pd.DataFrame:
    """Extract passing statistics from play-by-play data."""
    passing_plays = pbp[pbp['pass_attempt'] == 1].copy()
    if passing_plays.empty:
        return pd.DataFrame()
    
    passing_stats = passing_plays.groupby(['game_id', 'passer_player_id']).agg({
        'pass_attempt': 'sum',
        'complete_pass': 'sum', 
        'passing_yards': 'sum',
        'pass_touchdown': 'sum',
        'interception': 'sum'
    }).reset_index()
    
    passing_stats = passing_stats.rename(columns={
        'passer_player_id': 'player_id',
        'pass_attempt': 'pass_attempts',
        'complete_pass': 'completions',
        'passing_yards': 'pass_yards',
        'pass_touchdown': 'pass_tds'
    })
    
    return passing_stats


def extract_rushing_stats(pbp: pd.DataFrame) -> pd.DataFrame:
    """Extract rushing statistics from play-by-play data."""
    rushing_plays = pbp[pbp['rush_attempt'] == 1].copy()
    if rushing_plays.empty:
        return pd.DataFrame()
    
    rushing_stats = rushing_plays.groupby(['game_id', 'rusher_player_id']).agg({
        'rush_attempt': 'sum',
        'rushing_yards': 'sum',
        'rush_touchdown': 'sum'
    }).reset_index()
    
    rushing_stats = rushing_stats.rename(columns={
        'rusher_player_id': 'player_id',
        'rush_attempt': 'rush_attempts',
        'rush_touchdown': 'rush_tds'
    })
    
    return rushing_stats


def extract_receiving_stats(pbp: pd.DataFrame) -> pd.DataFrame:
    """Extract receiving statistics from play-by-play data."""
    receiving_plays = pbp[pbp['complete_pass'] == 1].copy()
    if receiving_plays.empty:
        return pd.DataFrame()
    
    receiving_stats = receiving_plays.groupby(['game_id', 'receiver_player_id']).agg({
        'complete_pass': 'sum',
        'receiving_yards': 'sum',
        'pass_touchdown': 'sum'  # This counts as receiving TD when it's a complete pass
    }).reset_index()
    
    receiving_stats = receiving_stats.rename(columns={
        'receiver_player_id': 'player_id',
        'complete_pass': 'receptions',
        'receiving_yards': 'rec_yards',
        'pass_touchdown': 'rec_tds'
    })
    
    return receiving_stats


def combine_player_stats(passing_df: pd.DataFrame, rushing_df: pd.DataFrame, 
                        receiving_df: pd.DataFrame) -> pd.DataFrame:
    """Combine all player statistics into a single fact table."""
    # Start with empty DataFrame with all possible columns
    all_stats = pd.DataFrame()
    
    # Add each stat type
    for df in [passing_df, rushing_df, receiving_df]:
        if not df.empty:
            if all_stats.empty:
                all_stats = df
            else:
                all_stats = pd.merge(all_stats, df, on=['game_id', 'player_id'], how='outer')
    
    # Fill NaN values with 0 for all numeric columns
    numeric_cols = all_stats.select_dtypes(include=['number']).columns
    all_stats[numeric_cols] = all_stats[numeric_cols].fillna(0)
    
    # Calculate total touchdowns
    td_cols = [col for col in all_stats.columns if col.endswith('_tds')]
    if td_cols:
        all_stats['total_tds'] = all_stats[td_cols].sum(axis=1)
    
    return all_stats


def transform_pbp_to_player_game(pbp_parquet_path: str) -> pd.DataFrame:
    """Transform play-by-play data into player game statistics."""
    pbp = pd.read_parquet(pbp_parquet_path)
    
    # Extract stats by type
    passing_stats = extract_passing_stats(pbp)
    rushing_stats = extract_rushing_stats(pbp)
    receiving_stats = extract_receiving_stats(pbp)
    
    # Combine all stats
    player_game_stats = combine_player_stats(passing_stats, rushing_stats, receiving_stats)
    
    return player_game_stats


def save_player_game(df: pd.DataFrame) -> str:
    """Save player game facts to parquet file."""
    out_path = os.path.join(TRANSFORMED_DIR, 'fact_player_game.parquet')
    df.to_parquet(out_path, index=False)
    print(f"Saved: {out_path}")
    return out_path


# --- INJURIES FACT TABLE ---
def transform_injuries(injury_parquet_path: str) -> pd.DataFrame:
    """Transform injury data into fact table."""
    inj = pd.read_parquet(injury_parquet_path)
    required_cols = ['player_id', 'season', 'week', 'injury_type', 'status']
    
    # Check if all required columns exist
    missing_cols = [col for col in required_cols if col not in inj.columns]
    if missing_cols:
        print(f"Warning: Missing columns in injury data: {missing_cols}")
        available_cols = [col for col in required_cols if col in inj.columns]
        return inj[available_cols].copy()
    
    return inj[required_cols].copy()


def save_injuries(df: pd.DataFrame) -> str:
    """Save injury facts to parquet file."""
    out_path = os.path.join(TRANSFORMED_DIR, 'fact_player_injury.parquet')
    df.to_parquet(out_path, index=False)
    print(f"Saved: {out_path}")
    return out_path


# --- DATA VALIDATION ---
def validate_file_exists(file_path: str) -> bool:
    """Check if file exists and print warning if not."""
    if not os.path.exists(file_path):
        print(f"Warning: File not found: {file_path}")
        return False
    return True


# --- MAIN RUN FUNCTION ---
def run_transform(year: int) -> Dict[str, str]:
    """Run the complete transformation pipeline."""
    print(f"Starting transformation for year {year}")
    results = {}
    
    # Player dimension
    roster_path = os.path.join(RAW_DIR, f'rosters_{year}.parquet')
    if validate_file_exists(roster_path):
        try:
            players_df = transform_players(roster_path)
            results['players'] = save_players(players_df)
            print(f"Processed {len(players_df)} players")
        except Exception as e:
            print(f"Error processing players: {e}")
    
    # Player-game fact
    pbp_path = os.path.join(RAW_DIR, f'pbp_{year}.parquet')
    if validate_file_exists(pbp_path):
        try:
            player_game_df = transform_pbp_to_player_game(pbp_path)
            results['player_game'] = save_player_game(player_game_df)
            print(f"Processed {len(player_game_df)} player-game records")
        except Exception as e:
            print(f"Error processing player game stats: {e}")
    
    # Injuries fact
    injury_path = os.path.join(RAW_DIR, f'injuries_{year}.parquet')
    if validate_file_exists(injury_path):
        try:
            injuries_df = transform_injuries(injury_path)
            results['injuries'] = save_injuries(injuries_df)
            print(f"Processed {len(injuries_df)} injury records")
        except Exception as e:
            print(f"Error processing injuries: {e}")
    
    print("Transformation complete!")
    return results


if __name__ == "__main__":
    YEAR = 2025
    run_transform(YEAR)
import os
import pandas as pd

# --- CONFIG ---
RAW_DIR = "raw/"
TRANSFORMED_DIR = "transformed/"

os.makedirs(TRANSFORMED_DIR, exist_ok=True)

pbp = pd.read_parquet("raw/pbp_2025.parquet")
print(pbp.columns.tolist())


 

# --- PLAYER DIMENSION ---
def transform_players(roster_parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(roster_parquet_path)
    players = df[['player_id', 'first_name', 'last_name', 'position', 'birth_date']].drop_duplicates()
    players['full_name'] = players['first_name'] + ' ' + players['last_name']
    return players

def save_players(df: pd.DataFrame):
    out_path = os.path.join(TRANSFORMED_DIR, 'dim_players.parquet')
    df.to_parquet(out_path, index=False)
    print("Saved:", out_path)
    return out_path

# --- PLAYER-GAME FACT TABLE ---
def transform_pbp_to_player_game(pbp_parquet_path: str) -> pd.DataFrame:
    pbp = pd.read_parquet(pbp_parquet_path)
    player_cols = [
        'game_id', 'posteam', 'defteam', 'player_id', 'player_name',
        'pass_attempt','pass_complete','pass_yards',
        'rush_attempt','rush_yards',
        'reception','receiving_yards',
        'touchdown','fumble'
    ]
    sub = pbp[player_cols].copy()
    print(pbp.columns.tolist())

    agg = sub.groupby(['game_id','player_id']).agg(
        pass_attempts=('pass_attempt','sum'),
        pass_completions=('pass_complete','sum'),
        pass_yards=('pass_yards','sum'),
        rush_attempts=('rush_attempt','sum'),
        rush_yards=('rush_yards','sum'),
        receptions=('reception','sum'),
        rec_yards=('receiving_yards','sum'),
        fumbles=('fumble','sum'),
        touchdowns=('touchdown','sum'),
    ).reset_index()
    return agg

def save_player_game(df: pd.DataFrame):
    out_path = os.path.join(TRANSFORMED_DIR, 'fact_player_game.parquet')
    df.to_parquet(out_path, index=False)
    print("Saved:", out_path)
    return out_path

# --- INJURIES FACT TABLE ---
def transform_injuries(injury_parquet_path: str) -> pd.DataFrame:
    inj = pd.read_parquet(injury_parquet_path)
    inj_table = inj[['player_id','season','week','injury_type','status']].copy()
    return inj_table

def save_injuries(df: pd.DataFrame):
    out_path = os.path.join(TRANSFORMED_DIR, 'fact_player_injury.parquet')
    df.to_parquet(out_path, index=False)
    print("Saved:", out_path)
    return out_path

# --- MAIN RUN FUNCTION ---
def run_transform(year: int):
    # Player dimension
    roster_path = os.path.join(RAW_DIR, f'rosters_{year}.parquet')
    players_df = transform_players(roster_path)
    save_players(players_df)
    
    # Player-game fact
    pbp_path = os.path.join(RAW_DIR, f'pbp_{year}.parquet')
    player_game_df = transform_pbp_to_player_game(pbp_path)
    save_player_game(player_game_df)
    
    # Injuries fact
    injury_path = os.path.join(RAW_DIR, f'injuries_{year}.parquet')
    injuries_df = transform_injuries(injury_path)
    save_injuries(injuries_df)

if __name__ == "__main__":
    YEAR = 2025  # change for season
    run_transform(YEAR)

import pandas as pd
import json
import os

def safe_split_excel():
    file_name = "data/Smite 2 Gods.xlsx"
    if not os.path.exists(file_name):
        print("❌ Error: 'Smite 2 Gods.xlsx' not found.")
        return

    # Load the sheets
    # We use None for names to see the raw Unnamed: X headers first
    df_rankings_raw = pd.read_excel(file_name, sheet_name='Rankings', header=1)
    df_input_raw = pd.read_excel(file_name, sheet_name='Input', header=1)

    # --- 1. CREATE GOD METADATA (Static Data) ---
    # We map specific Unnamed columns to prevent duplicates
    # This logic matches your app.py exactly but ensures uniqueness
    meta_mapping = {
        'Unnamed: 5':  'God',
        'Unnamed: 8':  'Pantheon',
        'Unnamed: 10': 'Role',
        'Unnamed: 12': 'Attack Type',
        'Unnamed: 14': 'Damage Type',
        'Unnamed: 16': 'Class',
        'Tier':        'Tier',
        'Rank':        'Rank',
        'Title':       'Title'
    }
    
    # Select only the columns we actually want
    # This ignores any existing 'God' or 'Role' columns that were empty/placeholders
    meta_df = df_rankings_raw[list(meta_mapping.keys())].copy()
    meta_df = meta_df.rename(columns=meta_mapping)
    
    # Drop rows where God name is missing and remove any accidental duplicates
    meta_df = meta_df.dropna(subset=['God'])
    meta_df = meta_df.loc[:, ~meta_df.columns.duplicated()] # THE FIX: Ensure unique columns

    # --- 2. CREATE COUNCIL RATINGS (Dynamic Data) ---
    players = ['Joey', 'Darian', 'Jami', 'Jamie']
    # Find which player columns actually exist
    available_players = [p for p in players if p in df_input_raw.columns]
    
    # 'Unnamed: 1' is where the God names live in the Input sheet
    ratings_df = df_input_raw[['Unnamed: 1'] + available_players].copy()
    ratings_df = ratings_df.rename(columns={'Unnamed: 1': 'God'})
    ratings_df = ratings_df.dropna(subset=['God'])
    ratings_df = ratings_df.loc[:, ~ratings_df.columns.duplicated()] # THE FIX

    # --- 3. SAVE TO DATA FOLDER ---
    os.makedirs('data', exist_ok=True)
    
    # Save Metadata
    meta_df.to_json('data/gods_metadata.json', orient='records', indent=4)
    # Save Ratings
    ratings_df.to_json('data/council_ratings.json', orient='records', indent=4)
    
    print("✅ Success! Created:")
    print("   - data/gods_metadata.json")
    print("   - data/council_ratings.json")

if __name__ == "__main__":
    safe_split_excel()
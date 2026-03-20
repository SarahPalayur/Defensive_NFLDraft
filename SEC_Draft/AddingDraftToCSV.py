import pandas as pd
import ssl
import re

# 1. FIX MAC SSL ISSUES (Allows downloading the draft database)
ssl._create_default_https_context = ssl._create_unverified_context

# 2. SETTINGS
input_filename = 'V2SEC_Defensive_Picks.csv'
output_filename = 'SEC_Defensive_Draft_Final.csv'

print(f"Reading {input_filename}...")

# 3. LOAD DATA (Automatically finds the row where 'Year' starts)
temp_df = pd.read_csv(input_filename, header=None)
header_row_index = temp_df[temp_df.eq("Year").any(axis=1)].index[0]
df = pd.read_csv(input_filename, skiprows=header_row_index)

# 4. CLEAN NAMES AND SCHOOLS
sec_schools = ['MIZ', 'TA&M', 'AUB', 'SC', 'MSST', 'TENN', 'UGA', 'ALA', 'UK', 'ARK', 'LSU', 'MISS', 'UF', 'VU', 'OU', 'UT']

def split_name_school(val):
    val = str(val)
    for school in sec_schools:
        if val.endswith(school):
            player_name = val[:-len(school)].strip()
            return player_name, school
    return val, "Unknown"

# Rename the Rank and Player/School column
rank_col_name = df.columns[0]
name_col_name = df.columns[1]
df.rename(columns={rank_col_name: 'Rank'}, inplace=True)
df[['Player', 'School']] = df.apply(lambda x: split_name_school(x[name_col_name]), axis=1, result_type='expand')

# 5. FETCH NFL DRAFT DATA
print("Fetching NFL Draft database...")
draft_url = "https://github.com/nflverse/nflverse-data/releases/download/draft_picks/draft_picks.csv"
draft_df = pd.read_csv(draft_url)

# Find the right column names in the downloaded file
name_col = next((c for c in ['player', 'pfr_name', 'pfr_player_name'] if c in draft_df.columns), None)
year_col = next((c for c in ['season', 'draft_year'] if c in draft_df.columns), None)

# 6. MERGE DATA
# Note: College Season + 1 = Draft Year
df['Draft_Year_Lookup'] = df['Year'] + 1

merged = pd.merge(
    df, 
    draft_df[[name_col, year_col, 'round']], 
    left_on=['Player', 'Draft_Year_Lookup'], 
    right_on=[name_col, year_col], 
    how='left'
)

# 7. FORMAT FINAL COLUMNS
draft_col_name = 'Draft Round -0 if not drafted'
merged[draft_col_name] = merged['round'].fillna(0).astype(int)

# Organize columns for the final CSV
base_cols = ['Rank', 'Player', 'School', 'POS', 'SOLO', 'AST', 'TOT', 'SACK']
extra_cols = [c for c in ['YDS', 'PD', 'INT', 'TD', 'FF'] if c in df.columns]
final_cols = base_cols + extra_cols + ['Year', draft_col_name]

final_df = merged[final_cols]

# 8. SAVE AND COUNT
final_df.to_csv(output_filename, index=False)

# This counts how many players have a Draft Round greater than 0
drafted_count = (final_df[draft_col_name] > 0).sum()
total_players = len(final_df)

print("-" * 30)
print(f"SUCCESS!")
print(f"Total players processed: {total_players}")
print(f"Number of players drafted: {drafted_count}")
print(f"File saved as: {output_filename}")
print("-" * 30)

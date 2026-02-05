import json
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict
import os


# STEP 1: LOAD AND FILTER JSON DATA

print("SPOTIFY 2025 ANALYTICS - ETL PIPELINE")

json_file = 'Streaming_History_Audio_2024-2025_1.json'  

try:
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"✓ Loaded JSON file: {len(data)} total records")
except FileNotFoundError:
    print(f"✗ Error: File '{json_file}' not found!")
    exit()


# STEP 2: FILTER FOR 2025 DATA ONLY

data_2025 = []
for record in data:
    if record.get('ts'):
        ts = record['ts']
        year = int(ts[:4])
        if year == 2025:
            data_2025.append(record)

print(f"✓ Filtered for 2025: {len(data_2025)} records")


# STEP 3: CREATE BASE DATAFRAME


df = pd.DataFrame(data_2025)

# Keep relevant columns only
cols_to_keep = [
    'ts', 'master_metadata_track_name', 'master_metadata_album_artist_name',
    'master_metadata_album_album_name', 'ms_played', 'skipped', 'shuffle',
    'offline', 'incognito_mode', 'platform', 'conn_country', 'reason_end'
]

df = df[cols_to_keep].copy()
print(f"✓ Created base dataframe with {len(df)} records")

# STEP 4: DATA CLEANING


# Handle missing track/artist names (skip entries without them)
df = df[df['master_metadata_track_name'].notna()].copy()
df = df[df['master_metadata_album_artist_name'].notna()].copy()
print(f"✓ After removing null tracks/artists: {len(df)} records")

# Remove duplicates (exact same record)
df = df.drop_duplicates()
print(f"✓ After removing duplicates: {len(df)} records")


# STEP 5: FEATURE ENGINEERING


# Parse timestamp
df['datetime'] = pd.to_datetime(df['ts'])
df['date'] = df['datetime'].dt.date
df['hour'] = df['datetime'].dt.hour
df['day_of_week'] = df['datetime'].dt.day_name()
df['day_of_week_num'] = df['datetime'].dt.dayofweek
df['week_number'] = df['datetime'].dt.isocalendar().week
df['month'] = df['datetime'].dt.month
df['month_name'] = df['datetime'].dt.strftime('%B')
df['day_name'] = df['datetime'].dt.strftime('%A')

# Convert milliseconds to minutes
df['minutes_played'] = df['ms_played'] / 60000

# Listening completion (if not skipped, assume completed)
df['was_completed'] = (~df['skipped']).astype(int)
df['was_skipped'] = df['skipped'].astype(int)

# Rename columns for clarity
df = df.rename(columns={
    'master_metadata_track_name': 'track_name',
    'master_metadata_album_artist_name': 'artist_name',
    'master_metadata_album_album_name': 'album_name'
})

print("✓ Feature engineering complete")


# STEP 6: GENERATE AGGREGATION TABLES


print("\n" + "=" * 80)
print("GENERATING AGGREGATION TABLES")
print("=" * 80)

# TABLE 1: DAILY SUMMARY
daily_summary = df.groupby('date').agg(
    total_minutes=('minutes_played', 'sum'),
    tracks_played=('track_name', 'count'),
    skips=('was_skipped', 'sum'),
    completions=('was_completed', 'sum'),
    unique_artists=('artist_name', 'nunique')
).reset_index()

daily_summary['skip_rate'] = (daily_summary['skips'] / daily_summary['tracks_played'] * 100).round(2)
daily_summary['hours_played'] = (daily_summary['total_minutes'] / 60).round(2)
daily_summary = daily_summary.sort_values('date')
print(f"✓ Daily Summary: {len(daily_summary)} days")

# TABLE 2: ARTIST SUMMARY
artist_summary = df.groupby('artist_name').agg(
    total_minutes=('minutes_played', 'sum'),
    track_count=('track_name', 'nunique'),
    plays=('track_name', 'count'),
    skips=('was_skipped', 'sum'),
    first_play=('date', 'min'),
    last_play=('date', 'max')
).reset_index()

artist_summary['skip_rate'] = (artist_summary['skips'] / artist_summary['plays'] * 100).round(2)
artist_summary['hours_played'] = (artist_summary['total_minutes'] / 60).round(2)
artist_summary = artist_summary.sort_values('total_minutes', ascending=False)
print(f"✓ Artist Summary: {len(artist_summary)} unique artists")

# TABLE 3: TRACK SUMMARY
track_summary = df.groupby(['track_name', 'artist_name']).agg(
    total_minutes=('minutes_played', 'sum'),
    play_count=('track_name', 'count'),
    skips=('was_skipped', 'sum'),
    completions=('was_completed', 'sum'),
    first_play=('date', 'min'),
    last_play=('date', 'max')
).reset_index()

track_summary['skip_rate'] = (track_summary['skips'] / track_summary['play_count'] * 100).round(2)
track_summary = track_summary.sort_values('total_minutes', ascending=False)
print(f"✓ Track Summary: {len(track_summary)} unique tracks")

# TABLE 4: HOURLY PATTERN
hourly_pattern = df.groupby('hour').agg(
    total_minutes=('minutes_played', 'sum'),
    track_count=('track_name', 'count'),
    skips=('was_skipped', 'sum'),
    unique_artists=('artist_name', 'nunique')
).reset_index()

hourly_pattern['avg_minutes_per_session'] = (hourly_pattern['total_minutes'] / hourly_pattern['track_count']).round(2)
hourly_pattern['skip_rate'] = (hourly_pattern['skips'] / hourly_pattern['track_count'] * 100).round(2)
print(f"✓ Hourly Pattern: {len(hourly_pattern)} hours")

# TABLE 5: DAY OF WEEK PATTERN
day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
weekly_pattern = df.groupby('day_name').agg(
    total_minutes=('minutes_played', 'sum'),
    track_count=('track_name', 'count'),
    skips=('was_skipped', 'sum'),
    unique_artists=('artist_name', 'nunique')
).reset_index()

weekly_pattern['skip_rate'] = (weekly_pattern['skips'] / weekly_pattern['track_count'] * 100).round(2)
weekly_pattern['day_order'] = weekly_pattern['day_name'].map(lambda x: day_order.index(x))
weekly_pattern = weekly_pattern.sort_values('day_order')
print(f"✓ Weekly Pattern: {len(weekly_pattern)} days")

# TABLE 6: MONTHLY PROGRESSION
monthly_progression = df.groupby('month').agg(
    total_minutes=('minutes_played', 'sum'),
    tracks_played=('track_name', 'count'),
    skips=('was_skipped', 'sum'),
    unique_artists=('artist_name', 'nunique'),
    unique_tracks=('track_name', 'nunique'),
    days_with_listening=('date', 'nunique')
).reset_index()

monthly_progression['hours_played'] = (monthly_progression['total_minutes'] / 60).round(2)
monthly_progression['skip_rate'] = (monthly_progression['skips'] / monthly_progression['tracks_played'] * 100).round(2)
monthly_progression['month_name'] = monthly_progression['month'].map({
    1: 'January', 2: 'February', 3: 'March', 4: 'April', 5: 'May', 6: 'June',
    7: 'July', 8: 'August', 9: 'September', 10: 'October', 11: 'November', 12: 'December'
})
print(f"✓ Monthly Progression: {len(monthly_progression)} months")

# TABLE 7: PLATFORM DISTRIBUTION
platform_dist = df.groupby('platform').agg(
    total_minutes=('minutes_played', 'sum'),
    track_count=('track_name', 'count')
).reset_index()

platform_dist['percentage'] = (platform_dist['track_count'] / platform_dist['track_count'].sum() * 100).round(2)
platform_dist = platform_dist.sort_values('track_count', ascending=False)
print(f"✓ Platform Distribution: {len(platform_dist)} platforms")


# STEP 7: CALCULATE OVERALL KPIs


print("\n" + "=" * 80)
print("KEY PERFORMANCE INDICATORS (2025)")
print("=" * 80)

total_minutes = df['minutes_played'].sum()
total_hours = total_minutes / 60
total_tracks = len(df)
unique_artists = df['artist_name'].nunique()
unique_tracks = df['track_name'].nunique()
skip_rate = (df['was_skipped'].sum() / len(df) * 100)
completion_rate = (df['was_completed'].sum() / len(df) * 100)
listening_days = df['date'].nunique()
avg_daily_minutes = total_minutes / listening_days

print(f"Total Listening Time: {total_hours:,.1f} hours ({total_minutes:,.0f} minutes)")
print(f"Total Tracks Played: {total_tracks:,.0f}")
print(f"Unique Artists: {unique_artists:,.0f}")
print(f"Unique Tracks: {unique_tracks:,.0f}")
print(f"Skip Rate: {skip_rate:.2f}%")
print(f"Completion Rate: {completion_rate:.2f}%")
print(f"Listening Days: {listening_days}")
print(f"Average Daily Listening: {avg_daily_minutes:.2f} minutes ({avg_daily_minutes/60:.2f} hours)")


# STEP 8: EXPORT ALL TABLES AS CSV


print("\n" + "=" * 80)
print("EXPORTING TO CSV FILES")
print("=" * 80)

output_dir = 'spotify_analytics_output'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Export all tables
tables = {
    'daily_summary.csv': daily_summary,
    'artist_summary.csv': artist_summary,
    'track_summary.csv': track_summary,
    'hourly_pattern.csv': hourly_pattern,
    'weekly_pattern.csv': weekly_pattern,
    'monthly_progression.csv': monthly_progression,
    'platform_distribution.csv': platform_dist,
    'raw_data_2025.csv': df
}

for filename, table in tables.items():
    filepath = os.path.join(output_dir, filename)
    table.to_csv(filepath, index=False)
    print(f"✓ Exported: {filename}")

print("\n" + "=" * 80)
print("✓ ETL PIPELINE COMPLETE!")
print("=" * 80)
print(f"\nAll CSV files saved to: '{output_dir}/' folder")

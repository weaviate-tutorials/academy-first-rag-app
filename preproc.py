from datasets import load_dataset
import pandas as pd
from datetime import datetime

# Load the dataset
ds = load_dataset("wykonos/movies")["train"]
df = ds.to_pandas()

print(f"Original dataset size: {len(df)}")

# Convert release_date to datetime and extract year
df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
df["year"] = df["release_date"].dt.year

# Filter out rows with missing dates or revenue
df_filtered = df.dropna(subset=["release_date", "year", "revenue"])

# Apply new filtering criteria: top 50k by vote_count OR revenue > 1M
# First, get movie with vote count > 100
df_vote_count_filter = df_filtered[df_filtered["vote_count"] > 100]

# Then get movies with revenue > 1M
df_high_revenue = df_filtered[df_filtered["revenue"] > 1000000]

# Combine both sets and remove duplicates based on movie ID
df_filtered = pd.concat([df_vote_count_filter, df_high_revenue]).drop_duplicates(
    subset=["id"]
)

# Also remove duplicates based on title and overview (content-based deduplication)
df_filtered = df_filtered.drop_duplicates(subset=["title", "overview"])

# Filter to only include movies from 1930 onwards
df_filtered = df_filtered[df_filtered["year"] >= 1930]

print(
    f"Filtered dataset size (top 50k by vote_count OR revenue > $1M, duplicates removed, 1930+ only): {len(df_filtered)}"
)
print(f"Year range: {df_filtered['year'].min()} - {df_filtered['year'].max()}")

# Get top 1000 movies by revenue for each year
top_movies_by_year = []

for year in sorted(df_filtered["year"].unique()):
    year_data = df_filtered[df_filtered["year"] == year]

    if len(year_data) > 0:
        # Sort by revenue in descending order and get top 1000
        year_top = year_data.sort_values(by="revenue", ascending=False).head(1000)
        top_movies_by_year.append(year_top)

        print(
            f"Year {year}: {len(year_top)} movies (max revenue: ${year_top['revenue'].max():,})"
        )

# Combine all years and sort by year first, then by revenue (descending)
df_top_by_year = pd.concat(top_movies_by_year, ignore_index=True)
df_top_by_year = df_top_by_year.sort_values(
    ["year", "revenue"], ascending=[True, False]
)

print(f"\nTotal movies across all years: {len(df_top_by_year)}")
print(f"Unique years: {df_top_by_year['year'].nunique()}")

# Show sample of results
print(f"\nSample of top movies by year:")
for year in sorted(df_top_by_year["year"].unique())[-5:]:  # Show first 5 years
    year_top = df_top_by_year[df_top_by_year["year"] == year].head(3)
    print(f"\nYear {year}:")
    for _, movie in year_top.iterrows():
        print(f"  - {movie['title']}: ${movie['revenue']:,}")

# Export to multiple parquet files (10k objects each)
chunk_size = 5000
num_chunks = len(df_top_by_year) // chunk_size + (
    1 if len(df_top_by_year) % chunk_size != 0 else 0
)

print(f"\nExporting to {num_chunks} parquet files with {chunk_size} objects each...")

for i in range(num_chunks):
    start_idx = i * chunk_size
    end_idx = min((i + 1) * chunk_size, len(df_top_by_year))

    chunk = df_top_by_year.iloc[start_idx:end_idx]
    filename = f"data/movies_popular_{i+1:02d}.parquet"

    chunk.to_parquet(path=filename)
    print(f"Exported chunk {i+1}: {len(chunk)} objects to {filename}")

print(f"\nExport complete! Total files created: {num_chunks}")

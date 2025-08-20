from helpers import CollectionName, process_str_categorical, process_int_categorical, connect_to_weaviate
from weaviate.util import generate_uuid5
from weaviate.classes.config import Property, DataType, Configure, Tokenization
from tqdm import tqdm
import glob
import pandas as pd
from datetime import datetime, timezone
from typing import Iterator, Dict, Union


def get_data_objects_from_parquet() -> Iterator[Dict[str, Union[datetime, str, int]]]:
    """Load movie data from parquet files instead of streaming dataset."""

    # Find all parquet files in the data directory
    parquet_files = glob.glob("data/movies_popular_*.parquet")
    parquet_files.sort()  # Ensure consistent ordering

    for parquet_file in parquet_files:
        print(f"Loading data from {parquet_file}...")
        df = pd.read_parquet(parquet_file)

        for _, row in df.iterrows():
            # Handle release_date - it's already a datetime from preprocessing
            release_date = row["release_date"]
            if pd.isna(release_date):
                release_date = None
            elif isinstance(release_date, str):
                # Fallback: if it's still a string, parse it
                try:
                    release_date = datetime.strptime(release_date, "%Y-%m-%d").replace(
                        tzinfo=timezone.utc
                    )
                except ValueError:
                    release_date = None
            elif isinstance(release_date, datetime):
                # Ensure timezone info is set
                if release_date.tzinfo is None:
                    release_date = release_date.replace(tzinfo=timezone.utc)

            yield {
                "movie_id": row["id"],
                "title": row["title"],
                "overview": row["overview"],
                "original_language": row["original_language"],
                "tagline": row["tagline"],
                "poster_path": row["poster_path"],
                "genres": process_str_categorical(row["genres"]),
                "keywords": process_str_categorical(row["keywords"]),
                "credits": process_str_categorical(row["credits"]),
                "recommendations": process_int_categorical(row["recommendations"]),
                "budget": int(row["budget"]) if pd.notna(row["budget"]) else 0,
                "revenue": int(row["revenue"]) if pd.notna(row["revenue"]) else 0,
                "vote_average": (
                    row["vote_average"] if pd.notna(row["vote_average"]) else 0.0
                ),
                "popularity": int(row["popularity"]) if pd.notna(row["popularity"]) else 0.0,
                "runtime": int(row["runtime"]) if pd.notna(row["runtime"]) else 0,
                "year": int(row["year"]) if pd.notna(row["year"]) else 0,
                "release_date": release_date,
            }


MAX_OBJECTS = 20000


with connect_to_weaviate() as client:

    # client.collections.delete(CollectionName.MOVIES)

    if not client.collections.exists(CollectionName.MOVIES):
        client.collections.create(
            name=CollectionName.MOVIES,
            properties=[
                Property(name="title", data_type=DataType.TEXT),
                Property(name="overview", data_type=DataType.TEXT),
                Property(name="original_language", data_type=DataType.TEXT),
                Property(name="tagline", data_type=DataType.TEXT),
                Property(name="poster_path", data_type=DataType.TEXT),
                Property(name="genres", data_type=DataType.TEXT_ARRAY),
                Property(name="keywords", data_type=DataType.TEXT_ARRAY),
                Property(name="recommendations", data_type=DataType.INT_ARRAY),
                Property(
                    name="credits",
                    data_type=DataType.TEXT_ARRAY,
                    tokenization=Tokenization.FIELD,
                ),
                Property(name="movie_id", data_type=DataType.INT),
                Property(name="budget", data_type=DataType.INT),
                Property(name="revenue", data_type=DataType.INT),
                Property(name="vote_average", data_type=DataType.NUMBER),
                Property(name="vote_count", data_type=DataType.INT),
                Property(name="popularity", data_type=DataType.NUMBER),
                Property(name="runtime", data_type=DataType.INT),
                Property(name="year", data_type=DataType.INT),
                Property(name="release_date", data_type=DataType.DATE),
            ],
            vector_config=[
                Configure.Vectors.text2vec_weaviate(
                    name="default",
                    source_properties=["title", "overview"],
                    model="Snowflake/snowflake-arctic-embed-l-v2.0",
                    quantizer=Configure.VectorIndex.Quantizer.rq(),
                ),
                Configure.Vectors.text2vec_weaviate(
                    name="genres",
                    source_properties=["genres"],
                    model="Snowflake/snowflake-arctic-embed-l-v2.0",
                    quantizer=Configure.VectorIndex.Quantizer.rq(),
                ),
            ],
        )

    movies = client.collections.get(CollectionName.MOVIES)

    # Add objects to the collection
    counter = 0
    with movies.batch.fixed_size(batch_size=200) as batch:
        for obj in tqdm(get_data_objects_from_parquet()):
            uuid = generate_uuid5(obj)
            batch.add_object(properties=obj, uuid=generate_uuid5(obj))

            counter += 1

            if counter >= MAX_OBJECTS:
                break


    if len(movies.batch.failed_objects) > 0:
        print("*" * 80)
        print(f"***** Failed to add {len(movies.batch.failed_objects)} objects *****")
        print("*" * 80)
        print(movies.batch.failed_objects[:3])

    print(len(movies))

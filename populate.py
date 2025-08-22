import pandas as pd
import glob
from datetime import datetime
from typing import Iterator, Dict, Union
from weaviate import WeaviateClient
from weaviate.util import generate_uuid5
from weaviate.classes.config import Property, DataType, Configure
from tqdm import tqdm
from helpers import CollectionName, connect_to_weaviate


def get_data_objects_from_parquet() -> Iterator[Dict[str, Union[datetime, str, int]]]:
    """
    TODO: Implement this function to load movie data from parquet files

    This function should:
    1. Find all parquet files in the data directory (look for files starting with 'movies_popular_w_vectors_')
    2. Load each parquet file using pandas
    3. Process each row and yield a dictionary with the movie data

    Hints:
    - Use glob.glob() to find files
    - Use pd.read_parquet() to load files
    - Process each row and handle missing values
    """

    # Find all parquet files in the data directory
    parquet_files = glob.glob("data/movies_popular_w_vectors_*.parquet")

    # Sort the files to ensure consistent ordering
    parquet_files.sort()

    # Loop through each parquet file
    for parquet_file in parquet_files:
        print(f"Loading data from {parquet_file}...")
        df = pd.read_parquet(parquet_file)

        # Process each row in the dataframe
        for _, row in df.iterrows():
            # Process fields using helper functions - only extract what's needed for the simplified Movie model
            properties = row["properties"]
            processed_properties = {
                "movie_id": properties["movie_id"],
                "title": properties["title"],
                "overview": properties["overview"],
                "genres": None if properties["genres"] is None else list(properties["genres"]),
                "year": properties["year"],
                "popularity": properties["popularity"]
            }
            yield {
                "properties": processed_properties,
                "vectors": row["vectors"]
            }


def create_movies_collection(client: WeaviateClient):
    """
    TODO: Implement this function to create the Movies collection in Weaviate

    This function should:
    1. Check if the collection already exists
    2. If not, create it with the proper schema
    3. Include vector configurations for text embeddings

    The collection should have these properties:
    - movie_id (INT)
    - title (TEXT)
    - overview (TEXT)
    - genres (TEXT_ARRAY)
    - year (INT)
    - popularity (NUMBER)

    Vector configurations:
    - Use the `text2vec_weaviate` model integration.
    - "default" vector: uses title and overview property
    - "genres" vector: uses genres property
    - Use the Snowflake/snowflake-arctic-embed-l-v2.0 model
    """

    # STUDENT TODO - implement the above
    # `if not ...`
    # Write your code here according to the instructions
    else:
        raise RuntimeError(
            "Collection 'Movies' already exists! "
            "If you like to re-build the collection, create and run a separate script to delete the existing collection. "
            "\n\nTo delete a collection, run: client.collections.delete(<collection_name>)"
        )


def ingest_movies_data(client: WeaviateClient, max_objects=20000):
    """
    TODO: Implement this function to ingest movie data into Weaviate

    This function should:
    1. Get the Movies collection
    2. Use batch operations to efficiently add objects
    3. Generate UUIDs for each movie
    4. Handle errors and show progress

    Hints:
    - Use client.collections.get() to get the collection
    - Use batch.fixed_size() for efficient ingestion
    - Use generate_uuid5() to create UUIDs
    - Use tqdm for progress tracking
    """

    # STUDENT TODO - Get the Movies collection
    # Write your code here according to the instructions

    # STUDENT TODO - Batch import with context manager, with fixed size & size 100
    # `with ... as batch`
    # Write your code here according to the instructions

        # Process each movie object
        for obj in tqdm(get_data_objects_from_parquet()):
            # STUDENT TODO - Add object to batch, and pass data from `obj`:
            # - Properties `obj["properties"]`
            # - UUID `uuid`
            # - Vectors `obj["vectors"]`
            uuid = generate_uuid5(obj)
            # Write your code here according to the instructions

    # TODO - Handle any failed objects
    # Check if there are any failed objects, and display the first few if so
    # Write your code here according to the instructions

    # Print final count
    print(f"Successfully added {len(movies)} movies")


def main():
    """
    Main function to run the data ingestion process
    """
    print("🎬 Starting Movie Data Ingestion into Weaviate")
    print("=" * 50)

    try:
        # Connect to Weaviate
        print("🔌 Connecting to Weaviate...")
        with connect_to_weaviate() as client:
            print("✅ Connected successfully!")

            # Create the collection
            print("📚 Creating Movies collection...")
            create_movies_collection(client)
            print("✅ Collection ready!")

            # Ingest the data
            print("📥 Ingesting movie data...")
            ingest_movies_data(client)
            print("✅ Data ingestion complete!")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("💡 Check your Weaviate connection and try again")


if __name__ == "__main__":
    main()

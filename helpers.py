import weaviate
from weaviate import WeaviateClient
import os
from enum import Enum
from datetime import datetime, timezone
from collections.abc import Iterator
from datasets import load_dataset
from typing import Dict, Union, Literal, Optional


class CollectionName(str, Enum):
    """Enum for Weaviate collection names."""

    MOVIES = "Movies"


def connect_to_weaviate() -> WeaviateClient:
    client = weaviate.connect_to_local(
        port=8080,
        headers={
            "X-COHERE-API-KEY": os.environ["COHERE_API_KEY"],
        },
    )
    return client


def process_movie_categorical(raw_string: Union[str, None]) -> Union[list[str], None]:
    if raw_string == None:
        processed_data = None
    else:
        processed_data = raw_string.split("-")
    return processed_data


def get_data_objects() -> Iterator[Dict[str, Union[datetime, str, int]]]:
    ds = load_dataset("wykonos/movies", streaming=True)["train"]
    for item in ds:
        if item["release_date"] == None:
            release_date = None
        else:
            release_date = datetime.strptime(item["release_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)

        yield {
            "title": item["title"],
            "overview": item["overview"],
            "genres": process_movie_categorical(item["genres"]),
            "keywords": process_movie_categorical(item["keywords"]),
            "credits": process_movie_categorical(item["credits"]),
            "movie_id": item["id"],
            "budget": int(item["budget"]),
            "revenue": int(item["revenue"]),
            "vote_average": item["vote_average"],
            "release_date": release_date,
        }

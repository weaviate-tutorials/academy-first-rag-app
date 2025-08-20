import weaviate
from weaviate import WeaviateClient
import os
from anthropic import Anthropic
from enum import Enum
from datetime import datetime, timezone
from collections.abc import Iterator
from datasets import load_dataset
from typing import Dict, Union, Literal, Optional


class CollectionName(str, Enum):
    """Enum for Weaviate collection names."""

    MOVIES = "Movies"


def connect_to_weaviate() -> WeaviateClient:
    client = weaviate.connect_to_weaviate_cloud(
        cluster_url=os.getenv("WCD_TEST_URL"),
        auth_credentials=os.getenv("WCD_TEST_KEY")
    )
    return client


def process_str_categorical(raw_string: Union[str, None]) -> Union[list[str], None]:
    if raw_string == None:
        processed_data = None
    else:
        processed_data = raw_string.split("-")
    return processed_data


def process_int_categorical(raw_string: Union[str, None]) -> Union[list[int], None]:
    if raw_string == None:
        processed_data = None
    else:
        processed_data = raw_string.split("-")
        processed_data = [int(i) for i in processed_data]
    return processed_data


def get_data_objects() -> Iterator[Dict[str, Union[datetime, str, int]]]:
    ds = load_dataset("wykonos/movies", streaming=True)["train"]
    for item in ds:
        if item["release_date"] == None:
            release_date = None
        else:
            release_date = datetime.strptime(item["release_date"], "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )

        yield {
            "title": item["title"],
            "overview": item["overview"],
            "genres": process_str_categorical(item["genres"]),
            "keywords": process_str_categorical(item["keywords"]),
            "credits": process_str_categorical(item["credits"]),
            "movie_id": item["id"],
            "budget": int(item["budget"]),
            "revenue": int(item["revenue"]),
            "vote_average": item["vote_average"],
            "release_date": release_date,
        }


def call_claude(prompt: str) -> str:

    client = Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    message = client.messages.create(
        max_tokens=1024,
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
        model="claude-3-5-haiku-latest",
    )
    return message.content


def movie_occasion_to_query(occasion: str) -> str:

    prompt = f"""
    I would like to perform a vector search to find movies best matching this occasion

    ========== OCCASION INPUT FROM USER ==========
    {occasion}
    ========== END INPUT ==========

    What should my search string be?

    IMPORTANT: Only include the search string text in your response and nothing else.
    """
    return call_claude(prompt)

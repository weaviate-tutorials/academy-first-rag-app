from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import weaviate
from weaviate import WeaviateClient
from weaviate.classes.query import Filter
from datetime import datetime, timezone
from helpers import connect_to_weaviate, CollectionName
import uvicorn


app = FastAPI(
    title="MovieInsights API",
    description="A movie discovery and recommendation platform using Weaviate",
    version="1.0.0",
)


# Pydantic models for request/response
class Movie(BaseModel):
    movie_id: int
    title: str
    overview: Optional[str] = None
    genres: Optional[List[str]] = None
    keywords: Optional[List[str]] = None
    credits: Optional[List[str]] = None
    budget: int
    revenue: int
    vote_average: float
    release_date: Optional[datetime] = None


class SearchResponse(BaseModel):
    movies: List[Movie]
    current_page: int


class MovieDetailResponse(BaseModel):
    movie: Movie
    similar_movies: List[Movie]


class ExplorerResponse(BaseModel):
    movies: List[Movie]
    genre: str
    year: Optional[int]
    total_count: int


class RecommendationResponse(BaseModel):
    recommended_movie: Movie
    occasion: str
    reasoning: str


class StatsResponse(BaseModel):
    total_movies: int
    movies_by_year: Dict[int, int]


# Weaviate client (placeholder - students need to implement connection)
# client = weaviate.Client("http://localhost:8080")


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "MovieInsights API",
        "version": "1.0.0",
        "endpoints": [
            "/search - Search movies by text",
            "/movie/{movie_id} - Get movie details and similar movies",
            "/explore - Explore movies by genre and year",
            "/recommend - Get movie recommendations for occasions",
            "/stats - Get dataset statistics",
        ],
    }


@app.get("/search", response_model=SearchResponse)
async def search_movies(
    q: str = Query(..., description="Search query for movies"),
    page: int = Query(1, ge=1, le=3, description="Page number (1-3)"),
    year_min: Optional[int] = Query(
        None, description="Filter by release year - from this year"
    ),
    year_max: Optional[int] = Query(
        None, description="Filter by release year - to this year"
    ),
):
    """
    Search for movies using hybrid search (vector + BM25)
    - Returns 20 movies per page
    - Supports up to 3 pages
    - Optional year filtering
    """
    try:
        # TODO: Students implement Weaviate hybrid search here
        # - Use vector search for semantic similarity
        # - Add BM25 for text relevance
        # - Implement pagination (limit=20, offset based on page)
        # - Add year filtering if provided
        page_size = 20
        if page >= 1:
            offset = page_size * (page - 1)

        filters = None
        if year_min and year_max:
            filters = Filter.by_property("release_date").greater_or_equal(
                datetime(year=year_min, month=1, day=1).replace(tzinfo=timezone.utc)
            ) & Filter.by_property("release_date").less_or_equal(
                datetime(year=year_max, month=12, day=31).replace(tzinfo=timezone.utc)
            )
        elif year_min:
            filters = Filter.by_property("release_date").greater_or_equal(
                datetime(year=year_min, month=1, day=1).replace(tzinfo=timezone.utc)
            )
        elif year_max:
            filters = Filter.by_property("release_date").less_or_equal(
                datetime(year=year_max, month=12, day=31).replace(tzinfo=timezone.utc)
            )

        with connect_to_weaviate() as client:
            movies = client.collections.get(CollectionName.MOVIES)
            response = movies.query.hybrid(
                query=q,
                offset=offset,
                limit=page_size,
                filters=filters,
                target_vector="default"
            )

            return SearchResponse(
                movies=[
                    o.properties for o in response.objects
                ],  # Convert response.objects to Movie models
                current_page=page,
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/movie/{movie_id}", response_model=MovieDetailResponse)
async def get_movie_details(movie_id: str):
    """
    Get detailed information about a specific movie
    - Returns movie metadata
    - Returns top 15 most similar movies using NearObject search
    """
    try:
        # TODO: Students implement here
        # - Fetch movie by ID from Weaviate
        # - Use nearObject search to find similar movies
        # - Limit to top 15 results

        # Placeholder response
        raise HTTPException(
            status_code=501,
            detail="Movie details not implemented yet. Students need to implement Weaviate object retrieval and similarity search.",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/explore", response_model=ExplorerResponse)
async def explore_movies(
    genre: str = Query(..., description="Movie genre to explore"),
    year: Optional[int] = Query(None, description="Filter by release year"),
):
    """
    Explore movies by genre and optional year
    - Returns most popular movies in the specified genre
    - Can filter by year
    - Sorted by popularity/rating
    """
    try:
        # TODO: Students implement here
        # - Filter by genre and year using Weaviate filters
        # - Sort by popularity/rating
        # - Use aggregation features if needed

        # Placeholder response
        raise HTTPException(
            status_code=501,
            detail="Explorer not implemented yet. Students need to implement Weaviate filtering and sorting.",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/recommend", response_model=RecommendationResponse)
async def recommend_movie(
    occasion: str = Query(
        ..., description="Viewing occasion (e.g., 'date night', 'family movie')"
    )
):
    """
    Get movie recommendations based on viewing occasion
    - Converts occasion to embedding
    - Performs semantic search against movie descriptions
    - Returns best match with reasoning
    """
    try:
        # TODO: Students implement here
        # - Convert occasion text to embedding
        # - Perform semantic search against movie plots/descriptions
        # - Return best match with explanation

        # Placeholder response
        raise HTTPException(
            status_code=501,
            detail="Recommender not implemented yet. Students need to implement semantic search with embeddings.",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/stats", response_model=StatsResponse)
async def get_dataset_stats():
    """
    Get dataset statistics
    - Total movie count
    - Count by year
    """
    try:
        # TODO: Students implement here
        # - Use Weaviate aggregation queries
        # - Get total count
        # - Group by year for time-based analysis

        # Placeholder response
        raise HTTPException(
            status_code=501,
            detail="Statistics not implemented yet. Students need to implement Weaviate aggregation queries.",
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


if __name__ == "__main__":


    uvicorn.run(app, host="0.0.0.0", port=8000)

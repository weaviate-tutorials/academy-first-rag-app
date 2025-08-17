from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List, Dict
from pydantic import BaseModel
from weaviate.classes.query import Filter, GenerativeConfig, Sort
from weaviate.classes.aggregate import GroupByAggregate
from datetime import datetime, timezone
from helpers import connect_to_weaviate, CollectionName, movie_occasion_to_query
import uvicorn


app = FastAPI(
    title="MovieInsights API",
    description="A movie discovery and recommendation platform using Weaviate",
    version="0.1.0",
)

PAGE_SIZE = 20


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
    year_min: Optional[int]
    year_max: Optional[int]


class RecommendationResponse(BaseModel):
    recommendation: str
    query_string: str
    movies_considered: List[Movie]
    occasion: str


class StatsResponse(BaseModel):
    total_movies: int
    movies_by_genres: Dict[str, int]
    oldest_movie: Movie
    most_recent_movie: Movie


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "MovieInsights API - Powered by Weaviate",
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
        if page >= 1:
            offset = PAGE_SIZE * (page - 1)

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
                limit=PAGE_SIZE,
                filters=filters,
                target_vector="default",
            )

            return SearchResponse(
                movies=[o.properties for o in response.objects],
                current_page=page,
            )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/movie/{movie_id}", response_model=MovieDetailResponse)
async def get_movie_details(movie_id: str):
    """
    Get detailed information about a specific movie, using the Weaviate object UUID
    - Returns movie metadata
    - Returns top 15 most similar movies using NearObject search
    """
    try:
        # TODO: Students implement here
        # - Fetch movie by ID from Weaviate
        # - Use nearObject search to find similar movies
        # - Limit to top n results
        with connect_to_weaviate() as client:
            movies = client.collections.get(CollectionName.MOVIES)
            movie = movies.query.fetch_objects(
                filters=Filter.by_property("movie_id").equal(int(movie_id)),
                limit=1
            ).objects[0]

            response = movies.query.near_object(near_object=movie.uuid, target_vector="default", limit=PAGE_SIZE)
            similar_movies = [o.properties for o in response.objects[1:]]  # Exclude itself

        return MovieDetailResponse(
            movie=movie.properties, similar_movies=similar_movies
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/explore", response_model=ExplorerResponse)
async def explore_movies(
    genre: str = Query(..., description="Movie genre to explore"),
    year_min: Optional[int] = Query(
        None, description="Filter by release year - from this year"
    ),
    year_max: Optional[int] = Query(
        None, description="Filter by release year - to this year"
    ),
):
    """
    Explore movies by genre(s) and optional year
    - Returns most popular movies best matching the specified genre
    - Can filter by year
    - Sorted by popularity/rating
    """
    try:
        # TODO: Students implement here
        # - Filter by genre and year using Weaviate filters
        # - Sort by popularity/rating
        with connect_to_weaviate() as client:
            movies = client.collections.get(CollectionName.MOVIES)

            if year_min and year_max:
                year_filters = Filter.by_property("release_date").greater_or_equal(
                    datetime(year=year_min, month=1, day=1).replace(tzinfo=timezone.utc)
                ) & Filter.by_property("release_date").less_or_equal(
                    datetime(year=year_max, month=12, day=31).replace(
                        tzinfo=timezone.utc
                    )
                )
            elif year_min:
                year_filters = Filter.by_property("release_date").greater_or_equal(
                    datetime(year=year_min, month=1, day=1).replace(tzinfo=timezone.utc)
                )
            elif year_max:
                year_filters = Filter.by_property("release_date").less_or_equal(
                    datetime(year=year_max, month=12, day=31).replace(
                        tzinfo=timezone.utc
                    )
                )
            else:
                year_filters = None

            response = movies.query.hybrid(
                query=genre,
                target_vector="genres",
                filters=year_filters,
                limit=PAGE_SIZE,
            )
            movies = sorted([o.properties for o in response.objects], key=lambda x: x["popularity"], reverse=True)

        return ExplorerResponse(
            movies=movies,
            genre=genre,
            year_min=year_min,
            year_max=year_max,
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
    - Generates a query string from occasion
    - Performs semantic search against movie descriptions
    - Returns best match with reasoning
    """
    try:
        # TODO: Students implement here
        # - Convert occasion text to embedding
        # - Perform semantic search against movie plots/descriptions
        # - Return best match with explanation
        query_string = movie_occasion_to_query(occasion=occasion)

        with connect_to_weaviate() as client:
            movies = client.collections.get(CollectionName.MOVIES)
            response = movies.generate.near_text(
                query=query_string,
                limit=PAGE_SIZE,
                grouped_task=f"""
                The user is interested in movie recommendations for this occasion:
                ========== OCCASION INPUT FROM USER ==========
                {occasion}
                ========== END INPUT ==========

                Out of these movies, what would you recommend and why?

                IMPORTANT: Only include the recommendation text in your response and nothing else.
                """,
                generative_provider=GenerativeConfig.anthropic(
                    model="claude-3-5-haiku-latest"
                ),
            )

        return RecommendationResponse(
            recommendation=response.generative.text,
            query_string=query_string,
            movies_considered=[o.properties for o in response.objects],
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
        # TODO: Students to implement:
        # - Use Weaviate aggregation queries
        # - Get total count
        # - Group by year for time-based analysis
        with connect_to_weaviate() as client:
            movies = client.collections.get(CollectionName.MOVIES)
            total_count = len(movies)
            movies_by_genres = movies.aggregate.over_all(
                group_by=GroupByAggregate(prop="genres")
            )
            oldest_movie = movies.query.fetch_objects(
                limit=1, sort=Sort.by_property("release_date", ascending=True)
            ).objects[0]
            latest_movie = movies.query.fetch_objects(
                limit=1, sort=Sort.by_property("release_date", ascending=False)
            ).objects[0]

        return StatsResponse(
            total_movies=len(total_count),
            movies_by_genres=[
                {g.grouped_by.value: g.total_count} for g in movies_by_genres.groups
            ],
            oldest_movie=oldest_movie,
            latest_movie=latest_movie,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8000)

from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from pydantic import BaseModel
from weaviate.classes.query import Filter, GenerativeConfig
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
    genres: Optional[list[str]] = None
    popularity: float
    year: int


class SearchResponse(BaseModel):
    movies: list[Movie]
    current_page: int


class MovieDetailResponse(BaseModel):
    movie: Movie
    similar_movies: list[Movie]


class ExplorerResponse(BaseModel):
    movies: list[Movie]
    genre: str
    year_min: Optional[int]
    year_max: Optional[int]


class RecommendationResponse(BaseModel):
    recommendation: str
    query_string: str
    movies_considered: list[Movie]
    occasion: str


class InfoResponse(BaseModel):
    movies_count: int
    sample_movies: list[Movie]


@app.get("/")
def root():
    """Root endpoint with API information"""
    return {
        "message": "MovieInsights API - Powered by Weaviate",
        "version": "1.0.0",
        "endpoints": [
            "/info - Get basic information about the dataset",
            "/search - Search movies by text",
            "/movie/{movie_id} - Get movie details and similar movies",
            "/explore - Explore movies by genre and year",
            "/recommend - Get movie recommendations for occasions",
        ],
    }


@app.get("/info", response_model=InfoResponse)
def get_dataset_info():
    """
    Get basic information about the dataset
    - Total movie count
    - Some example movies
    """
    try:
        with connect_to_weaviate() as client:
            # Student TODO:
            # - Get total count
            # - Fetch some movies
            # Write your code here according to the instructions
            sample_movies = [o.properties for o in sample_movies_response]

        return InfoResponse(movies_count=movies_count, sample_movies=sample_movies)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/search", response_model=SearchResponse)
def search_movies(
    q: str = Query(..., description="Search query for movies"),
    page: int = Query(1, ge=1, le=10, description="Page number (1-10)"),
    year_min: Optional[int] = Query(
        None, description="Filter by release year - from this year"
    ),
    year_max: Optional[int] = Query(
        None, description="Filter by release year - to this year"
    ),
):
    """
    Search for movies using hybrid search
    - Return 20 movies per page
    - Support pagination, up to 10 pages
    - Optional year filtering
    """
    try:
        if page >= 1:
            offset = PAGE_SIZE * (page - 1)

        filters = None
        if year_min and year_max:
            # Student TODO:
            # Build a filter (`filters`) where `year` is greater than or equal to `year_min`, and less than or equal to `year_max`
            # Write your code here according to the instructions
        elif year_min:
            # Student TODO: Build a filter (`filters`) where `year` is greater than or equal to `year_min`
            # Write your code here according to the instructions
        elif year_max:
            # Student TODO: Build a filter (`filters`) where `year` is less than or equal to `year_max`
            # Write your code here according to the instructions

        with connect_to_weaviate() as client:
            # Student TODO: Perform a hybrid search, with:
            # Query: q, offset: offset, limit: PAGE_SIZE, filters= filters, target "default" vector
            # Write your code here according to the instructions

        return SearchResponse(
            movies=[o.properties for o in response.objects],
            current_page=page,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/movie/{movie_id}", response_model=MovieDetailResponse)
def get_movie_details(movie_id: str):
    """
    Get detailed information about a specific movie, using the Weaviate object UUID
    - Returns movie metadata
    - Returns top 15 most similar movies
    """
    try:
        with connect_to_weaviate() as client:
            # Student TODO:
            # - Fetch movie by ID from Weaviate (the `movie_id` property should be `int(movie_id)` exactly)
            # - Use the retuend object's UUID to find PAGE_SIZE number of similar movies
            # Write your code here according to the instructions
            similar_movies = [
                o.properties for o in response.objects[1:]  # Exclude itself
            ]

        return MovieDetailResponse(
            movie=movie.properties, similar_movies=similar_movies
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/explore", response_model=ExplorerResponse)
def explore_movies(
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
        with connect_to_weaviate() as client:
            movies = client.collections.use(CollectionName.MOVIES)

            # Student TODO:
            # Build filters (`filters`) just like we did for `search_movies` above
            # Write your code here according to the instructions

            # Student TODO:
            # Perform a hybrid search for the given genres.
            # Target `genres` vector, apply the filters, and limit to PAGE_SIZE results
            # Write your code here according to the instructions
            sorted_movies = sorted(
                [o.properties for o in response.objects],
                key=lambda x: x["popularity"],
                reverse=True,
            )

        return ExplorerResponse(
            movies=sorted_movies,
            genre=genre,
            year_min=year_min,
            year_max=year_max,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@app.get("/recommend", response_model=RecommendationResponse)
def recommend_movie(
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
        query_string = movie_occasion_to_query(occasion=occasion)

        full_task_prompt = f"""
        The user is interested in movie recommendations for this occasion:
        ========== OCCASION INPUT FROM USER ==========
        {occasion}
        ========== END INPUT ==========

        Out of these movies, recommend 2-4 suitable movies, and describe why, so the user can choose for themselves.

        IMPORTANT: Only include the recommendation text in your response and nothing else.
        """

        with connect_to_weaviate() as client:
            movies = client.collections.use(CollectionName.MOVIES)
            # Student TODO:
            # Perform a RAG query (near_text) for the given query, using `full_task_prompt` constructed above.
            # Target `default` vector, and limit to PAGE_SIZE results
            # Specify `anthropic` as the generative provider, and `claude-3-5-haiku-latest` as the model
            # Write your code here according to the instructions

        return RecommendationResponse(
            recommendation=response.generative.text,
            query_string=query_string,
            movies_considered=[o.properties for o in response.objects],
            occasion=occasion
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8000)

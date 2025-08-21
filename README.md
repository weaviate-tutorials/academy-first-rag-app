## Student scenario

You are building an application for MovieInsights, a platform that helps movie enthusiasts discover and analyze films. Your application should do the following:

- Find films from a search string
    - List 20 most relevant movies per page, up to 3 pages
    - Allow optional year filters
- For a movie, show:
    - Movie data (title, release date, genre, etc.)
    - Top 15 most similar movies
- Explorer: Given a genre and optional release year, identify the most popular movies
- Recommender: Given a viewing occasion, perform a search & recommend a movie from the database
- Provide statistics on the dataset
    - Total object count; count by year

### Dataset

The application uses a curated movie dataset containing just under 20,00 movies.

### Learning objectives

You will learn how to connect a FastAPI app to Weaviate to:

- Implement hybrid search
    - With pagination
    - With filtering
- View individual object data
    - Perform a NearObject search
- Perform retrieval augmented generation
- Manage Weaviate collections
    - Create collections with vector configurations
    - Delete and rebuild collections
    - Handle batch data ingestion

### Project outline

You will be given a skeleton FastAPI-based application, along with scripts to:

- **Populate collection** (`populate.py`): Add movie data to the Weaviate collection
- **Delete collection** (`delete_collection.py`): Remove the collection when needed
- **Main application** (`main.py`): FastAPI app with endpoints for search, recommendations, and exploration

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

### Learning objectives

You will learn how to connect a FastAPI app to Weaviate to:

- Implement hybrid search
    - With pagination
    - With filtering
- View individual object data
    - Perform a NearObject search
- Sort filtered data
- Perform retrieval augmented generation
- Aggregate data

### Project outline

You will be given a skeleton FastAPI-based application,

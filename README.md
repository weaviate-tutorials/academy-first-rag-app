## Student scenario

You are building an application for SupportMe, an online platform that aims to help online support agents. Your application should:
- Identify common recent issues for each support account
- For a topic, find related issues
    - based on issues found, suggest what a support agent should say, or avoid saying
    - identify how often they've been occurring
    - if no account provided, identify common companies in the result set
    - if account provided, show trends in similar topics by quarter
- For an issue, show:
    - Issue data
    - AI summary
    - Top n most similar issues
- Provide statistics on the dataset
- Provide basic error handling
    - 400 for missing issue or account
    - 500 for internal errors


PROJECT TODO
- recreate dataset with snowflake embeddings
- set up app with weaviate embeddings + cloud / local + ollama
- Sample answer FastAPI app
    - Scaffold app; leave Weaviate functions for users to implement
    - `/<account_name>`: n most common issues; UUID;
    - `


- Get collection summary `/summary`
    - Total count
    - Counts by quarter
    - Counts for top companies
- n most recent issues `/recent`
    - UUIDs
    - Companies
    - RAG summary
- Summary for given account `/account_name/summary`
    - Total count
    - Latest count
    - n latest issues
    - RAG
- Search results `/topics/<search_topic>`
    -
- Deep-dive into each conversation
    - `/conversation/<id>`
    - Details
    - `/conversation/<id>/similar`
        - Trends (counts by quarter)
        -

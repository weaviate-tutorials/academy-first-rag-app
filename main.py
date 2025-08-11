from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from dotenv import load_dotenv
from helpers import connect_to_weaviate

# Load environment variables
load_dotenv()

app = FastAPI(
    title="Customer Support Search API",
    description="A simple API to search customer support conversations using Weaviate",
    version="1.0.0"
)

# Pydantic models for request/response
class SearchRequest(BaseModel):
    query: str
    limit: int = 5

class RAGRequest(BaseModel):
    question: str
    limit: int = 3

class SupportConversation(BaseModel):
    id: str
    customer_message: str
    agent_response: str
    issue_type: str
    sentiment: str
    brand: str

class SearchResponse(BaseModel):
    results: List[SupportConversation]
    total_found: int

class RAGResponse(BaseModel):
    answer: str
    sources: List[SupportConversation]

def get_weaviate_client():
    try:
        return connect_to_weaviate()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to Weaviate: {str(e)}")

@app.get("/")
def root():
    """Health check endpoint"""
    return {"message": "Customer Support Search API is running!"}

@app.post("/search", response_model=SearchResponse)
def search_conversations(request: SearchRequest):
    """
    Search for similar customer support conversations
    """
    try:
        with get_weaviate_client() as client:
            client.get_meta()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {e}")


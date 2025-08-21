# Weaviate Academy - First RAG App (Developer Guide)

This repository contains the complete implementation and student template for the **MovieInsights** application - a foundation-level capstone project for Weaviate Academy.

## Repository Structure

### Student Files (Templates)
These files contain TODO sections for students to implement:
- `main.py` - FastAPI application with search, recommendations, and exploration endpoints
- `populate.py` - Data ingestion script to populate Weaviate collection
- `delete_collection.py` - Collection management script with safety checks
- `helpers.py` - Shared utilities and connection logic

### Complete Implementation Files  
Reference implementations with full solutions:
- `main_complete.py` - Complete FastAPI app
- `populate_complete.py` - Complete data ingestion
- `delete_collection_complete.py` - Complete collection management

### Data Processing Scripts
Development utilities for preparing the dataset:
- `_dev_0_preproc.py` - Data preprocessing
- `_dev_1_build_dataset.py` - Dataset construction 
- `_dev_2_export_data.py` - Data export utilities
- `_dev_3_create_student_scripts.py` - **Converts complete files to student templates**

### Data Directory
Pre-processed movie data:
- `movies_popular_*.parquet` - Raw movie data files
- `movies_popular_w_vectors_*.parquet` - Movie data with embeddings

## Usage

### For Academy Instructors

1. **Generate student templates:**
   ```bash
   python _dev_3_create_student_scripts.py
   ```
   This strips solution code from `*_complete.py` files to create student templates.

2. **Distribute to students:**
   - `main.py`, `populate.py`, `delete_collection.py`
   - `helpers.py`, `data/` directory
   - `README.md` (student instructions)

### For Students

Follow the main `README.md` for project instructions and learning objectives.

## Learning Objectives Covered

- **Hybrid Search** with filtering and pagination
- **Vector Similarity** using nearObject queries  
- **Retrieval Augmented Generation** with Anthropic models
- **Collection Management** (create, delete, batch operations)
- **Multiple Vector Configurations** (text + genre embeddings)

## Technical Stack

- **Weaviate** - Vector database
- **FastAPI** - REST API framework
- **Anthropic Claude** - LLM for RAG
- **Pandas** - Data processing
- **UV** - Python package management

## Dataset

~20,000 curated movies with:
- Metadata (title, year, genres, popularity)
- Text embeddings (title + overview)  
- Genre embeddings (separate vector space)

Perfect size for foundation-level learning without overwhelming students.
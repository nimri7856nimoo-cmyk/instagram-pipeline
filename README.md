# Instagram Social Media Pipeline

## Features

- Instagram scraping using Apify
- MongoDB raw ingestion
- ETL post processing
- Prefect orchestration
- Docker Compose infrastructure
- Qdrant vector database setup

## Stack

- Python
- Prefect
- MongoDB
- Docker
- Apify
- Qdrant

## Run

```bash
docker compose up -d
python flows/stage1_scraper.py
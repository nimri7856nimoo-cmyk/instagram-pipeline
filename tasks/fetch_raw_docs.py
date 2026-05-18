from prefect import task
from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI)

db = client["instagram_pipeline"]

raw_collection = db["raw_docs"]


@task
def fetch_raw_docs():

    docs = list(
        raw_collection.find(
            {"status": "unprocessed"}
        )
    )

    print(f"Fetched {len(docs)} raw docs")

    return docs
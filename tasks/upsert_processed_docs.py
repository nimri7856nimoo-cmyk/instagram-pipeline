from prefect import task
from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI)

db = client["instagram_pipeline"]

processed_collection = db["processed_docs"]
raw_collection = db["raw_docs"]


@task
def upsert_processed_docs(cleaned_docs):

    count = 0

    for doc in cleaned_docs:

        processed_collection.update_one(
            {"_id": doc["_id"]},
            {"$set": doc},
            upsert=True
        )

        raw_collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"status": "processed"}}
        )

        count += 1

    print(f"Inserted {count} processed docs")

    return count
from prefect import task
from pymongo import MongoClient
from datetime import datetime
import os

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI)

db = client["instagram_pipeline"]

raw_collection = db["raw_docs"]


@task
def write_raw_docs(posts):

    inserted_ids = []

    for post in posts:

        post_id = str(post.get("id"))

        existing = raw_collection.find_one({"post_id": post_id})

        if existing:
            continue

        raw_doc = {
            "source": "instagram",
            "status": "unprocessed",
            "post_id": post_id,
            "raw": post,
            "scraped_at": datetime.utcnow().isoformat()
        }

        result = raw_collection.insert_one(raw_doc)

        inserted_ids.append(str(result.inserted_id))

    print(f"Inserted {len(inserted_ids)} documents into MongoDB")

    return inserted_ids
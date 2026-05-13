import os
from dotenv import load_dotenv
from prefect import task
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI)

db = client["instagram_pipeline"]

collection = db["raw_posts"]

@task
def write_raw_docs(posts):

    if posts:
        collection.insert_many(posts)
        print(f"Inserted {len(posts)} documents into MongoDB")

    else:
        print("No posts found")
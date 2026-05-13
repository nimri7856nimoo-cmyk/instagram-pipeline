from prefect import task
from pymongo import MongoClient
import os

MONGO_URI = os.getenv("MONGO_URI")

client = MongoClient(MONGO_URI)

db = client["instagram_pipeline"]

raw_collection = db["raw_posts"]
processed_collection = db["processed_posts"]


@task
def process_posts():

    posts = list(raw_collection.find())

    processed = []

    for profile in posts:

        latest_posts = profile.get("latestPosts", [])

        for post in latest_posts:

            cleaned_post = {
                "username": post.get("ownerUsername"),
                "caption": post.get("caption"),
                "likes": post.get("likesCount"),
                "timestamp": post.get("timestamp"),
                "image_url": post.get("displayUrl")
            }

            processed.append(cleaned_post)

    if processed:
        processed_collection.insert_many(processed)
        print(f"Processed {len(processed)} posts")

    return processed
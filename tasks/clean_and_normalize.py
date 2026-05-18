from prefect import task
from datetime import datetime


@task
def clean_and_normalize(raw_docs):

    cleaned_docs = []
    seen_post_ids = set()

    for doc in raw_docs:

        raw = doc["raw"]

        post_id = str(raw.get("id"))

        if post_id in seen_post_ids:
            continue

        seen_post_ids.add(post_id)

        caption = raw.get("caption", "")
        clean_caption = caption.encode(
            "ascii",
            "ignore"
        ).decode().strip()

        username = raw.get("ownerUsername", "")
        username = username.replace("@", "").lower()

        cleaned_doc = {
            "_id": doc["_id"],
            "source": "instagram",
            "status": "pending_analysis",
            "post_id": post_id,
            "username": username,
            "caption": clean_caption,
            "likes": int(raw.get("likesCount", 0)),
            "comments_count": int(raw.get("commentsCount", 0)),
            "content_type": raw.get("type", "").lower(),
            "hashtags": raw.get("hashtags", []),
            "posted_at": raw.get("timestamp"),
            "processed_at": datetime.utcnow().isoformat()
        }

        cleaned_docs.append(cleaned_doc)

    print(f"Cleaned {len(cleaned_docs)} documents")

    return cleaned_docs
import os
from dotenv import load_dotenv
from prefect import task
from apify_client import ApifyClient

load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_TOKEN")

@task
def call_apify_actor(username="nike"):
    client = ApifyClient(APIFY_TOKEN)

    run_input = {
        "usernames": [username],
        "resultsLimit": 5
    }

    run = client.actor(
        "apify/instagram-profile-scraper"
    ).call(run_input=run_input)

    items = []

    for item in client.dataset(
        run["defaultDatasetId"]
    ).iterate_items():
        items.append(item)

    print(f"Fetched {len(items)} posts")

    return items
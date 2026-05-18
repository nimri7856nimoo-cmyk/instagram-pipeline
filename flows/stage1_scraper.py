from prefect import flow

from tasks.call_apify_actor import call_apify_actor
from tasks.write_raw_docs import write_raw_docs
from tasks.process_posts import process_posts


@flow(name="instagram-pipeline", log_prints=True)
def apify_instagram_scraper(username: str = "nike"):

    posts = call_apify_actor(username)

    write_raw_docs(posts)

    processed = process_posts()

    return processed


if __name__ == "__main__":
    apify_instagram_scraper("nike")
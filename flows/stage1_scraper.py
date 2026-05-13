from prefect import flow

from tasks.call_apify_actor import call_apify_actor
from tasks.write_raw_docs import write_raw_docs
from tasks.process_posts import process_posts


@flow
def instagram_pipeline(username: str):

    posts = call_apify_actor(username)

    write_raw_docs(posts)

    process_posts()


if __name__ == "__main__":
    instagram_pipeline("nike")
from prefect import flow

from tasks.fetch_raw_docs import fetch_raw_docs
from tasks.clean_and_normalize import clean_and_normalize
from tasks.upsert_processed_docs import upsert_processed_docs


@flow(log_prints=True)
def transform_instagram_docs():

    raw_docs = fetch_raw_docs()

    if not raw_docs:
        print("No raw docs found")
        return 0

    cleaned_docs = clean_and_normalize(raw_docs)

    count = upsert_processed_docs(cleaned_docs)

    return count


if __name__ == "__main__":
    transform_instagram_docs()
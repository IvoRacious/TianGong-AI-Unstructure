import logging
import os
import pickle
import requests
import time
import random
from datetime import UTC, datetime

import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    filename="esg_pickle.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    force=True,
)

token = os.environ.get("TOKEN")
input_dir = "docs/esg"
output_dir = "docs/processed_docs/esg_pickle"
pdf_url = "http://localhost:8770/mineru"


conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)

with conn_pg.cursor() as cur:
    cur.execute(
        "SELECT id FROM esg_meta WHERE created_time > '2025-10-01' AND unstructure_time IS NULL"
    )
    records = cur.fetchall()


def unstructure_by_service(doc_id, doc_path, token, url):
    """Process document through the appropriate unstructure service"""
    pickle_filename = f"{doc_id}.pkl"
    pickle_path = os.path.join(output_dir, pickle_filename)

    # Check if pickle file already exists
    if os.path.exists(pickle_path):
        logging.info(f"Pickle file already exists for document ID: {doc_id}, skipping.")
        return doc_id

    try:
        with open(doc_path, "rb") as f:
            files = {"file": f}
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.post(url, files=files, headers=headers)
            response.raise_for_status()
            response_data = response.json()

        result = response_data.get("result")

        with open(pickle_path, "wb") as pkl_file:
            pickle.dump(result, pkl_file)

        logging.info(
            f"Document ID: {doc_id} processed successfully with service: {url}"
        )
        time.sleep(1 + 2 * random.random())
        return doc_id

    except Exception as e:
        logging.error(
            f"Error processing document ID: {doc_id} with service {url}: {str(e)}"
        )
        return None


def process_documents():
    """Process documents sequentially using the single configured service"""
    documents = []
    for record in records:
        doc_id = record[0]
        doc_path = f"{input_dir}/{doc_id}.pdf"

        if os.path.exists(doc_path):
            documents.append((doc_id, doc_path))
        else:
            logging.info(f"File not found for ID {doc_id}: {doc_path}")

    if not documents:
        logging.info("No documents to process.")
        return

    for doc_id, doc_path in documents:
        result = unstructure_by_service(doc_id, doc_path, token, pdf_url)
        if result:
            logging.info(f"{doc_id} processed with service: {pdf_url}")
            try:
                with conn_pg.cursor() as cur:
                    cur.execute(
                        "UPDATE esg_meta SET unstructure_time = %s WHERE id = %s",
                        (datetime.now(UTC), doc_id),
                    )
                    conn_pg.commit()
                    logging.info(f"Updated unstructure_time for {doc_id}")
            except Exception as e:
                logging.error(
                    f"Error updating unstructure_time for document ID: {doc_id}: {str(e)}"
                )
        else:
            logging.error(
                f"Failed to process document ID: {doc_id} with service: {pdf_url}"
            )


# Run the document processing function
if __name__ == "__main__":
    process_documents()
    conn_pg.close()

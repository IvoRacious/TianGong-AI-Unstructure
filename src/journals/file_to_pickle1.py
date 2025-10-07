import os
import requests
import pickle
from urllib.parse import quote
from datetime import UTC, datetime
import psycopg2
import logging
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(
    filename="journal_redo_1.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
    filemode="w",
    force=True,
)

token = os.environ.get("TOKEN")
base_dir = "docs/journals/"
output_dir = "docs/processed_docs/journal_new_pickle"
pdf_url = "http://localhost:8770/mineru"


def unstructure_by_service(doc_path, file_id, url, token):
    with open(doc_path, "rb") as f:
        pickle_filename = f"{file_id}.pkl"
        pickle_path = os.path.join(output_dir, pickle_filename)

        files = {"file": f}
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(url, files=files, headers=headers)
        response.raise_for_status()
        response_data = response.json()
        result = response_data.get("result")

        with open(pickle_path, "wb") as pkl_file:
            pickle.dump(result, pkl_file)


conn_pg = psycopg2.connect(
    database=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
)


with open("part1_journals.pkl", "rb") as f:
    data = pickle.load(f)

# 读取 error_file_id_1.csv 里的 id 到集合
error_file_ids = set()
with open("error_file_id_1.csv", "r") as ef:
    for line in ef:
        error_file_ids.add(line.strip())

for result in data:
    file_id = result[0]
    # 如果在 error_file_id_1.csv 里，跳过
    if file_id in error_file_ids:
        logging.info(f"Skipping file_id in error_file_id_1.csv: {file_id}")
        continue
    doi = result[1]

    coded_doi = quote(quote(doi))
    file_path = os.path.join(base_dir, coded_doi + ".pdf")
    pickle_path = os.path.join(output_dir, f"{file_id}.pkl")

    # 如果pickle存在直接跳过，不写log
    if os.path.exists(pickle_path):
        continue
    else:
        if os.path.exists(file_path):
            try:
                logging.info(f"Processing {file_id} at {file_path}")
                unstructure_by_service(file_path, file_id, pdf_url, token)
            except Exception as e:
                logging.error(f"Error during unstructure for {file_id}: {e}")
                try:
                    with open("error_file_id_1.csv", "a") as ef:
                        ef.write(f"{file_id}\n")
                except Exception as write_err:
                    logging.error(
                        f"Failed to append error file_id for {file_id}: {write_err}"
                    )
                continue

            try:
                with conn_pg.cursor() as cur:
                    cur.execute(
                        "UPDATE journals SET upload_time = %s WHERE id = %s",
                        (datetime.now(UTC), file_id),
                    )
                    conn_pg.commit()
                    logging.info(f"Updated upload_time for {file_id}")
            except Exception as e:
                logging.error(f"Error updating database for {file_id}: {e}")
        else:
            logging.info(f"PDF file not found for {file_id}: {file_path}")

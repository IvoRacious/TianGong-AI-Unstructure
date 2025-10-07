import os
import logging
from datetime import UTC, datetime
from typing import Iterable, List, Set

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_batch


def _project_root() -> str:
    here = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(here, "..", ".."))


def collect_pickle_ids(pickle_dir: str) -> Set[str]:
    """Collects pickle IDs from filenames, assuming they are UUIDs or strings."""
    ids: Set[str] = set()
    if not os.path.isdir(pickle_dir):
        logging.warning("Pickle directory does not exist: %s", pickle_dir)
        return ids
    for name in os.listdir(pickle_dir):
        if not name.endswith(".pkl"):
            continue
        stem = name[:-4]
        ids.add(stem)
    return ids


def main() -> None:
    load_dotenv()

    logging.basicConfig(
        filename="journal.log",
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s",
        filemode="w",
        force=True,
    )

    cutoff = datetime(2025, 6, 25, tzinfo=UTC)
    now_ts = datetime.now(UTC)

    root = _project_root()
    pickle_dir = os.path.join(root, "processed_docs", "journal_new_pickle")

    all_ids = sorted(collect_pickle_ids(pickle_dir))
    logging.info("Found %d pickle IDs to check", len(all_ids))
    if not all_ids:
        return

    conn_pg = psycopg2.connect(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )

    # try:
    with conn_pg.cursor() as cur:
        cur.execute(
            "SELECT id, upload_time FROM journals WHERE id IN %s",
            (tuple(all_ids),),
        )
        db_results = cur.fetchall()
        logging.info("Fetched %d records from database.", len(db_results))

        ids_to_update = [
            doc_id
            for doc_id, upload_time in db_results
            if upload_time.replace(tzinfo=UTC) < cutoff
        ]

        logging.info("Found %d IDs to update.", len(ids_to_update))
        logging.info("IDs to update: %s", ids_to_update)
        logging.info("New timestamp will be: %s", now_ts)

        if not ids_to_update:
            return

        update_data = [(now_ts, doc_id) for doc_id in ids_to_update]

        execute_batch(
            cur,
            "UPDATE journals SET upload_time = %s WHERE id = %s",
            update_data,
        )
        updated_count = cur.rowcount
        conn_pg.commit()
        logging.info("Batch updated %d rows.", updated_count)
    # finally:
    #     try:
        conn_pg.close()
    #     except Exception:
    #         pass


if __name__ == "__main__":
    main()


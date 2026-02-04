import logging
import os
import pickle
import time
from csv import writer as csv_writer
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.parse import quote

import requests
from dotenv import load_dotenv
import psycopg2
import psycopg2.pool

load_dotenv()

API_BASE = (
    os.environ.get("TWO_STAGE_BASE")
    or os.environ.get("MINERU_TASK_BASE")
    or "http://localhost:7770"
).rstrip("/")
SUBMIT_URL = f"{API_BASE}/two_stage/task"
LOG_FILE = "celery_two_stage.log"
DEFAULT_OUTPUT_DIR = Path("journal_pickle_queue")
DEFAULT_JOURNALS_PKL = Path("journals_all.pkl")
PDF_BASE_DIR = Path(os.environ.get("JOURNAL_PDF_DIR") or "docs/journals")
DEFAULT_INTERVAL = float(os.environ.get("TWO_STAGE_POLL_INTERVAL", 3))
DEFAULT_TIMEOUT = float(os.environ.get("TWO_STAGE_POLL_TIMEOUT", 800))
DEFAULT_BATCH_INTERVAL = float(os.environ.get("TWO_STAGE_BATCH_INTERVAL", 10))
MAX_ATTEMPTS = 3  # initial attempt + up to 2 retries
BATCH_SIZE = 1000
MAX_DB_CONNECTIONS = int(os.environ.get("TWO_STAGE_DB_MAXCONN", 4))
DEFAULT_ERROR_CSV = Path(
    os.environ.get("TWO_STAGE_ERROR_CSV") or "error_file_id_two_stage.csv"
)

VISION_PROVIDER = (os.environ.get("VISION_PROVIDER") or "").strip()
VISION_MODEL = (os.environ.get("VISION_MODEL") or "").strip()
VISION_PROMPT = (os.environ.get("VISION_PROMPT") or "").strip()
PRIORITY = (os.environ.get("TWO_STAGE_PRIORITY") or "normal").strip().lower() or "normal"


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


CHUNK_TYPE = _bool_env("TWO_STAGE_CHUNK_TYPE", True)
RETURN_TXT = _bool_env("TWO_STAGE_RETURN_TXT", False)

db_pool: psycopg2.pool.ThreadedConnectionPool | None = None

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    filemode="w",
    force=True,
)


def _build_form_data() -> Dict[str, str]:
    form: Dict[str, str] = {}
    form["priority"] = PRIORITY
    if VISION_PROVIDER:
        form["provider"] = VISION_PROVIDER
    if VISION_MODEL:
        form["model"] = VISION_MODEL
    if VISION_PROMPT:
        form["prompt"] = VISION_PROMPT
    if CHUNK_TYPE:
        form["chunk_type"] = "true"
    if RETURN_TXT:
        form["return_txt"] = "true"
    return form


def submit_task(session: requests.Session, pdf_path: Path, token: str) -> str:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    form_data = _build_form_data()
    logging.info(
        "Submitting %s with priority=%s provider=%s model=%s",
        pdf_path,
        form_data.get("priority", "<default>"),
        form_data.get("provider", "<default>"),
        form_data.get("model", "<default>"),
    )
    with pdf_path.open("rb") as f:
        resp = session.post(
            SUBMIT_URL,
            files={"file": f},
            data=form_data,
            headers=headers,
            timeout=120,
        )
    resp.raise_for_status()
    data = resp.json()
    task_id = data.get("task_id")
    if not task_id:
        raise RuntimeError(f"Task ID missing in response for {pdf_path}")
    logging.info("Submitted %s -> task %s", pdf_path, task_id)
    return task_id


def fetch_status(
    session: requests.Session,
    task_id: str,
    token: str,
) -> Dict:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    resp = session.get(
        f"{API_BASE}/two_stage/task/{task_id}",
        headers=headers,
        timeout=30000,
    )
    resp.raise_for_status()
    return resp.json()


@dataclass(frozen=True)
class JournalItem:
    file_id: str
    doi: str
    pdf_path: Path


def init_db_pool() -> None:
    global db_pool
    if db_pool is not None:
        return
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=MAX_DB_CONNECTIONS,
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )
    logging.info("Database connection pool created.")


def close_db_pool() -> None:
    global db_pool
    if db_pool is None:
        return
    db_pool.closeall()
    db_pool = None
    logging.info("Database connection pool closed.")


def update_upload_time(file_id: str) -> bool:
    if db_pool is None:
        raise RuntimeError("Database pool not initialized")
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE journals SET upload_time = %s WHERE id = %s",
                (datetime.now(UTC), file_id),
            )
            conn.commit()
        logging.info("Updated upload_time for %s", file_id)
        return True
    except Exception as exc:
        logging.error("DB update failed for %s: %s", file_id, exc)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            db_pool.putconn(conn)


def iter_journal_batches(pkl_path: Path, batch_size: int) -> Iterable[List[Tuple[str, str]]]:
    if not pkl_path.exists():
        raise FileNotFoundError(f"Journal pickle not found: {pkl_path}")
    with pkl_path.open("rb") as f:
        data = pickle.load(f)
    if not isinstance(data, list):
        data = list(data)
    for start in range(0, len(data), batch_size):
        yield data[start : start + batch_size]


def build_pdf_path(doi: str) -> Path:
    quoted_doi = quote(quote(doi))
    return PDF_BASE_DIR / f"{quoted_doi}.pdf"


def append_failure_id(error_csv: Path, file_id: str) -> None:
    error_csv.parent.mkdir(parents=True, exist_ok=True)
    with error_csv.open("a", newline="") as f:
        csv_writer(f).writerow([file_id])


def main() -> None:
    token = os.environ.get("FASTAPI_BEARER_TOKEN")
    if not token:
        raise RuntimeError("FASTAPI_BEARER_TOKEN not found in environment")

    journals_pkl = Path(DEFAULT_JOURNALS_PKL)
    output_dir = Path(DEFAULT_OUTPUT_DIR)
    error_csv = Path(DEFAULT_ERROR_CSV)
    output_dir.mkdir(parents=True, exist_ok=True)

    init_db_pool()
    session = requests.Session()
    try:
        attempts: Dict[str, int] = {}
        successes: Dict[str, str] = {}
        failures: Dict[str, str] = {}
        db_failures: Dict[str, str] = {}
        recorded_failures: set[str] = set()

        has_work = False

        for batch in iter_journal_batches(journals_pkl, BATCH_SIZE):
            tasks: Dict[str, JournalItem] = {}
            run_start_times: Dict[str, float] = {}
            batch_had_tasks = False

            for entry in batch:
                if not entry or len(entry) < 2:
                    continue
                file_id, doi = entry[0], entry[1]
                if not file_id or not doi:
                    continue

                pickle_path = output_dir / f"{file_id}.pkl"
                if pickle_path.exists():
                    continue

                pdf_path = build_pdf_path(doi)
                if not pdf_path.exists():
                    logging.info("PDF missing for %s (%s): %s", file_id, doi, pdf_path)
                    continue

                task_id = None
                attempts[file_id] = 0
                while attempts[file_id] < MAX_ATTEMPTS:
                    attempts[file_id] += 1
                    try:
                        task_id = submit_task(session, pdf_path, token)
                        break
                    except Exception as exc:
                        logging.error(
                            "Failed to submit %s (%s) attempt %d/%d: %s",
                            file_id,
                            pdf_path,
                            attempts[file_id],
                            MAX_ATTEMPTS,
                            exc,
                        )
                        if attempts[file_id] < MAX_ATTEMPTS:
                            time.sleep(DEFAULT_INTERVAL)
                if task_id is None:
                    failures[file_id] = "submit_task failed"
                    if file_id not in recorded_failures:
                        append_failure_id(error_csv, file_id)
                        recorded_failures.add(file_id)
                    continue
                tasks[task_id] = JournalItem(file_id=file_id, doi=doi, pdf_path=pdf_path)

            if not tasks:
                continue

            batch_had_tasks = True
            has_work = True
            logging.info("Starting batch with %d tasks", len(tasks))

            while tasks:
                finished: Dict[str, JournalItem] = {}
                for task_id, item in list(tasks.items()):
                    try:
                        data = fetch_status(session, task_id, token)
                        state = data.get("state")
                        if not state:
                            raise RuntimeError(f"Task {task_id} response missing state: {data}")
                        if state == "SUCCESS":
                            result = data.get("result") or data.get("Result")
                            if result is None:
                                raise RuntimeError(f"Task {task_id} succeeded without result")
                        elif state in {"FAILURE", "REVOKED"}:
                            raise RuntimeError(f"Task failed: {data.get('error')}")
                        else:
                            if state == "STARTED" and task_id not in run_start_times:
                                run_start_times[task_id] = time.time()
                            started_at = run_start_times.get(task_id)
                            if started_at is not None:
                                elapsed = time.time() - started_at
                                if elapsed >= DEFAULT_TIMEOUT:
                                    raise TimeoutError(f"Task {task_id} timeout")
                            continue
                        pickle_path = output_dir / f"{item.file_id}.pkl"
                        with pickle_path.open("wb") as f:
                            pickle.dump(result, f)
                        logging.info("Wrote %s", pickle_path)
                        if update_upload_time(item.file_id):
                            successes[item.file_id] = task_id
                        else:
                            db_failures[item.file_id] = "upload_time update failed"
                        finished[task_id] = item
                    except TimeoutError:
                        error_msg = f"timeout after {DEFAULT_TIMEOUT:.1f}s"
                        logging.error(
                            "Task %s timed out for %s (%s)", task_id, item.pdf_path, error_msg
                        )
                        finished[task_id] = item
                        attempts[item.file_id] = attempts.get(item.file_id, 1)
                        if attempts[item.file_id] < MAX_ATTEMPTS:
                            attempts[item.file_id] += 1
                            logging.info(
                                "Retrying %s (attempt %d/%d)...",
                                item.pdf_path,
                                attempts[item.file_id],
                                MAX_ATTEMPTS,
                            )
                            new_task = submit_task(session, item.pdf_path, token)
                            tasks[new_task] = item
                        else:
                            failures[item.file_id] = error_msg
                            if item.file_id not in recorded_failures:
                                append_failure_id(error_csv, item.file_id)
                                recorded_failures.add(item.file_id)
                    except Exception as exc:
                        logging.error(
                            "Failed to process %s (task %s): %s", item.pdf_path, task_id, exc
                        )
                        finished[task_id] = item
                        attempts[item.file_id] = attempts.get(item.file_id, 1)
                        if attempts[item.file_id] < MAX_ATTEMPTS:
                            attempts[item.file_id] += 1
                            logging.info(
                                "Retrying %s (attempt %d/%d)...",
                                item.pdf_path,
                                attempts[item.file_id],
                                MAX_ATTEMPTS,
                            )
                            new_task = submit_task(session, item.pdf_path, token)
                            tasks[new_task] = item
                        else:
                            failures[item.file_id] = str(exc)
                            if item.file_id not in recorded_failures:
                                append_failure_id(error_csv, item.file_id)
                                recorded_failures.add(item.file_id)

                for task_id in finished:
                    tasks.pop(task_id, None)
                    run_start_times.pop(task_id, None)

                if tasks:
                    time.sleep(DEFAULT_INTERVAL)
            if batch_had_tasks:
                logging.info("Batch complete; sleeping %.1fs", DEFAULT_BATCH_INTERVAL)
                time.sleep(DEFAULT_BATCH_INTERVAL)

        if not has_work:
            logging.info("No eligible journals found to enqueue.")
            return

        logging.info(
            "Finished two-stage enqueue: %d successes, %d failures, %d db failures.",
            len(successes),
            len(failures),
            len(db_failures),
        )
        for file_id, err in failures.items():
            logging.error(
                "Failed after %d attempts: %s (%s)", attempts.get(file_id, 0), file_id, err
            )
            if file_id not in recorded_failures:
                append_failure_id(error_csv, file_id)
                recorded_failures.add(file_id)
        for file_id, err in db_failures.items():
            logging.error("DB update failed for %s (%s)", file_id, err)
    finally:
        session.close()
        close_db_pool()


if __name__ == "__main__":
    main()

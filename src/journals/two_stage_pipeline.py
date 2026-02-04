# 1、配置与常量加载。

import logging # 导入日志模块
import os # 导入操作系统模块
import pickle # 导入pickle模块
import time # 导入时间模块
from dataclasses import dataclass # 导入数据结构
from pathlib import Path # 导入路径模块
from typing import Dict, Iterable, Iterator, List, Optional # 导入类型注解模块（字典、可迭代对象类型、迭代器类型、列表、可选等）
from datetime import UTC, datetime # 导入时间模块

import psycopg2 # 导入psycopg2库，用于连接PostgreSQL数据库
import psycopg2.pool # 导入连接池模块
import requests # 导入请求模块
from dotenv import load_dotenv # 导入环境变量加载模块

load_dotenv()

## 读取环境变量，配置常量，设置 API 地址、目录、批量参数、超时等
API_BASE = (
    os.environ.get("TWO_STAGE_BASE")
    or os.environ.get("MINERU_TASK_BASE")
    or "http://localhost:7770" 
).rstrip("/") # 去除末尾斜杠

SUBMIT_URL = f"{API_BASE}/two_stage/task" # 提交任务的URL
LOG_FILE = "celery_two_stage_normal.log" # 写死日志文件名

DEFAULT_INPUT_DIR = Path("docs/journals") # 输入地址
DEFAULT_OUTPUT_DIR = Path("docs/processed_docs/journal_two_stage_pickle") # 输出地址
DEFAULT_INTERVAL = float(os.environ.get("TWO_STAGE_POLL_INTERVAL", 3)) # 轮询间隔
DEFAULT_TIMEOUT = float(os.environ.get("TWO_STAGE_POLL_TIMEOUT", 800)) # 轮询超时

PENDING_TIMEOUT = float(os.environ.get("TWO_STAGE_PENDING_TIMEOUT", 5000)) # 待处理超时
MAX_ATTEMPTS = int(os.environ.get("TWO_STAGE_MAX_ATTEMPTS", 3)) # 最大尝试次数
MAX_WORKERS = int(os.environ.get("TWO_STAGE_DB_WORKERS", 4)) # 最大工作线程数
BATCH_SIZE = int(os.environ.get("TWO_STAGE_BATCH_SIZE", 5000)) # 批处理大小
DB_FETCH_SIZE = int(os.environ.get("TWO_STAGE_DB_FETCH_SIZE", 1000)) # 数据库提取大小
SUBMIT_TIMEOUT = float(os.environ.get("TWO_STAGE_SUBMIT_TIMEOUT", 120)) # 提交超时
STATUS_TIMEOUT = float(os.environ.get("TWO_STAGE_STATUS_TIMEOUT", 30000)) # 状态超时

VISION_PROVIDER = (os.environ.get("VISION_PROVIDER") or "").strip() # 视觉提供商
VISION_MODEL = (os.environ.get("VISION_MODEL") or "").strip() # 视觉模型
VISION_PROMPT = (os.environ.get("VISION_PROMPT") or "").strip() # 视觉提示
PRIORITY = (os.environ.get("TWO_STAGE_PRIORITY") or "normal").strip().lower() or "normal" # 优先级


## 获取表单数据
def _build_form_data() -> Dict[str, str]:
    form: Dict[str, str] =  {}
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


# 2、配置日志。

logging.basicConfig( 
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s", # 日志格式
    filemode="w",
    force=True,
)

def _bool_env(name: str, default: bool = False) -> bool: # 定义布尔环境变量
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# 3、定义数据结构类。
@dataclass(frozen=True)
class WorkItem:
    file_id: str # uuid
    doi: str # DOI
    pdf_path: Path

CHUNK_TYPE = _bool_env("TWO_STAGE_CHUNK_TYPE", True) # 是否分块类型
RETURN_TXT = _bool_env("TWO_STAGE_RETURN_TXT", False) # 是否返回文本


# 4、初始化数据库连接池。

db_pool = None # 把变量 db_pool 先初始化为空值（None）用于保存数据库连接池对象

def init_db_pool() -> None:
    """Initializes the database connection pool.""" 
    global db_pool
    try:
        db_pool = psycopg2.pool.ThreadSafeConnectionPool(
            minconn=1,
            maxconn=MAX_WORKERS,
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
        )
        logging.info("Database connection pool created successfully.")
    except psycopg2.OperationalError as exc:
        logging.error("Failed to create database connection pool: %s", exc)
        raise

def close_db_pool() -> None: # 关闭数据库连接池
    """Closes all connections in the pool."""
    if db_pool:
        db_pool.closeall()
        logging.info("Database connection pool closed.")


# 5、定位PDF，按「父目录名/文件名主干」建立索引。

def _build_pdf_index(base_dir: Path) -> Dict[str, Path]:
    index: Dict[str, Path] = {}
    if not base_dir.exists(): 
        return index 
    for path in base_dir.rglob("*.pdf"): # 递归遍历目录里所有pdf文件
        key = f"{path.parent.name}/{path.stem}"
        if key not in index:
            index[key] = path
        else:
            logging.warning("Duplicate PDF key %s: %s", key, path)
    return index

def _resolve_pdf_path(doi: str, pdf_index: Dict[str, Path]) -> Optional[Path]: # 通过 DOI 定位 PDF 文件路径
    return pdf_index.get(doi)


# 6、待处理任务获取，从数据库拉取 upload_time 为空的记录。！！

# 查数据库 → 用 doi 找 PDF → 遍历目录下的pdf文件，跳过已有 pickle → 产出待处理项
def iter_unprocessed_items( 
    base_dir: Path, output_dir: Path, pdf_index: Dict[str, Path]
) -> Iterator[WorkItem]:
    if not base_dir.exists():
        logging.error("PDF base directory not found: %s", base_dir) # 目录不存在就直接停止
        return
    if not pdf_index:
        logging.error("No PDFs found under %s", base_dir) # 没有 PDF 文件直接停止
        return
    if not db_pool:
        raise RuntimeError("Database pool is not initialized") # 数据库连接池未初始化就停止

    conn = None # 初始化连接变量
    try:
        conn = db_pool.getconn() # 从连接池获取数据库连接
        with conn.cursor() as cur:
            cur.execute("SELECT id, doi FROM journals WHERE upload_time IS NULL") # 拉取 upload_time 为空的记录
            while True:
                rows = cur.fetchmany(DB_FETCH_SIZE) # 分批获取记录，避免一次性加载太多
                if not rows:
                    break
                for file_id, doi in rows:
                    if not doi:
                        logging.warning("Missing DOI for id=%s; skipping", file_id) # DOI 为空就跳过
                        continue
                    pdf_path = _resolve_pdf_path(doi, pdf_index)
                    if not pdf_path:
                        logging.info(
                            "PDF file not found for %s (doi=%s)", # 找不到对应 PDF 文件就跳过，并会打印 doi，方便确认文件名映射
                            file_id,
                            doi,
                        )
                        continue
                    pickle_path = output_dir / f"{file_id}.pkl"
                    if pickle_path.exists():
                        logging.info("Pickle already exists for %s; skipping", file_id) # 已有 pickle 文件就跳过
                        continue
                    yield WorkItem(file_id=file_id, doi=doi, pdf_path=pdf_path) # 产出待处理项
    finally:
        if conn:
            db_pool.putconn(conn) # 连接池获取失败时，不在 finally 里报错：先 conn = None，回收时加 if conn


# 7、任务提交与状态查询。


## 提交任务（PDF提交到两阶段处理接口，并拿回task ID）
def submit_task(session: requests.Session, pdf_path: Path, token: str) -> str:
    headers = {"Authorization": f"Bearer {token}"} if token else {} # 如果有 token，就加上授权头
    form_data = _build_form_data() # 生成表单参数
    logging.info(
        "Submitting %s with priority=%s provider=%s model=%s", 
        pdf_path,
        form_data.get("priority", "<default>"),
        form_data.get("provider", "<default>"),
        form_data.get("model", "<default>"),
    )
    with pdf_path.open("rb") as f: # 以二进制方式打开 PDF 文件
        resp = session.post(
            SUBMIT_URL,
            files={"file": f}, # 上传文件
            data=form_data, # 表单数据
            headers=headers, # 请求头
            timeout=SUBMIT_TIMEOUT, # 防止卡死
        )
    resp.raise_for_status()
    data = resp.json() # 解析服务端返回的 JSON
    task_id = data.get("task_id") # 取出任务 ID，如果没有就报错
    if not task_id:
        raise RuntimeError(f"Task ID missing in response for {pdf_path}")
    logging.info("Submitted %s -> task %s", pdf_path, task_id)
    return task_id # 返回任务 ID，后续用它去轮询状态


## 轮询查询任务状态
def fetch_status(
    session: requests.Session, 
    task_id: str, 
    token: str
) -> Dict:
    headers = {"Authorization": f"Bearer {token}"} if token else {} 
    resp = session.get(
        f"{API_BASE}/two_stage/task/{task_id}",
        headers=headers,
        timeout=30000, # 超时时间写死
    )
    resp.raise_for_status()
    return resp.json()


# 8、结果存储为 pickle 文件。 一次性提取id、doi（根据doi匹配）

def _write_pickle(output_dir: Path, file_id: str, result: object) -> Path:
    pickle_path = output_dir / f"{file_id}.pkl"
    with pickle_path.open("wb") as f:
        pickle.dump(result, f)
    return pickle_path

# 9、更新数据库中的 upload_time。
def update_upload_time(file_id: str) -> None:
    if not db_pool:
        raise RuntimeError("Database pool is not initialized")
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
    except Exception as exc:
        logging.error("DB Error processing %s: %s", file_id, exc)
        if conn:
            conn.rollback()
    finally:
        if conn:
            db_pool.putconn(conn)


## 批量处理与重试，按批提交、轮询、超时重试、统计成功失败数量。
def _process_batch(
    session: requests.Session,
    token: str,
    batch: List[WorkItem],
    output_dir: Path,
    attempts: Dict[str, int],
    failures: Dict[str, str],
) -> int:
    tasks: Dict[str, WorkItem] = {}
    run_start_times: Dict[str, float] = {}
    submit_times: Dict[str, float] = {} # 记录提交时间
    success_count = 0

    for item in batch:
        attempts[item.file_id] = attempts.get(item.file_id, 0) + 1
        try:
            task_id = submit_task(session, item.pdf_path, token)
        except Exception as exc:
            logging.error("Submit failed for %s: %s", item.pdf_path, exc)
            failures[item.file_id] = str(exc)
            continue
        submit_times[task_id] = time.time()  # 记录提交时间
        tasks[task_id] = item

    if not tasks:
        return success_count

    while tasks:
        finished: List[str] = []
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
                    submitted_at = submit_times.get(task_id)
                    if submitted_at is not None:
                        pending_elapsed = time.time() - submitted_at
                        if pending_elapsed >= PENDING_TIMEOUT:
                            raise TimeoutError(f"Task {task_id} pending timeout")
                    started_at = run_start_times.get(task_id)
                    if started_at is not None:
                        elapsed = time.time() - started_at
                        if elapsed >= DEFAULT_TIMEOUT:
                            raise TimeoutError(f"Task {task_id} timeout")
                    continue

                pickle_path = _write_pickle(output_dir, item.file_id, result)
                logging.info("Wrote %s", pickle_path)
                update_upload_time(item.file_id)
                finished.append(task_id)
                success_count += 1
            except TimeoutError as exc:
                error_msg = str(exc)
                if "pending" not in error_msg.lower():
                    error_msg = f"timeout after {DEFAULT_TIMEOUT:.1f}s"
                logging.error("Task %s timed out for %s (%s)", task_id, item.pdf_path, error_msg)
                finished.append(task_id)
                if attempts.get(item.file_id, 1) < MAX_ATTEMPTS:
                    attempts[item.file_id] += 1
                    logging.info(
                        "Retrying %s (attempt %d/%d)...",
                        item.pdf_path,
                        attempts[item.file_id],
                        MAX_ATTEMPTS,
                    )
                    try:
                        new_task = submit_task(session, item.pdf_path, token)
                    except Exception as retry_exc:
                        logging.error(
                            "Retry submit failed for %s: %s",
                            item.pdf_path,
                            retry_exc,
                        )
                        failures[item.file_id] = str(retry_exc)
                    else:
                        tasks[new_task] = item
                else:
                    failures[item.file_id] = error_msg
            except Exception as exc:
                logging.error("Failed to process %s (task %s): %s", item.pdf_path, task_id, exc)
                finished.append(task_id)
                if attempts.get(item.file_id, 1) < MAX_ATTEMPTS:
                    attempts[item.file_id] += 1
                    logging.info(
                        "Retrying %s (attempt %d/%d)...",
                        item.pdf_path,
                        attempts[item.file_id],
                        MAX_ATTEMPTS,
                    )
                    try:
                        new_task = submit_task(session, item.pdf_path, token)
                    except Exception as retry_exc:
                        logging.error(
                            "Retry submit failed for %s: %s",
                            item.pdf_path,
                            retry_exc,
                        )
                        failures[item.file_id] = str(retry_exc)
                    else:
                        tasks[new_task] = item
                else:
                    failures[item.file_id] = str(exc)

        for task_id in finished:
            tasks.pop(task_id, None)
            run_start_times.pop(task_id, None)

        if tasks:
            time.sleep(DEFAULT_INTERVAL)

    return success_count

## 批量处理所有待处理项
def process_items(session: requests.Session, token: str, items: Iterable[WorkItem]) -> None:
    attempts: Dict[str, int] = {}
    failures: Dict[str, str] = {}
    total_success = 0

    batch: List[WorkItem] = []
    for item in items:
        batch.append(item)
        if len(batch) >= BATCH_SIZE:
            total_success += _process_batch(session, token, batch, OUTPUT_DIR, attempts, failures)
            batch = []

    if batch:
        total_success += _process_batch(session, token, batch, OUTPUT_DIR, attempts, failures)

    logging.info(
        "Finished two-stage pipeline: %d successes, %d failures.",
        total_success,
        len(failures),
    )
    for file_id, err in failures.items():
        logging.error(
            "Failed after %d attempts: %s (%s)",
            attempts.get(file_id, 0),
            file_id,
            err,
        )


# 9、主函数入口，环境变量读取，目录创建，数据库连接池初始化，任务处理调度。
def main() -> None:
    token = os.environ.get("FASTAPI_BEARER_TOKEN") or os.environ.get("TOKEN")
    if not token:
        raise RuntimeError("FASTAPI_BEARER_TOKEN/TOKEN not found in environment")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    init_db_pool()
    session = requests.Session()
    try:
        pdf_index = _build_pdf_index(BASE_DIR)
        items = iter_unprocessed_items(BASE_DIR, OUTPUT_DIR, pdf_index)
        process_items(session, token, items)
    finally:
        session.close()
        close_db_pool()


if __name__ == "__main__":
    main()

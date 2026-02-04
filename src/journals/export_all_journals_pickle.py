import json
import os
import pickle
import time
from dotenv import load_dotenv

import psycopg2

load_dotenv()

def load_checkpoint(path: str) -> tuple[str | None, int]:
    if not os.path.exists(path):
        return None, 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        last_id = data.get("last_id")
        total = int(data.get("total", 0))
        if not last_id:
            return None, total
        return str(last_id), total
    except Exception:
        # Backward-compat: checkpoint might be a plain integer.
        try:
            with open(path, "r", encoding="utf-8") as f:
                value = int(f.read().strip() or "0")
            # Old numeric checkpoint isn't compatible with UUID ids.
            if value <= 0:
                return None, 0
            return None, 0
        except Exception:
            return None, 0


def save_checkpoint(path: str, last_id: str | None, total: int) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump({"last_id": last_id, "total": total}, f, ensure_ascii=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)


def connect_pg():
    return psycopg2.connect(
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
    )

def main() -> None:
    output_path = "journals_all.pkl"
    checkpoint_path = f"{output_path}.checkpoint"
    batch_size = 10000

    last_id, total = load_checkpoint(checkpoint_path)
    if last_id and not os.path.exists(output_path):
        print("⚠️ 检测到断点文件但输出文件不存在，将从头开始导出。")
        last_id = None
        total = 0

    mode = "ab" if last_id else "wb"
    batch_idx = 0
    max_retries = 5
    retries = 0

    with open(output_path, mode) as f:
        while True:
            try:
                conn_pg = connect_pg()
                with conn_pg:
                    with conn_pg.cursor() as cur:
                        if last_id:
                            cur.execute(
                                "SELECT id, doi FROM journals WHERE id > %s::uuid "
                                "ORDER BY id LIMIT %s;",
                                (last_id, batch_size),
                            )
                        else:
                            cur.execute(
                                "SELECT id, doi FROM journals ORDER BY id LIMIT %s;",
                                (batch_size,),
                            )
                        rows = cur.fetchall()
                conn_pg.close()

                if not rows:
                    break

                pickle.dump(rows, f, protocol=pickle.HIGHEST_PROTOCOL)
                f.flush()
                total += len(rows)
                last_id = str(rows[-1][0])
                batch_idx += 1
                save_checkpoint(checkpoint_path, last_id, total)
                print(f"✅ 批次 {batch_idx} 已写入，累计 {total} 条，last_id={last_id}。")
                retries = 0
            except Exception as exc:
                retries += 1
                print(f"⚠️ 批次导出失败（{retries}/{max_retries}）：{exc}")
                if retries >= max_retries:
                    raise
                time.sleep(2)

    print(f"✅ 已保存数据库记录到 {output_path}，共 {total} 条。")


if __name__ == "__main__":
    main()

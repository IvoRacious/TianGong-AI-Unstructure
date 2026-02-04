import os
import pickle
import math
from pathlib import Path

# ===== 参数 =====
parts_files = [f"part{i}_journals.pkl" for i in range(1, 6)]
processed_folder = Path("processed_docs/journal_new_pickle")
num_parts = 8

def to_str_id(x):
    # 统一成字符串，避免 int/str 不一致导致的无法匹配
    return str(x).strip()

# 1) 合并所有原始ID
all_ids = []
for fp in parts_files:
    with open(fp, "rb") as f:
        ids = pickle.load(f)
    print(f"{fp} 读取 {len(ids):,} 条")
    all_ids.extend(ids)

all_ids_set = set(map(to_str_id, all_ids))
print(f"合并后去重总量: {len(all_ids_set):,}")

# 2) 读取 processed 文件名作为候选已处理ID（仅取去掉 .pkl 的“主干名”）
processed_candidate_ids = set()
for name in os.listdir(processed_folder):
    if not name.endswith(".pkl"):
        continue
    stem = name[:-4]  # 去掉 .pkl
    processed_candidate_ids.add(to_str_id(stem))

print(f"候选已处理（按文件名）数量: {len(processed_candidate_ids):,}")

# 3) 只保留与 all_ids 相交的“有效已处理ID”
processed_ids = processed_candidate_ids & all_ids_set
print(f"有效已处理ID数量（与原始ID求交后）: {len(processed_ids):,}")

# 4) 计算未处理
unprocessed_ids = sorted(all_ids_set - processed_ids)
print(f"未处理ID数量: {len(unprocessed_ids):,}")

# 5) 平均分成 8 份并保存
chunk_size = math.ceil(len(unprocessed_ids) / num_parts) if unprocessed_ids else 0
for i in range(num_parts):
    part = unprocessed_ids[i * chunk_size : (i + 1) * chunk_size] if chunk_size else []
    out_file = f"unprocessed_part_{i+1}.pkl"
    with open(out_file, "wb") as f:
        pickle.dump(part, f)
    print(f"保存 {out_file}（{len(part):,} 条）")

# 6) 简要校验信息
print("\n=== 校验 ===")
print(f"原始ID总数（去重）：{len(all_ids_set):,}")
print(f"已处理ID（候选）：{len(processed_candidate_ids):,}")
print(f"已处理ID（有效）：{len(processed_ids):,}  | 占原始比例：{len(processed_ids)/max(1,len(all_ids_set)):.2%}")
print(f"未处理ID：{len(unprocessed_ids):,}  | 理论应在 ~{len(all_ids_set)-len(processed_ids):,}")

# Journal Pickle Queue Agent 使用说明

## 作用
- 读取单个元数据 pkl（默认 `part1_journals.pkl`），调用 unstructure 服务生成对应的 pickle，成功后更新数据库 upload_time，失败写入错误 CSV。
- 通过线程+队列控制并发，默认指向单一服务 `http://localhost:7770/mineru_sci`（服务自身有 4 个 worker）。

## 前置
- 环境变量：`TOKEN`、`POSTGRES_DB`、`POSTGRES_USER`、`POSTGRES_PASSWORD`、`POSTGRES_HOST`、`POSTGRES_PORT`。
- 目录：PDF 放在 `docs/journals/`；输出 pickle 在 `docs/processed_docs/journal_new_pickle`（脚本会自动创建）。
- 元数据：默认 `part1_journals.pkl`，可换成其他 pkl。

## 运行
- 默认参数：  
  `python src/journals/file_to_pickle_queue.py`
- 自定义参数：  
  - `--service-url` 指定服务地址（默认 `http://localhost:7770/mineru_sci`）  
  - `--workers` 线程数，默认 4  
  - `--input` 单个元数据 pkl（默认 `part1_journals.pkl`）  
  - `--error-file` 失败 ID 的 CSV（默认 `error_file_id_1.csv`）  
  - `--output-dir` 输出 pickle 的目录（默认 `docs/processed_docs/journal_new_pickle`）
  
  示例：`python src/journals/file_to_pickle_queue.py --workers 4 --input part2_journals.pkl --error-file error_file_id_2.csv --output-dir test/queue`

## 记录与失败重试
- 日志：`journal_queue.log`。
- 失败 ID：追加到 error CSV（线程安全），下次运行会自动跳过这些失败 ID。
- 已有 pickle 会跳过，不再请求服务。

## 其他
- 需要停止旧的多进程脚本时，可自行 `pkill -f "file_to_pickle*.py"` 后再启动本队列脚本。
pkill -f "file_to_pickle_queue.py"

# Databricks notebook source
# MAGIC %md
# MAGIC # データの取り込み（Data Ingestion）
# MAGIC
# MAGIC このノートブックでは、Databricks 上でのデータ取り込み方法を体系的に学びます。
# MAGIC CSV・JSON・Parquet などの生ファイルの読み込みから、**Auto Loader** による増分取り込みまでカバーします。
# MAGIC
# MAGIC **データの取り込みとは？**
# MAGIC > 外部のデータソース（ファイル、データベース、ストリーミングなど）から
# MAGIC > Databricks 上のテーブルにデータを読み込む処理です。
# MAGIC > データパイプラインの最初のステップであり、後続の処理の品質を左右します。
# MAGIC
# MAGIC ### データ取り込みの流れ
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐     ┌───────────────┐     ┌──────────────┐
# MAGIC │  外部ソース    │     │  取り込み方法   │     │  Delta Table │
# MAGIC │  CSV/JSON/    │ ──→ │  • Spark Read  │ ──→ │  (Bronze層)   │
# MAGIC │  Parquet/DB   │     │  • COPY INTO   │     │              │
# MAGIC │              │     │  • Auto Loader │     │              │
# MAGIC └──────────────┘     └───────────────┘     └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - Spark での各種ファイル形式の読み込み（CSV, JSON, Parquet）
# MAGIC - COPY INTO コマンドによるデータ取り込み
# MAGIC - Auto Loader（cloudFiles）による増分データ取り込み
# MAGIC - スキーマの推論と進化（Schema Inference & Evolution）
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime（例: 16.x）のクラスター
# MAGIC - クラスターサイズは **シングルノード（Single Node）** で十分です
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. サンプルデータの準備
# MAGIC
# MAGIC まず、ハンズオンで使用するサンプルデータを作成します。
# MAGIC EC サイトの注文データを模擬したデータセットを使います。
# MAGIC
# MAGIC > **Unity Catalog ボリューム（Volumes）** を使ってファイルを管理します。
# MAGIC > ボリュームは Unity Catalog で管理されるファイルストレージです。
# MAGIC > 従来の DBFS（`/tmp/` 等）は Unity Catalog 環境では無効化されているため、
# MAGIC > `/Volumes/<catalog>/<schema>/<volume>/` パスを使用します。

# COMMAND ----------

import json
from datetime import datetime, timedelta
import random
import os
import shutil

# Unity Catalog ボリュームの設定
CATALOG = "main"
SCHEMA = "default"
VOLUME_NAME = "data_engineer_handson"

# ボリュームの作成（存在しない場合）
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")

# サンプルデータの保存先（Unity Catalog Volume）
BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"

# 前回のデータがあれば削除して再作成（ノートブックの再実行に対応）
for subdir in ["csv", "json", "parquet", "checkpoints"]:
    subdir_path = f"{BASE_PATH}/{subdir}"
    if os.path.exists(subdir_path):
        shutil.rmtree(subdir_path)
    os.makedirs(subdir_path)

# --- CSV データの作成 ---
csv_data = "order_id,customer_id,product_name,quantity,price,order_date\n"
products = ["ノートPC", "マウス", "キーボード", "モニター", "USBメモリ", "ヘッドセット"]
random.seed(42)

for i in range(1, 101):
    product = random.choice(products)
    qty = random.randint(1, 5)
    price = random.randint(500, 50000)
    date = (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 180))).strftime("%Y-%m-%d")
    csv_data += f"{i},{random.randint(1000, 1099)},{product},{qty},{price},{date}\n"

with open(f"{BASE_PATH}/csv/orders.csv", "w") as f:
    f.write(csv_data)

# --- JSON データの作成 ---
json_records = []
for i in range(1, 51):
    record = {
        "customer_id": random.randint(1000, 1099),
        "name": f"顧客_{random.randint(1000, 1099)}",
        "email": f"user{random.randint(1, 100)}@example.com",
        "city": random.choice(["東京", "大阪", "名古屋", "福岡", "札幌"]),
        "registered_at": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).isoformat(),
    }
    json_records.append(json.dumps(record, ensure_ascii=False))

with open(f"{BASE_PATH}/json/customers.json", "w") as f:
    f.write("\n".join(json_records))

print("サンプルデータを作成しました:")
print(f"  ボリューム: {CATALOG}.{SCHEMA}.{VOLUME_NAME}")
print(f"  CSV: {BASE_PATH}/csv/orders.csv")
print(f"  JSON: {BASE_PATH}/json/customers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. CSV ファイルの読み込み
# MAGIC
# MAGIC CSV（Comma-Separated Values）は最も一般的なデータ形式の一つです。
# MAGIC Spark では `spark.read.csv()` または `spark.read.format("csv")` で読み込みます。
# MAGIC
# MAGIC ### 主なオプション
# MAGIC
# MAGIC | オプション | 説明 | 例 |
# MAGIC |---|---|---|
# MAGIC | `header` | 1行目をヘッダーとして使用 | `True` |
# MAGIC | `inferSchema` | データ型を自動推論 | `True` |
# MAGIC | `sep` | 区切り文字を指定 | `,`（デフォルト） |
# MAGIC | `encoding` | 文字エンコーディング | `UTF-8` |
# MAGIC | `multiLine` | 複数行にまたがるフィールド | `True` |

# COMMAND ----------

# CSV ファイルの読み込み
df_csv = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BASE_PATH}/csv/orders.csv")
)

print(f"読み込み件数: {df_csv.count()}")
print("\nスキーマ:")
df_csv.printSchema()

# COMMAND ----------

display(df_csv.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: inferSchema とは？**
# MAGIC >
# MAGIC > `inferSchema=True` を指定すると、Spark がデータの内容を見て自動的に型を判定します。
# MAGIC > 例: "123" → IntegerType、"2024-01-01" → StringType（日付は明示指定が安全）
# MAGIC > 指定しない場合、すべての列が StringType になります。
# MAGIC >
# MAGIC > **注意**: inferSchema はデータを2回読む必要があるため、大量データでは遅くなることがあります。
# MAGIC > 本番環境ではスキーマを明示的に定義するのがベストプラクティスです。

# COMMAND ----------

# スキーマを明示的に定義する方法
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
)

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True),
    StructField("order_date", DateType(), True),
])

df_csv_typed = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(order_schema)
    .load(f"{BASE_PATH}/csv/orders.csv")
)

print("明示スキーマで読み込み:")
df_csv_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. JSON ファイルの読み込み
# MAGIC
# MAGIC JSON（JavaScript Object Notation）はネストされた構造を持てる柔軟なデータ形式です。
# MAGIC API のレスポンスやログデータでよく使われます。
# MAGIC
# MAGIC | 種類 | 説明 |
# MAGIC |---|---|
# MAGIC | 単一行 JSON | 1行に1つの JSON オブジェクト（デフォルト） |
# MAGIC | 複数行 JSON | JSON オブジェクトが複数行にまたがる |

# COMMAND ----------

# JSON ファイルの読み込み
df_json = (
    spark.read.format("json")
    .option("multiLine", "false")  # 1行1レコードの JSONL 形式
    .load(f"{BASE_PATH}/json/customers.json")
)

print(f"読み込み件数: {df_json.count()}")
print("\nスキーマ:")
df_json.printSchema()

# COMMAND ----------

display(df_json.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Parquet ファイルの読み込み
# MAGIC
# MAGIC **Parquet** は列指向のバイナリ形式で、ビッグデータ処理で最も効率的な形式の一つです。
# MAGIC
# MAGIC | 特徴 | CSV | JSON | Parquet |
# MAGIC |---|---|---|---|
# MAGIC | 形式 | テキスト | テキスト | バイナリ（列指向） |
# MAGIC | スキーマ | なし | 暗黙的 | 内蔵 |
# MAGIC | 圧縮 | 低い | 低い | 高い（Snappy等） |
# MAGIC | 読み込み速度 | 遅い | 遅い | 高速 |
# MAGIC | 列選択（射影） | 全列読み込み | 全列読み込み | 必要な列のみ読み込み可能 |
# MAGIC
# MAGIC > **試験ポイント**: Parquet は列指向形式のため、SELECT で一部の列だけ取得する場合に非常に高速です。
# MAGIC > Delta Lake は内部的に Parquet ファイルを使用しています。

# COMMAND ----------

# CSV データを Parquet 形式で保存
df_csv.write.mode("overwrite").parquet(f"{BASE_PATH}/parquet/orders")
print("Parquet 形式で保存しました")

# Parquet ファイルの読み込み（スキーマ指定不要）
df_parquet = spark.read.parquet(f"{BASE_PATH}/parquet/orders")
print(f"\n読み込み件数: {df_parquet.count()}")
print("\nスキーマ（Parquet に埋め込まれている）:")
df_parquet.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. COPY INTO によるデータ取り込み
# MAGIC
# MAGIC `COPY INTO` は、クラウドストレージからDelta テーブルにデータを**べき等**にコピーするSQL コマンドです。
# MAGIC
# MAGIC **べき等（idempotent）とは？**
# MAGIC > 同じコマンドを何回実行しても結果が同じになる性質です。
# MAGIC > `COPY INTO` は既に取り込み済みのファイルをスキップするため、重複取り込みが発生しません。
# MAGIC
# MAGIC ### COPY INTO の特徴
# MAGIC
# MAGIC | 特徴 | 説明 |
# MAGIC |---|---|
# MAGIC | べき等 | 同じファイルを重複して取り込まない |
# MAGIC | 対応形式 | CSV, JSON, Parquet, Avro, ORC, Text |
# MAGIC | スキーマ指定 | テーブルのスキーマに従う |
# MAGIC | 増分取り込み | 新しいファイルのみ自動的に処理 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COPY INTO 用のテーブルを作成
# MAGIC CREATE OR REPLACE TABLE default.orders_copy_into (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   product_name STRING,
# MAGIC   quantity INT,
# MAGIC   price INT,
# MAGIC   order_date DATE
# MAGIC )

# COMMAND ----------

# COPY INTO でデータを取り込む
# SELECT サブクエリで明示的に型をキャストし、スキーマの不一致を防ぐ
spark.sql(f"""
COPY INTO default.orders_copy_into
FROM (
  SELECT
    CAST(order_id AS INT) AS order_id,
    CAST(customer_id AS INT) AS customer_id,
    product_name,
    CAST(quantity AS INT) AS quantity,
    CAST(price AS INT) AS price,
    CAST(order_date AS DATE) AS order_date
  FROM '{BASE_PATH}/csv/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
""")
print("COPY INTO が完了しました")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 取り込み結果を確認
# MAGIC SELECT COUNT(*) AS total_rows FROM default.orders_copy_into

# COMMAND ----------

# 2回目の実行 → 重複取り込みされないことを確認
spark.sql(f"""
COPY INTO default.orders_copy_into
FROM (
  SELECT
    CAST(order_id AS INT) AS order_id,
    CAST(customer_id AS INT) AS customer_id,
    product_name,
    CAST(quantity AS INT) AS quantity,
    CAST(price AS INT) AS price,
    CAST(order_date AS DATE) AS order_date
  FROM '{BASE_PATH}/csv/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
""")
print("2回目の COPY INTO が完了しました（べき等）")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 件数が変わらないことを確認（べき等）
# MAGIC SELECT COUNT(*) AS total_rows FROM default.orders_copy_into

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: `COPY INTO` は同じファイルを再処理しないべき等な操作です。
# MAGIC > 少量〜中量のファイルを定期的に取り込む場合に適しています。
# MAGIC > 大量のファイルや頻繁な取り込みには Auto Loader がより適しています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Auto Loader（cloudFiles）による増分取り込み
# MAGIC
# MAGIC **Auto Loader** は、クラウドストレージに新しく到着したファイルを自動的に検出して取り込む機能です。
# MAGIC Structured Streaming を使って実装されています。
# MAGIC
# MAGIC ### Auto Loader vs COPY INTO
# MAGIC
# MAGIC | 特徴 | COPY INTO | Auto Loader |
# MAGIC |---|---|---|
# MAGIC | 処理方式 | バッチ | ストリーミング（増分） |
# MAGIC | ファイル検出 | 毎回ディレクトリをスキャン | 通知ベース / リストベース |
# MAGIC | スケーラビリティ | 数千ファイルまで | 数百万ファイル以上 |
# MAGIC | スキーマ進化 | 手動 | 自動対応可能 |
# MAGIC | 推奨シーン | 小〜中規模、不定期 | 大規模、継続的な取り込み |
# MAGIC
# MAGIC ### Auto Loader の仕組み
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────┐         ┌──────────────┐         ┌──────────────┐
# MAGIC │ クラウドストレージ  │  検出   │  Auto Loader  │  書込み  │  Delta Table │
# MAGIC │ (新ファイル到着)   │ ──────→ │ (cloudFiles)  │ ──────→ │              │
# MAGIC │                 │         │ • スキーマ推論  │         │              │
# MAGIC │                 │         │ • チェックポイント│         │              │
# MAGIC └─────────────────┘         └──────────────┘         └──────────────┘
# MAGIC ```

# COMMAND ----------

# Auto Loader によるデータ取り込み
# 前回の実行結果をクリーンアップ（再実行に対応）
spark.sql("DROP TABLE IF EXISTS default.orders_autoloader")
checkpoint_path = f"{BASE_PATH}/checkpoints/orders_autoloader"

df_autoloader = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BASE_PATH}/csv/")
)

print("Auto Loader のスキーマ:")
df_autoloader.printSchema()

# COMMAND ----------

# Auto Loader のデータを Delta テーブルに書き込み
query = (
    df_autoloader.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)  # 現在利用可能なデータをすべて処理して停止
    .toTable("default.orders_autoloader")
)

query.awaitTermination()
print("Auto Loader による取り込みが完了しました")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 取り込み結果を確認
# MAGIC SELECT COUNT(*) AS total_rows FROM default.orders_autoloader

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: trigger(availableNow=True) とは？**
# MAGIC >
# MAGIC > Auto Loader は通常ストリーミングで「常時稼働」しますが、
# MAGIC > `trigger(availableNow=True)` を指定すると**バッチ的に動作**します。
# MAGIC > 「今あるファイルをすべて処理して、完了したら停止する」という動作です。
# MAGIC > 定期的なジョブで Auto Loader を使う場合によく使われるパターンです。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auto Loader のスキーマ進化（Schema Evolution）
# MAGIC
# MAGIC Auto Loader は新しいカラムが追加された場合に自動的にスキーマを更新できます。
# MAGIC
# MAGIC | オプション | 説明 |
# MAGIC |---|---|
# MAGIC | `cloudFiles.inferColumnTypes` | カラム型を自動推論 |
# MAGIC | `cloudFiles.schemaEvolutionMode` | スキーマ進化の方式（`addNewColumns`, `rescue`, `none`） |
# MAGIC | `cloudFiles.schemaHints` | 特定カラムの型をヒントとして指定 |
# MAGIC | `cloudFiles.schemaLocation` | 推論されたスキーマの保存先 |
# MAGIC
# MAGIC > **試験ポイント**: Auto Loader は `cloudFiles.schemaLocation` にスキーマを保存し、
# MAGIC > 新しいカラムが検出された場合にスキーマを自動的に進化させることができます。
# MAGIC > `_rescued_data` カラムには、スキーマに合わないデータが自動的に格納されます。
# MAGIC
# MAGIC > **動作メモ**: `schemaEvolutionMode="addNewColumns"` は新しいカラムを検出すると
# MAGIC > `UNKNOWN_FIELD_EXCEPTION` で意図的に1度失敗します。スキーマ情報を更新した後、
# MAGIC > ストリームを再起動すると新しいスキーマで正常に取り込まれます。

# COMMAND ----------

# スキーマ進化のデモ: 新しいカラムを持つデータを追加
csv_data_v2 = "order_id,customer_id,product_name,quantity,price,order_date,discount\n"
for i in range(101, 111):
    product = random.choice(products)
    qty = random.randint(1, 5)
    price = random.randint(500, 50000)
    date = (datetime(2024, 7, 1) + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
    discount = random.choice([0, 5, 10, 15, 20])
    csv_data_v2 += f"{i},{random.randint(1000, 1099)},{product},{qty},{price},{date},{discount}\n"

with open(f"{BASE_PATH}/csv/orders_v2.csv", "w") as f:
    f.write(csv_data_v2)
print("新しいカラム（discount）を含むデータを追加しました")

# COMMAND ----------

# Auto Loader で再度取り込み（新しいファイルのみ処理される）
# schemaEvolutionMode="addNewColumns" は新しいカラムを検出すると
# スキーマを更新した上で意図的に1度失敗し、再起動時に新スキーマで取り込みます。
checkpoint_path_v2 = f"{BASE_PATH}/checkpoints/orders_autoloader"

for attempt in range(2):
    try:
        df_autoloader_v2 = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", checkpoint_path_v2)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(f"{BASE_PATH}/csv/")
        )

        query_v2 = (
            df_autoloader_v2.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_path_v2)
            .option("mergeSchema", "true")
            .outputMode("append")
            .trigger(availableNow=True)
            .toTable("default.orders_autoloader")
        )

        query_v2.awaitTermination()
        print("スキーマ進化を含むデータの取り込みが完了しました")
        break
    except Exception as e:
        if "UNKNOWN_FIELD_EXCEPTION" in str(e) and attempt == 0:
            print("新しいカラムを検出しました。スキーマを更新して再試行します...")
        else:
            raise

# COMMAND ----------

# MAGIC %sql
# MAGIC -- スキーマが進化したことを確認（discount カラムが追加されている）
# MAGIC DESCRIBE TABLE default.orders_autoloader

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 新しいデータを確認
# MAGIC SELECT * FROM default.orders_autoloader
# MAGIC WHERE order_id >= 101
# MAGIC ORDER BY order_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. ファイル形式の選び方
# MAGIC
# MAGIC | シナリオ | 推奨形式 | 理由 |
# MAGIC |---|---|---|
# MAGIC | 外部システムからのエクスポート | CSV / JSON | 汎用性が高い |
# MAGIC | Databricks 内部でのデータ保存 | Delta (Parquet) | 性能・機能が最良 |
# MAGIC | API レスポンスの保存 | JSON | ネスト構造に対応 |
# MAGIC | 大量データの分析 | Parquet / Delta | 列指向で高速 |
# MAGIC | ストリーミングデータ | Auto Loader + Delta | 増分処理に最適 |
# MAGIC
# MAGIC > **試験ポイント**: Databricks では最終的に **Delta Lake** 形式でデータを保存するのがベストプラクティスです。
# MAGIC > 生データは CSV/JSON で受け取り、Delta テーブルに変換して保存する流れが一般的です。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **CSV の読み込み** — `spark.read.csv()` でヘッダー・スキーマ推論を指定
# MAGIC 2. **JSON の読み込み** — ネスト構造を持つデータの取り込み
# MAGIC 3. **Parquet の読み込み** — 列指向フォーマットの効率的な読み込み
# MAGIC 4. **COPY INTO** — べき等なデータコピー（SQL ベース）
# MAGIC 5. **Auto Loader** — `cloudFiles` による増分取り込みとスキーマ進化
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - **次のノートブック `03_sql_dml_operations.py`** で SQL の DDL/DML 操作を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Engineer Associate):
# MAGIC > - **Development and Ingestion (30%)**: Auto Loader のソースと構文、COPY INTO、スキーマ推論と進化、ファイル形式の選択

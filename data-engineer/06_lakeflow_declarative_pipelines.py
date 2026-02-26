# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Declarative Pipelines（旧 Delta Live Tables）
# MAGIC
# MAGIC このノートブックでは、**Lakeflow Spark Declarative Pipelines** の概念と機能を学びます。
# MAGIC
# MAGIC **Lakeflow Declarative Pipelines とは？**
# MAGIC > ETL パイプラインを**宣言的に**定義するフレームワークです。
# MAGIC > 「何を（What）」処理するかを記述すれば、「どのように（How）」実行するかは
# MAGIC > Databricks が自動的に管理します。
# MAGIC >
# MAGIC > 以前は **Delta Live Tables (DLT)** と呼ばれていましたが、
# MAGIC > 2024年に **Lakeflow Declarative Pipelines** に名称変更されました。
# MAGIC
# MAGIC ### 従来の ETL vs Declarative Pipelines
# MAGIC
# MAGIC ```
# MAGIC 従来の ETL（命令型）              Declarative Pipelines（宣言型）
# MAGIC ┌──────────────────┐            ┌──────────────────┐
# MAGIC │ 1. データを読む     │            │ @dlt.table        │
# MAGIC │ 2. 変換する        │            │ def my_table():   │
# MAGIC │ 3. エラー処理する   │            │   return (        │
# MAGIC │ 4. 書き込む        │            │     spark.read...  │
# MAGIC │ 5. チェックポイント │            │     .filter(...)   │
# MAGIC │ 6. 監視する        │            │   )               │
# MAGIC └──────────────────┘            └──────────────────┘
# MAGIC    すべて自分で管理                  フレームワークが管理
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - Lakeflow Declarative Pipelines の基本概念
# MAGIC - ストリーミングテーブルとマテリアライズドビュー
# MAGIC - データ品質制約（Expectations）
# MAGIC - パイプラインの設定と管理
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime（例: 16.x）のクラスター
# MAGIC
# MAGIC > **注意**: Lakeflow Declarative Pipelines のコードは通常 **DLT パイプライン内で実行** されます。
# MAGIC > このノートブックでは概念の理解とコードパターンの学習に重点を置きます。
# MAGIC > 実際のパイプラインは Lakeflow Jobs（旧 Workflows）から作成・実行します。
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lakeflow Declarative Pipelines の基本概念
# MAGIC
# MAGIC ### 主要なコンポーネント
# MAGIC
# MAGIC | コンポーネント | 説明 | 特徴 |
# MAGIC |---|---|---|
# MAGIC | **ストリーミングテーブル** | 追記のみのテーブル | 新データのみ処理（増分処理） |
# MAGIC | **マテリアライズドビュー** | 集計結果を実体化したテーブル | 全データを再計算して更新 |
# MAGIC | **Expectations** | データ品質制約 | 品質違反時の動作を定義 |
# MAGIC
# MAGIC ### ストリーミングテーブル vs マテリアライズドビュー
# MAGIC
# MAGIC | 特徴 | ストリーミングテーブル | マテリアライズドビュー |
# MAGIC |---|---|---|
# MAGIC | 処理方式 | 増分処理（新規データのみ） | 全件再計算 |
# MAGIC | データ操作 | 追記のみ | 結果の完全な再構築 |
# MAGIC | 適したレイヤー | Bronze, Silver | Silver, Gold |
# MAGIC | 用途 | ログ取り込み、イベントストリーム | 集計、レポート、KPI |
# MAGIC | データソース | 追記のみのソース | 任意のソース |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DLT パイプラインのコードパターン
# MAGIC
# MAGIC 以下は DLT パイプライン内で実行するコードパターンです。
# MAGIC
# MAGIC > **重要**: 以下のコードは DLT パイプラインのノートブックに記述するパターンの解説です。
# MAGIC > 通常のクラスター上で実行すると `dlt` モジュールが見つからずエラーになります。
# MAGIC > コードパターンを理解することが目的です。
# MAGIC
# MAGIC ### Python API のパターン
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC # --- Bronze: ストリーミングテーブル（生データ取り込み） ---
# MAGIC @dlt.table(
# MAGIC     comment="注文の生データ（Bronze）"
# MAGIC )
# MAGIC def orders_bronze():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .option("cloudFiles.inferColumnTypes", "true")
# MAGIC         .load("/data/orders/")
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC # --- Silver: クレンジング済みテーブル ---
# MAGIC @dlt.table(
# MAGIC     comment="クレンジング済み注文データ（Silver）"
# MAGIC )
# MAGIC @dlt.expect("valid_quantity", "quantity > 0")
# MAGIC @dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
# MAGIC def orders_silver():
# MAGIC     return (
# MAGIC         dlt.read_stream("orders_bronze")
# MAGIC         .withColumn("order_date", F.to_date("order_timestamp"))
# MAGIC         .withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
# MAGIC         .dropDuplicates(["order_id"])
# MAGIC     )
# MAGIC
# MAGIC
# MAGIC # --- Gold: 集計テーブル（マテリアライズドビュー） ---
# MAGIC @dlt.table(
# MAGIC     comment="日次売上サマリ（Gold）"
# MAGIC )
# MAGIC def daily_sales_gold():
# MAGIC     return (
# MAGIC         dlt.read("orders_silver")
# MAGIC         .groupBy("order_date", "region")
# MAGIC         .agg(
# MAGIC             F.count("order_id").alias("order_count"),
# MAGIC             F.sum("total_amount").alias("total_revenue"),
# MAGIC         )
# MAGIC     )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL API のパターン
# MAGIC
# MAGIC SQL でも同様のパイプラインを定義できます。
# MAGIC
# MAGIC ```sql
# MAGIC -- Bronze: ストリーミングテーブル
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_bronze
# MAGIC AS SELECT * FROM cloud_files("/data/orders/", "json")
# MAGIC
# MAGIC -- Silver: クレンジング済みテーブル（品質制約付き）
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_silver (
# MAGIC   CONSTRAINT valid_quantity EXPECT (quantity > 0),
# MAGIC   CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC AS SELECT
# MAGIC   *,
# MAGIC   to_date(order_timestamp) AS order_date,
# MAGIC   quantity * unit_price AS total_amount
# MAGIC FROM STREAM(LIVE.orders_bronze)
# MAGIC
# MAGIC -- Gold: マテリアライズドビュー（集計）
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW daily_sales_gold
# MAGIC AS SELECT
# MAGIC   order_date,
# MAGIC   region,
# MAGIC   COUNT(order_id) AS order_count,
# MAGIC   SUM(total_amount) AS total_revenue
# MAGIC FROM LIVE.orders_silver
# MAGIC GROUP BY order_date, region
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: DLT パイプラインでは Python（`@dlt.table`）と SQL（`CREATE OR REFRESH`）の
# MAGIC > 両方でテーブルを定義できます。
# MAGIC > - `dlt.read_stream()` / `STREAM(LIVE.table)` → ストリーミング読み取り（増分処理）
# MAGIC > - `dlt.read()` / `LIVE.table` → バッチ読み取り（全件処理）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. データ品質制約（Expectations）
# MAGIC
# MAGIC Expectations は DLT の強力な機能で、データ品質ルールを宣言的に定義できます。
# MAGIC
# MAGIC ### Expectations の種類
# MAGIC
# MAGIC | デコレータ / 制約 | 動作 | 品質違反時 |
# MAGIC |---|---|---|
# MAGIC | `@dlt.expect` | 警告を記録（データは保持） | メトリクスに記録、データは通過 |
# MAGIC | `@dlt.expect_or_drop` | 違反行を削除 | 違反行を除外、メトリクスに記録 |
# MAGIC | `@dlt.expect_or_fail` | パイプラインを停止 | エラーで処理を中断 |
# MAGIC | `@dlt.expect_all` | 複数条件を一括指定 | 条件ごとにメトリクス記録 |
# MAGIC
# MAGIC ### SQL の Expectations 構文
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE my_table (
# MAGIC   -- 警告のみ（デフォルト）
# MAGIC   CONSTRAINT positive_amount EXPECT (amount > 0),
# MAGIC
# MAGIC   -- 違反行を削除
# MAGIC   CONSTRAINT valid_email EXPECT (email IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC
# MAGIC   -- パイプラインを停止
# MAGIC   CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC AS SELECT ...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expectations のコードパターン（Python）
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC
# MAGIC # 単一の制約
# MAGIC @dlt.table
# MAGIC @dlt.expect("valid_quantity", "quantity > 0")
# MAGIC @dlt.expect_or_drop("non_null_customer", "customer_id IS NOT NULL")
# MAGIC @dlt.expect_or_fail("valid_price", "unit_price > 0")
# MAGIC def validated_orders():
# MAGIC     return dlt.read_stream("raw_orders")
# MAGIC
# MAGIC
# MAGIC # 複数の制約を一括指定
# MAGIC quality_rules = {
# MAGIC     "valid_quantity": "quantity > 0",
# MAGIC     "valid_price": "unit_price > 0",
# MAGIC     "valid_customer": "customer_id IS NOT NULL",
# MAGIC }
# MAGIC
# MAGIC @dlt.table
# MAGIC @dlt.expect_all(quality_rules)
# MAGIC def validated_orders_v2():
# MAGIC     return dlt.read_stream("raw_orders")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: Expectations は `expect`（警告）、`expect_or_drop`（行削除）、
# MAGIC > `expect_or_fail`（パイプライン停止）の3種類があります。
# MAGIC > `expect_all` / `expect_all_or_drop` / `expect_all_or_fail` で複数条件を一括指定できます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Expectations のシミュレーション
# MAGIC
# MAGIC DLT パイプライン外でも、Expectations の考え方を実践できます。
# MAGIC 以下は通常の Spark で品質チェックを行うシミュレーションです。

# COMMAND ----------

from pyspark.sql import functions as F

# Bronze データを読み込み
df_bronze = spark.table("default.orders_bronze")

# Expectation のシミュレーション
expectations = {
    "valid_quantity": "quantity > 0",
    "valid_customer": "customer_id IS NOT NULL",
    "valid_price": "unit_price > 0",
    "valid_region": "region IS NOT NULL",
}

print("=== データ品質チェック結果 ===")
total_count = df_bronze.count()
for name, condition in expectations.items():
    pass_count = df_bronze.filter(condition).count()
    fail_count = total_count - pass_count
    pass_rate = pass_count / total_count * 100
    status = "PASS" if fail_count == 0 else "WARN"
    print(f"  [{status}] {name}: {pass_rate:.1f}% 合格 ({fail_count} 件が違反)")

# COMMAND ----------

# expect_or_drop のシミュレーション: 違反行を除外
df_validated = df_bronze
for name, condition in expectations.items():
    df_validated = df_validated.filter(condition)

print(f"\n元データ: {total_count} 件")
print(f"品質チェック後: {df_validated.count()} 件")
print(f"除外された行: {total_count - df_validated.count()} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. パイプラインの設定と管理
# MAGIC
# MAGIC ### パイプラインの作成手順（UI）
# MAGIC
# MAGIC 1. 左サイドバーの **「Lakeflow Jobs」**（旧「ワークフロー」）→ **「パイプライン」** を選択
# MAGIC 2. **「パイプラインを作成」** をクリック
# MAGIC 3. 設定項目を入力:
# MAGIC
# MAGIC | 設定項目 | 説明 |
# MAGIC |---|---|
# MAGIC | パイプライン名 | パイプラインの識別名 |
# MAGIC | ソースコード | DLT コードを含むノートブックのパス |
# MAGIC | ターゲットスキーマ | 出力先の Unity Catalog スキーマ |
# MAGIC | クラスターポリシー | 使用するクラスター設定 |
# MAGIC | 実行モード | Triggered（手動/スケジュール） or Continuous（常時稼働） |
# MAGIC
# MAGIC ### 実行モード
# MAGIC
# MAGIC | モード | 説明 | 用途 |
# MAGIC |---|---|---|
# MAGIC | **Triggered** | 手動またはスケジュールで実行 | バッチ ETL、定期処理 |
# MAGIC | **Continuous** | 常時稼働でリアルタイム処理 | リアルタイムダッシュボード |
# MAGIC
# MAGIC ### 更新の種類
# MAGIC
# MAGIC | 更新タイプ | 説明 |
# MAGIC |---|---|
# MAGIC | **Full Refresh** | 全データを再計算（テーブルを再構築） |
# MAGIC | **Incremental Update** | 新規データのみ処理（デフォルト） |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. DLT パイプラインの監視とトラブルシューティング
# MAGIC
# MAGIC ### イベントログ
# MAGIC
# MAGIC DLT は実行に関する詳細なイベントログを自動的に記録します。
# MAGIC
# MAGIC | イベントタイプ | 説明 |
# MAGIC |---|---|
# MAGIC | `flow_progress` | データフローの進捗状況 |
# MAGIC | `dataset_definition` | テーブル定義情報 |
# MAGIC | `expectation` | 品質チェックの結果 |
# MAGIC | `maintenance` | メンテナンス操作（OPTIMIZE 等） |
# MAGIC
# MAGIC ### イベントログのクエリ例
# MAGIC
# MAGIC ```sql
# MAGIC -- Expectations の結果を確認
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   details:flow_progress:data_quality.expectations
# MAGIC FROM event_log(TABLE(my_pipeline.my_table))
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC ORDER BY timestamp DESC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Lakeflow Declarative Pipelines の利点
# MAGIC
# MAGIC | 利点 | 説明 |
# MAGIC |---|---|
# MAGIC | **宣言的定義** | 「何を」だけ記述すれば「どのように」は自動管理 |
# MAGIC | **自動エラー処理** | リトライやチェックポイントを自動管理 |
# MAGIC | **データ品質** | Expectations で品質ルールを宣言的に定義 |
# MAGIC | **依存関係管理** | テーブル間の依存関係を自動的に解決 |
# MAGIC | **増分処理** | 変更分のみを効率的に処理 |
# MAGIC | **自動OPTIMIZE** | テーブルの物理配置を自動最適化 |
# MAGIC | **監視** | イベントログとデータ品質メトリクスを自動記録 |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **Declarative Pipelines の概念** — 宣言型 ETL、ストリーミングテーブル vs マテリアライズドビュー
# MAGIC 2. **コードパターン** — Python（`@dlt.table`）と SQL（`CREATE OR REFRESH`）
# MAGIC 3. **Expectations** — `expect` / `expect_or_drop` / `expect_or_fail` によるデータ品質管理
# MAGIC 4. **パイプライン設定** — 実行モード（Triggered / Continuous）、更新タイプ
# MAGIC 5. **監視** — イベントログによるパイプラインの監視
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - **次のノートブック `07_workflows_and_jobs.py`** で Lakeflow Jobs とジョブ管理を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Engineer Associate):
# MAGIC > - **Data Processing & Transformations (31%)**: Lakeflow Declarative Pipelines の利点、ストリーミングテーブルとマテリアライズドビュー、Expectations によるデータ品質管理

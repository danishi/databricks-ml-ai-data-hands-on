# Databricks notebook source
# MAGIC %md
# MAGIC # メダリオンアーキテクチャと ETL パイプライン
# MAGIC
# MAGIC このノートブックでは、**メダリオンアーキテクチャ（Medallion Architecture）** に基づいた
# MAGIC ETL パイプラインの構築を学びます。Spark SQL と PySpark の両方を使って実践します。
# MAGIC
# MAGIC **メダリオンアーキテクチャとは？**
# MAGIC > データを **Bronze（生データ）→ Silver（クレンジング済み）→ Gold（集計・分析用）** の
# MAGIC > 3層に分けて管理する設計パターンです。データの品質を段階的に高めていきます。
# MAGIC
# MAGIC ### メダリオンアーキテクチャの全体像
# MAGIC
# MAGIC ```
# MAGIC ┌──────────┐     ┌──────────┐     ┌──────────┐
# MAGIC │  Bronze   │     │  Silver   │     │   Gold    │
# MAGIC │ (生データ)  │ ──→ │(クレンジング)│ ──→ │ (集計済み)  │
# MAGIC │          │     │          │     │          │
# MAGIC │• そのまま保存│     │• 重複排除   │     │• ビジネスKPI│
# MAGIC │• 型変換なし │     │• 型変換    │     │• ダッシュボード│
# MAGIC │• 追記のみ  │     │• バリデーション│     │• レポート   │
# MAGIC └──────────┘     └──────────┘     └──────────┘
# MAGIC      ↑                                    ↓
# MAGIC  外部ソース                            BI / ML
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - メダリオンアーキテクチャの各レイヤーの役割
# MAGIC - Bronze → Silver → Gold の ETL パイプライン構築
# MAGIC - Spark SQL と PySpark による複雑なデータ変換
# MAGIC - クラスタータイプの最適な選択
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime（例: 16.x）のクラスター
# MAGIC - `02_data_ingestion.py` の実行は不要です（このノートブックでデータを作成します）
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. サンプルデータの準備
# MAGIC
# MAGIC EC サイトの注文データを使って、メダリオンアーキテクチャを実践します。
# MAGIC 「生データに品質問題がある」状態を再現して、各レイヤーでどのように改善していくか学びます。

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, TimestampType
)

random.seed(42)

# 品質問題を含む生データを作成
raw_orders = []
products = {
    "P001": ("ノートPC", "PC", 150000),
    "P002": ("マウス", "周辺機器", 3500),
    "P003": ("キーボード", "周辺機器", 12000),
    "P004": ("モニター", "ディスプレイ", 45000),
    "P005": ("USBメモリ", "ストレージ", 2500),
}

for i in range(1, 501):
    pid = random.choice(list(products.keys()))
    pname, category, base_price = products[pid]
    qty = random.randint(1, 10)
    price = base_price * (1 + random.uniform(-0.1, 0.1))
    ts = datetime(2024, 1, 1) + timedelta(hours=random.randint(0, 4380))

    record = {
        "order_id": i,
        "customer_id": random.randint(1000, 1099),
        "product_id": pid,
        "product_name": pname,
        "category": category,
        "quantity": qty,
        "unit_price": round(price, 2),
        "order_timestamp": ts.isoformat(),
        "region": random.choice(["東京", "大阪", "名古屋", "福岡", "札幌"]),
    }

    # 品質問題を意図的に追加
    if random.random() < 0.03:  # 3% の欠損値
        record["customer_id"] = None
    if random.random() < 0.02:  # 2% の負の数量
        record["quantity"] = -1
    if random.random() < 0.05:  # 5% の重複レコード
        raw_orders.append(record)  # 同じレコードを2回追加

    raw_orders.append(record)

# Spark DataFrame に変換
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("region", StringType(), True),
])

df_raw = spark.createDataFrame(raw_orders, schema=schema)
print(f"生データの件数: {df_raw.count()}")
print(f"（重複や品質問題を含む）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze レイヤー: 生データの保存
# MAGIC
# MAGIC Bronze レイヤーでは、**ソースデータをそのまま保存**します。
# MAGIC
# MAGIC ### Bronze の原則
# MAGIC - データの変換は行わない
# MAGIC - メタデータ（取り込み時刻、ソースファイル名など）を追加
# MAGIC - 追記のみ（Append-only）
# MAGIC - 生データの「真実の記録」として機能
# MAGIC
# MAGIC > **なぜ生データを保存するのか？**
# MAGIC > 後から処理ロジックを変更した場合に、生データから再処理できるようにするためです。
# MAGIC > Bronze は「やり直し可能」にするための保険のような役割です。

# COMMAND ----------

# Bronze テーブルの作成（メタデータを追加）
df_bronze = (
    df_raw
    .withColumn("_ingested_at", F.current_timestamp())  # 取り込み日時
    .withColumn("_source", F.lit("ec_site_orders"))      # データソース名
)

# Bronze テーブルとして保存
df_bronze.write.mode("overwrite").saveAsTable("default.orders_bronze")

print(f"Bronze テーブルに {df_bronze.count()} 件を保存しました")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze データの確認（品質問題がそのまま残っている）
# MAGIC SELECT * FROM default.orders_bronze LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 品質問題の確認
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_rows,
# MAGIC   SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
# MAGIC   SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) AS negative_quantities
# MAGIC FROM default.orders_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver レイヤー: データのクレンジング
# MAGIC
# MAGIC Silver レイヤーでは、Bronze のデータを**クレンジング・変換**します。
# MAGIC
# MAGIC ### Silver で行う典型的な処理
# MAGIC
# MAGIC | 処理 | 説明 |
# MAGIC |---|---|
# MAGIC | 重複排除 | 同じレコードを1つにまとめる |
# MAGIC | 型変換 | 文字列を適切な型（日付、数値など）に変換 |
# MAGIC | バリデーション | 不正なデータ（負の数量など）を除外 |
# MAGIC | NULL処理 | 欠損値の除外または補完 |
# MAGIC | 正規化 | データの一貫性を確保 |

# COMMAND ----------

# Silver テーブルの作成: PySpark による処理
from pyspark.sql.window import Window

# Step 1: 重複排除（order_id で一意に）
window_spec = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
df_dedup = (
    spark.table("default.orders_bronze")
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

# Step 2: 型変換とバリデーション
df_silver = (
    df_dedup
    # 型変換
    .withColumn("order_timestamp", F.to_timestamp("order_timestamp"))
    .withColumn("order_date", F.to_date("order_timestamp"))
    .withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
    # バリデーション: NULL の customer_id と負の数量を除外
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("quantity") > 0)
    # 不要な列を削除
    .drop("_source")
    # クレンジング日時を追加
    .withColumn("_cleaned_at", F.current_timestamp())
)

# Silver テーブルとして保存
df_silver.write.mode("overwrite").saveAsTable("default.orders_silver")

print(f"Silver テーブルに {df_silver.count()} 件を保存しました")
print(f"（Bronze: {df_bronze.count()} 件 → Silver: {df_silver.count()} 件、品質問題を解消）")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver データの確認（クレンジング済み）
# MAGIC SELECT * FROM default.orders_silver LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 品質問題が解消されたことを確認
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT order_id) AS unique_orders,
# MAGIC   SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
# MAGIC   SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) AS negative_quantities
# MAGIC FROM default.orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold レイヤー: ビジネスレベルの集計
# MAGIC
# MAGIC Gold レイヤーでは、Silver のデータを**ビジネス要件に合わせて集計・変換**します。
# MAGIC ダッシュボードや ML モデルの入力として使われます。
# MAGIC
# MAGIC ### Gold テーブルの例
# MAGIC
# MAGIC | テーブル | 内容 | 用途 |
# MAGIC |---|---|---|
# MAGIC | 日次売上サマリ | 日別×地域別の売上集計 | 経営ダッシュボード |
# MAGIC | 商品別分析 | 商品ごとの売上・数量分析 | 在庫管理 |
# MAGIC | 顧客分析 | 顧客ごとの購買傾向 | マーケティング |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold テーブル 1: 日次×地域別の売上サマリ
# MAGIC CREATE OR REPLACE TABLE default.daily_sales_gold AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   region,
# MAGIC   COUNT(DISTINCT order_id) AS order_count,
# MAGIC   SUM(quantity) AS total_quantity,
# MAGIC   ROUND(SUM(total_amount), 2) AS total_revenue,
# MAGIC   ROUND(AVG(total_amount), 2) AS avg_order_value
# MAGIC FROM default.orders_silver
# MAGIC GROUP BY order_date, region
# MAGIC ORDER BY order_date, region

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 日次売上サマリの確認
# MAGIC SELECT * FROM default.daily_sales_gold LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold テーブル 2: 商品別分析
# MAGIC CREATE OR REPLACE TABLE default.product_analysis_gold AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   COUNT(DISTINCT order_id) AS total_orders,
# MAGIC   SUM(quantity) AS total_quantity_sold,
# MAGIC   ROUND(SUM(total_amount), 2) AS total_revenue,
# MAGIC   ROUND(AVG(unit_price), 2) AS avg_unit_price,
# MAGIC   MIN(order_date) AS first_sale_date,
# MAGIC   MAX(order_date) AS last_sale_date
# MAGIC FROM default.orders_silver
# MAGIC GROUP BY product_id, product_name, category
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 商品別分析の確認
# MAGIC SELECT * FROM default.product_analysis_gold

# COMMAND ----------

# Gold テーブル 3: PySpark で月次トレンドを作成
df_monthly = (
    spark.table("default.orders_silver")
    .withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
    .groupBy("year_month", "category")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("total_amount").alias("total_revenue"),
    )
    .orderBy("year_month", "category")
)

df_monthly.write.mode("overwrite").saveAsTable("default.monthly_trend_gold")
display(df_monthly)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 複雑な変換（高度な Spark SQL / PySpark）
# MAGIC
# MAGIC データエンジニアの試験では、以下のような複雑な変換が出題されます。

# COMMAND ----------

# MAGIC %md
# MAGIC ### ウィンドウ関数

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ウィンドウ関数: 地域別の累計売上と順位
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   region,
# MAGIC   total_revenue,
# MAGIC   SUM(total_revenue) OVER (PARTITION BY region ORDER BY order_date) AS cumulative_revenue,
# MAGIC   RANK() OVER (PARTITION BY region ORDER BY total_revenue DESC) AS revenue_rank
# MAGIC FROM default.daily_sales_gold
# MAGIC ORDER BY region, order_date
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### 高階関数（Higher-Order Functions）

# COMMAND ----------

# 配列データの操作
df_array = spark.sql("""
    SELECT
      region,
      collect_list(total_revenue) AS daily_revenues
    FROM default.daily_sales_gold
    GROUP BY region
""")

# TRANSFORM: 配列の各要素を変換
df_transformed = df_array.select(
    "region",
    F.expr("TRANSFORM(daily_revenues, x -> ROUND(x / 1000, 1))").alias("revenue_in_thousands"),
    F.expr("FILTER(daily_revenues, x -> x > 100000)").alias("high_revenue_days"),
    F.expr("AGGREGATE(daily_revenues, DOUBLE(0), (acc, x) -> acc + x)").alias("total_revenue"),
)

display(df_transformed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ピボットテーブル

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ピボット: 地域×カテゴリの売上クロス集計
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT region, category, total_amount
# MAGIC   FROM default.orders_silver
# MAGIC )
# MAGIC PIVOT (
# MAGIC   ROUND(SUM(total_amount), 0) AS revenue
# MAGIC   FOR category IN ('PC', '周辺機器', 'ディスプレイ', 'ストレージ')
# MAGIC )
# MAGIC ORDER BY region

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. クラスタータイプの最適な選択
# MAGIC
# MAGIC メダリオンアーキテクチャの各レイヤーに適したクラスター構成を理解しましょう。
# MAGIC
# MAGIC | レイヤー | 処理の特徴 | 推奨クラスター |
# MAGIC |---|---|---|
# MAGIC | Bronze（取り込み） | I/O 中心、大量データ | ストレージ最適化インスタンス |
# MAGIC | Silver（変換） | CPU 中心、複雑な変換 | 汎用またはコンピュート最適化 |
# MAGIC | Gold（集計） | メモリ中心、集約処理 | メモリ最適化 |
# MAGIC | BI クエリ | 低レイテンシ、SQL | SQL Warehouse |
# MAGIC
# MAGIC > **試験ポイント**: クラスターのノードタイプは処理の特性に合わせて選択します。
# MAGIC > - I/O 集約的な処理 → ストレージ最適化
# MAGIC > - CPU 集約的な変換 → コンピュート最適化
# MAGIC > - 大量のデータをメモリに保持 → メモリ最適化

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **メダリオンアーキテクチャ** — Bronze / Silver / Gold の3層構成
# MAGIC 2. **Bronze レイヤー** — 生データの保存（メタデータ追加、変換なし）
# MAGIC 3. **Silver レイヤー** — クレンジング（重複排除、型変換、バリデーション）
# MAGIC 4. **Gold レイヤー** — ビジネスレベルの集計（日次サマリ、商品分析、月次トレンド）
# MAGIC 5. **複雑な変換** — ウィンドウ関数、高階関数、ピボット
# MAGIC 6. **クラスター選択** — 処理特性に合わせたインスタンスタイプの選択
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - **次のノートブック `06_lakeflow_declarative_pipelines.py`** で Lakeflow Declarative Pipelines を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Engineer Associate):
# MAGIC > - **Data Processing & Transformations (31%)**: メダリオンアーキテクチャ、Spark SQL/PySpark による ETL、ウィンドウ関数、クラスタータイプの選択

# Databricks notebook source
# MAGIC %md
# MAGIC # クリーンアップ: ハンズオンで作成したリソースの削除
# MAGIC
# MAGIC このノートブックでは、ハンズオンで作成した以下のリソースを削除します:
# MAGIC
# MAGIC 1. **テーブル** — 分析用に作成したサンプルテーブル
# MAGIC 2. **ビュー** — 分析用に作成したビュー
# MAGIC
# MAGIC > **重要**: このノートブックを実行すると、ハンズオンで作成したすべてのテーブルとビューが
# MAGIC > 削除されます。再度ハンズオンを行う場合は、各ノートブックを最初から実行してください。
# MAGIC
# MAGIC ## 前提条件
# MAGIC - ハンズオンのノートブックを実行済みであること
# MAGIC - Databricks Runtime のクラスターを使用してください
# MAGIC
# MAGIC ---

# COMMAND ----------

# 初期設定
catalog = "main"
schema = "default"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"使用するカタログ: {catalog}")
print(f"使用するスキーマ: {schema}")
print("削除対象のリソースを確認します...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ビューの削除
# MAGIC
# MAGIC ビューはテーブルを参照しているため、先にビューを削除します。

# COMMAND ----------

views_to_drop = [
    "da_electronics_view",
    "da_orders_clean",
    "da_customers_secure",
    "da_orders_regional",
]

print("=== ビューの削除 ===")
for view_name in views_to_drop:
    try:
        spark.sql(f"DROP VIEW IF EXISTS {view_name}")
        print(f"  削除: {view_name}")
    except Exception as e:
        print(f"  スキップ: {view_name}（理由: {e}）")

print("\nビューの削除が完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. テーブルの削除

# COMMAND ----------

tables_to_drop = [
    # 01_databricks_platform.py
    "da_sample_products",
    # 02_managing_data.py
    "da_customers",
    "da_orders",
    "da_customer_order_summary",
    # 03_importing_data.py
    "da_daily_sales",
    "da_stores",
    # 04_sql_queries.py
    "da_monthly_sales",
    # 05_query_analysis.py
    "da_orders_clustered",
    # 08_data_modeling.py
    "da_dim_date",
    "da_dim_product",
    "da_dim_customer",
    "da_fact_orders",
    "da_bronze_orders",
    "da_silver_orders",
    "da_gold_daily_revenue",
]

print("=== テーブルの削除 ===")
for table_name in tables_to_drop:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"  削除: {table_name}")
    except Exception as e:
        print(f"  スキップ: {table_name}（理由: {e}）")

print("\nテーブルの削除が完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. クラスターの停止
# MAGIC
# MAGIC ハンズオンで使用したクラスターも忘れずに停止しましょう。
# MAGIC
# MAGIC 1. 左サイドバーの **「コンピューティング」** を選択
# MAGIC 2. 使用したクラスターの行で **「終了」** をクリック
# MAGIC
# MAGIC > **自動終了**: クラスター作成時に「非アクティブ後に自動終了」を設定していれば、
# MAGIC > 一定時間後に自動的に停止されます（デフォルト: 120分）。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## クリーンアップ完了
# MAGIC
# MAGIC 以下のリソースが削除されました:
# MAGIC
# MAGIC | リソース種類 | 削除数 |
# MAGIC |---|---|
# MAGIC | ビュー | 4 個 |
# MAGIC | テーブル | 15 個 |
# MAGIC | クラスター | 手動停止 |
# MAGIC
# MAGIC お疲れさまでした！

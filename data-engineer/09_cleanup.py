# Databricks notebook source
# MAGIC %md
# MAGIC # クリーンアップ: ハンズオンで作成したリソースの削除
# MAGIC
# MAGIC このノートブックでは、ハンズオンで作成した以下のリソースを削除します:
# MAGIC
# MAGIC 1. **Delta テーブル** — 各ノートブックで作成したテーブル
# MAGIC 2. **スキーマ** — ハンズオン用に作成したスキーマ
# MAGIC 3. **Unity Catalog ボリューム** — サンプルデータ用ボリューム
# MAGIC
# MAGIC > **重要**: ハンズオン終了後はこのノートブックを実行して不要なリソースを削除してください。
# MAGIC > テーブルやスキーマが残ったままだと、ストレージコストが発生する場合があります。
# MAGIC
# MAGIC ## 前提条件
# MAGIC - ハンズオンのノートブックを実行済みであること
# MAGIC - Databricks Runtime のクラスターを使用してください
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 設定値の確認

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "カタログ名")
dbutils.widgets.text("schema", "default", "スキーマ名")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# 削除対象のテーブル一覧
tables_to_drop = [
    # 02_data_ingestion.py で作成
    f"{catalog}.{schema}.orders_copy_into",
    f"{catalog}.{schema}.orders_autoloader",
    # 03_sql_dml_operations.py で作成
    f"{catalog}.{schema}.products",
    f"{catalog}.{schema}.product_summary",
    # 04_delta_lake.py で作成
    f"{catalog}.{schema}.sales_delta",
    f"{catalog}.{schema}.sales_clustered",
    # 05_medallion_architecture.py で作成
    f"{catalog}.{schema}.orders_bronze",
    f"{catalog}.{schema}.orders_silver",
    f"{catalog}.{schema}.daily_sales_gold",
    f"{catalog}.{schema}.product_analysis_gold",
    f"{catalog}.{schema}.monthly_trend_gold",
]

# ハンズオン用スキーマ（08_unity_catalog_governance.py で作成）
handson_schema = f"{catalog}.data_engineer_handson"
handson_tables = [
    f"{handson_schema}.managed_example",
]

# ハンズオン用ボリューム（02_data_ingestion.py で作成）
handson_volume = f"{catalog}.{schema}.data_engineer_handson"

print("=== 削除対象 ===")
print(f"\nカタログ: {catalog}")
print(f"スキーマ: {schema}")
print(f"\nテーブル ({len(tables_to_drop) + len(handson_tables)} 件):")
for t in tables_to_drop + handson_tables:
    print(f"  - {t}")
print(f"\nスキーマ:")
print(f"  - {handson_schema}")
print(f"\nボリューム:")
print(f"  - {handson_volume}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. テーブルの削除

# COMMAND ----------

print("=== テーブルの削除 ===")
all_tables = tables_to_drop + handson_tables

for table_name in all_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"  ✓ {table_name} を削除しました")
    except Exception as e:
        print(f"  - {table_name} の削除をスキップしました（理由: {e}）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. スキーマの削除

# COMMAND ----------

print("=== スキーマの削除 ===")
try:
    spark.sql(f"DROP SCHEMA IF EXISTS {handson_schema} CASCADE")
    print(f"  ✓ {handson_schema} を削除しました")
except Exception as e:
    print(f"  - {handson_schema} の削除をスキップしました（理由: {e}）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ボリュームの削除

# COMMAND ----------

print("=== ボリュームの削除 ===")
try:
    spark.sql(f"DROP VOLUME IF EXISTS {handson_volume}")
    print(f"  ✓ {handson_volume} を削除しました")
except Exception as e:
    print(f"  - {handson_volume} の削除をスキップしました（理由: {e}）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Auto Loader チェックポイントの確認
# MAGIC
# MAGIC Auto Loader のチェックポイントはボリュームの削除に含まれています。

# COMMAND ----------

print("=== チェックポイントの確認 ===")
print("Auto Loader のチェックポイントはボリュームの削除に含まれています。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. クラスターの停止
# MAGIC
# MAGIC ハンズオンで使用したクラスターも忘れずに停止しましょう。
# MAGIC
# MAGIC 1. 左サイドバーの **「コンピューティング」** を選択
# MAGIC 2. 使用したクラスターの行で **「終了」** をクリック
# MAGIC
# MAGIC > **自動終了**: クラスター作成時に「非アクティブ後に自動終了」を設定していれば、
# MAGIC > 一定時間後に自動的に停止されます（デフォルト: 120分）。

# COMMAND ----------

# ウィジェットの削除
dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## クリーンアップ完了
# MAGIC
# MAGIC 以下のリソースが削除されました:
# MAGIC
# MAGIC | リソース | 状態 |
# MAGIC |---|---|
# MAGIC | Delta テーブル（default スキーマ） | 削除済み |
# MAGIC | Delta テーブル（handson スキーマ） | 削除済み |
# MAGIC | ハンズオン用スキーマ | 削除済み |
# MAGIC | Unity Catalog ボリューム | 削除済み |
# MAGIC | クラスター | 手動停止 |
# MAGIC
# MAGIC お疲れさまでした！

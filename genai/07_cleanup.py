# Databricks notebook source
# MAGIC %md
# MAGIC # クリーンアップ: 生成AI ハンズオンのリソース削除
# MAGIC
# MAGIC このノートブックでは、生成AIハンズオンで作成したリソースを削除します。
# MAGIC
# MAGIC ## 削除対象
# MAGIC
# MAGIC | リソース | 作成元 | 削除方法 |
# MAGIC |---|---|---|
# MAGIC | Vector Search Index | `03_vector_search_rag.py` | このノートブックで自動削除 |
# MAGIC | Vector Search Endpoint | `03_vector_search_rag.py` | このノートブックで自動削除 |
# MAGIC | RAGドキュメントテーブル | `03_vector_search_rag.py` | このノートブックで自動削除 |
# MAGIC | Databricks App（RAGチャット） | `app_rag/` をデプロイ | 手動削除（UIから） |
# MAGIC
# MAGIC > **補足**: `01_foundation_model_apis.py` / `02_rag_chat.py` / `04_agents_tool_use.py` / `05_evaluation_governance.py` では
# MAGIC > Foundation Model APIs（pay-per-token）を使用しているため、エンドポイントやモデルの削除は不要です。
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %pip install -q databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "カタログ名")
dbutils.widgets.text("schema", "default", "スキーマ名")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

source_table = f"{catalog}.{schema}.rag_documents"
index_name = f"{catalog}.{schema}.rag_documents_index"
vs_endpoint_name = "rag_hands_on_endpoint"

print(f"削除対象テーブル: {source_table}")
print(f"削除対象インデックス: {index_name}")
print(f"削除対象エンドポイント: {vs_endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Vector Search Index の削除

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

try:
    w.vector_search_indexes.delete_index(index_name)
    print(f"Vector Search Index '{index_name}' を削除しました")
except Exception as e:
    print(f"インデックスの削除をスキップしました（理由: {e}）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Vector Search Endpoint の削除

# COMMAND ----------

try:
    w.vector_search_endpoints.delete_endpoint(vs_endpoint_name)
    print(f"Vector Search Endpoint '{vs_endpoint_name}' を削除しました")
except Exception as e:
    print(f"エンドポイントの削除をスキップしました（理由: {e}）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delta Table の削除

# COMMAND ----------

try:
    spark.sql(f"DROP TABLE IF EXISTS {source_table}")
    print(f"Delta Table '{source_table}' を削除しました")
except Exception as e:
    print(f"テーブルの削除をスキップしました（理由: {e}）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Databricks App の削除（手動）
# MAGIC
# MAGIC RAGチャットアプリをDatabricks Appsとしてデプロイした場合は、以下の手順で削除してください。
# MAGIC
# MAGIC 1. 左サイドバーの **「コンピューティング」** → **「アプリ」** を選択
# MAGIC 2. 削除したいアプリ（例: `rag-chat-app`）を選択
# MAGIC 3. 右上の **「...」メニュー** → **「削除」** をクリック

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. クラスターの停止
# MAGIC
# MAGIC 1. 左サイドバーの **「コンピューティング」** を選択
# MAGIC 2. 使用したクラスターの行で **「終了」** をクリック

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## クリーンアップ完了
# MAGIC
# MAGIC | リソース | 状態 |
# MAGIC |---|---|
# MAGIC | Vector Search Index | 削除済み |
# MAGIC | Vector Search Endpoint | 削除済み |
# MAGIC | RAGドキュメントテーブル | 削除済み |
# MAGIC | Foundation Model APIs | 追加の削除不要（pay-per-token） |
# MAGIC | Databricks App（RAGチャット） | 手動削除（UIから） |
# MAGIC | クラスター | 手動停止 |
# MAGIC
# MAGIC > **MLハンズオンのリソース**を削除する場合は `ml/10_cleanup.py` を実行してください。
# MAGIC
# MAGIC お疲れさまでした！

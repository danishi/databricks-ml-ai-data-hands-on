# Databricks notebook source
# MAGIC %md
# MAGIC # クリーンアップ: ハンズオンで作成したリソースの削除
# MAGIC
# MAGIC このノートブックでは、ハンズオンで作成した以下のリソースを削除します:
# MAGIC
# MAGIC 1. **Model Serving エンドポイント** — API公開用のサーバー（課金対象）
# MAGIC 2. **登録済みモデル** — Unity Catalog に登録したモデル
# MAGIC 3. **Databricks App** — Streamlitアプリ（手動で削除）
# MAGIC
# MAGIC > **重要**: 特に Model Serving エンドポイントは、放置すると課金が続く可能性があります。
# MAGIC > ハンズオン終了後は必ずこのノートブックを実行してクリーンアップしてください。
# MAGIC
# MAGIC ## 前提条件
# MAGIC - `08_model_serving.py` を実行済みであること
# MAGIC - Databricks Runtime ML のクラスターを使用してください
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %pip install -q databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 設定値の確認
# MAGIC
# MAGIC `08_model_serving.py` で使用したのと同じ値を設定します。

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "カタログ名")
dbutils.widgets.text("schema", "default", "スキーマ名")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

model_names = [
    f"{catalog}.{schema}.wine_classifier",
    f"{catalog}.{schema}.wine_best_model",
]
feature_table_name = f"{catalog}.{schema}.wine_features"
endpoint_name = "wine-classifier-endpoint"

print("削除対象モデル:")
for m in model_names:
    print(f"  - {m}")
print(f"削除対象特徴量テーブル: {feature_table_name}")
print(f"削除対象エンドポイント: {endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Model Serving エンドポイントの削除
# MAGIC
# MAGIC エンドポイントを削除すると、APIによる予測リクエストが受けられなくなります。
# MAGIC **課金も停止されます。**

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

try:
    w.serving_endpoints.delete(endpoint_name)
    print(f"エンドポイント '{endpoint_name}' を削除しました")
except Exception as e:
    print(f"エンドポイントの削除をスキップしました（理由: {e}）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 登録済みモデルの削除
# MAGIC
# MAGIC Unity Catalog に登録したモデル（全バージョン）を削除します。

# COMMAND ----------

from mlflow import MlflowClient

mlflow_client = MlflowClient()

for model_name in model_names:
    try:
        versions = mlflow_client.search_model_versions(f"name='{model_name}'")
        for v in versions:
            mlflow_client.delete_model_version(model_name, v.version)
            print(f"  モデルバージョン {v.version} を削除しました")
        mlflow_client.delete_registered_model(model_name)
        print(f"登録済みモデル '{model_name}' を削除しました")
    except Exception as e:
        print(f"モデル '{model_name}' の削除をスキップしました（理由: {e}）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 特徴量テーブルの削除

# COMMAND ----------

try:
    spark.sql(f"DROP TABLE IF EXISTS {feature_table_name}")
    print(f"特徴量テーブル '{feature_table_name}' を削除しました")
except Exception as e:
    print(f"特徴量テーブルの削除をスキップしました（理由: {e}）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. MLflow の実験ランの確認
# MAGIC
# MAGIC MLflow の実験ラン（学習の記録）はワークスペースに残りますが、
# MAGIC ストレージコストはほとんどかからないため、削除は任意です。
# MAGIC
# MAGIC 削除したい場合は、左サイドバーの **「エクスペリメント」** から手動で削除できます。

# COMMAND ----------

print("=== MLflow の実験ラン ===")
print("左サイドバーの「エクスペリメント」から確認・削除できます。")
print("（自動削除は行いません）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Databricks App の削除（手動）
# MAGIC
# MAGIC Streamlit アプリ（Databricks App）をデプロイした場合は、以下の手順で手動削除してください:
# MAGIC
# MAGIC 1. 左サイドバーの **「コンピューティング」** → **「アプリ」** を選択
# MAGIC 2. 削除したいアプリ（例: wine-classifier-app）を選択
# MAGIC 3. 右上の **「...」メニュー** → **「削除」** をクリック

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

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## クリーンアップ完了
# MAGIC
# MAGIC 以下のリソースが削除されました:
# MAGIC
# MAGIC | リソース | 状態 |
# MAGIC |---|---|
# MAGIC | Model Serving エンドポイント | 削除済み |
# MAGIC | Unity Catalog 登録モデル | 削除済み |
# MAGIC | MLflow 実験ラン | 手動削除（任意） |
# MAGIC | Databricks App | 手動削除（UIから） |
# MAGIC | クラスター | 手動停止 |
# MAGIC
# MAGIC お疲れさまでした！

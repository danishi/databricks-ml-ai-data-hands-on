# Databricks notebook source
# MAGIC %md
# MAGIC # モデルサービング: MLモデルをAPIとして公開する
# MAGIC
# MAGIC このノートブックでは、学習済みの機械学習モデルを **Databricks Model Serving** を使って
# MAGIC **REST API** として公開する方法を学びます。
# MAGIC
# MAGIC **モデルサービングとは？**
# MAGIC > 学習済みモデルをWebサーバーのように動かして、外部のアプリケーションから
# MAGIC > HTTPリクエスト（API呼び出し）でリアルタイムに予測を取得できる仕組みです。
# MAGIC >
# MAGIC > 例: Webアプリやモバイルアプリから「このワインの品種は？」と問い合わせると、
# MAGIC > モデルが即座に予測結果を返してくれます。
# MAGIC
# MAGIC ## 全体の流れ
# MAGIC
# MAGIC ```
# MAGIC ① モデルを学習 → ② MLflow に登録 → ③ エンドポイント作成 → ④ API で予測
# MAGIC
# MAGIC                     Unity Catalog        Model Serving
# MAGIC                     ┌──────────┐       ┌──────────────┐
# MAGIC  学習済みモデル  →  │ v1, v2.. │  →   │ REST API     │  ←  Webアプリ
# MAGIC                     └──────────┘       │ リアルタイム予測│  ←  モバイルアプリ
# MAGIC                                        └──────────────┘  ←  他のシステム
# MAGIC ```
# MAGIC
# MAGIC > **初心者の方へ: なぜ Model Serving が必要？**
# MAGIC >
# MAGIC > ノートブック上でモデルを動かすだけでは、他のシステムから使えません。
# MAGIC > Model Serving を使うと、Webアプリやモバイルアプリから
# MAGIC > HTTP リクエスト（URLにアクセスするのと同じ要領）で予測を取得できるようになります。
# MAGIC
# MAGIC > **試験ポイント: 3つのデプロイ方式**
# MAGIC >
# MAGIC > | 方式 | 説明 | 例 |
# MAGIC > |---|---|---|
# MAGIC > | **バッチ推論** | まとめて大量に予測 | 毎晩の顧客スコアリング |
# MAGIC > | **リアルタイム推論** | 1件ずつ即座に予測 | Webアプリの推薦 |
# MAGIC > | **ストリーミング推論** | データが来たら随時予測 | IoTセンサーの異常検知 |
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）のクラスターを使用してください
# MAGIC - Unity Catalog が有効なワークスペースであること
# MAGIC - クラスターサイズはシングルノード（Single Node）で十分です
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. セットアップ
# MAGIC
# MAGIC まず、必要なライブラリをインポートし、ワークスペース情報を取得します。

# COMMAND ----------

# MAGIC %pip install -q databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.datasets import load_wine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import mlflow
from mlflow.models.signature import infer_signature

print("ライブラリのインポートが完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ウィジェットで設定値を指定
# MAGIC
# MAGIC ノートブックの上部にウィジェット（入力欄）が表示されます。
# MAGIC **カタログ名とスキーマ名を自分の環境に合わせて変更してください。**
# MAGIC
# MAGIC - **catalog**: Unity Catalogのカタログ名
# MAGIC - **schema**: スキーマ（データベース）名

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "カタログ名")
dbutils.widgets.text("schema", "default", "スキーマ名")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

model_name = f"{catalog}.{schema}.wine_classifier"
endpoint_name = "wine-classifier-endpoint"

print(f"モデル登録先: {model_name}")
print(f"エンドポイント名: {endpoint_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. モデルの学習
# MAGIC
# MAGIC 前回のノートブック（`01_scikit-learn_classification.py`）と同様に、
# MAGIC ワインデータセットでランダムフォレストモデルを学習します。
# MAGIC
# MAGIC > **補足**: 本来は前回学習したモデルを再利用することもできますが、
# MAGIC > このノートブックを単独で実行できるように、ここで改めて学習します。

# COMMAND ----------

# データの準備
wine = load_wine()
X = pd.DataFrame(wine.data, columns=wine.feature_names)
y = wine.target

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)

print(f"学習データ: {len(X_train)} 件")
print(f"テストデータ: {len(X_test)} 件")

# COMMAND ----------

# モデルの学習
model = RandomForestClassifier(
    n_estimators=100,
    max_depth=5,
    random_state=42,
)
model.fit(X_train, y_train)

accuracy = accuracy_score(y_test, model.predict(X_test))
print(f"テストデータの正解率: {accuracy:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unity Catalog にモデルを登録
# MAGIC
# MAGIC **Unity Catalog** は、Databricksのデータとモデルを一元管理するための仕組みです。
# MAGIC
# MAGIC モデルをUnity Catalogに登録すると:
# MAGIC - バージョン管理ができます（v1, v2, ... と履歴を管理）
# MAGIC - アクセス権を細かく制御できます
# MAGIC - Model Servingでそのまま公開できます
# MAGIC
# MAGIC **モデルの3層構造**:
# MAGIC ```
# MAGIC catalog.schema.model_name
# MAGIC   例: main.default.wine_classifier
# MAGIC ```

# COMMAND ----------

# Unity Catalog をモデルレジストリとして設定
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# モデルの入出力のシグネチャ（型情報）を推論
# これにより、モデルがどんなデータを受け取り、何を返すかが記録されます
signature = infer_signature(X_train, model.predict(X_train))

# MLflow でモデルをログし、Unity Catalog に登録
with mlflow.start_run(run_name="wine_model_for_serving") as run:
    model_info = mlflow.sklearn.log_model(
        sk_model=model,
        name="model",
        signature=signature,
        input_example=X_train.head(3),
        registered_model_name=model_name,
    )
    print(f"モデルが登録されました: {model_name}")
    print(f"Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 登録されたモデルのバージョンを確認

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# 最新のモデルバージョンを取得
model_version_infos = client.search_model_versions(f"name='{model_name}'")
latest_version = max(int(mv.version) for mv in model_version_infos)

print(f"モデル名: {model_name}")
print(f"最新バージョン: {latest_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Model Serving エンドポイントの作成
# MAGIC
# MAGIC 登録したモデルを **REST APIエンドポイント** として公開します。
# MAGIC
# MAGIC **Databricks SDK** を使ってエンドポイントを作成します。
# MAGIC
# MAGIC | 設定項目 | 説明 |
# MAGIC |---|---|
# MAGIC | `endpoint_name` | エンドポイントの名前（一意である必要あり） |
# MAGIC | `model_name` | Unity Catalogに登録したモデル名 |
# MAGIC | `model_version` | 使用するモデルバージョン |
# MAGIC | `workload_size` | サーバーの規模（Small / Medium / Large） |
# MAGIC | `scale_to_zero_enabled` | リクエストがないとき自動的にゼロにスケールするか |
# MAGIC
# MAGIC > **注意**: `scale_to_zero_enabled=True` にすると、使っていないときはコストが発生しません。
# MAGIC > ただし、ゼロからの起動には数分かかります。

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)

w = WorkspaceClient()

# 既存の同名エンドポイントがあれば更新、なければ新規作成
try:
    existing = w.serving_endpoints.get(endpoint_name)
    print(f"既存のエンドポイント '{endpoint_name}' を更新します...")
    w.serving_endpoints.update_config_and_wait(
        name=endpoint_name,
        served_entities=[
            ServedEntityInput(
                entity_name=model_name,
                entity_version=str(latest_version),
                workload_size="Small",
                scale_to_zero_enabled=True,
            )
        ],
    )
except Exception:
    print(f"新しいエンドポイント '{endpoint_name}' を作成します...")
    w.serving_endpoints.create_and_wait(
        name=endpoint_name,
        config=EndpointCoreConfigInput(
            name=endpoint_name,
            served_entities=[
                ServedEntityInput(
                    entity_name=model_name,
                    entity_version=str(latest_version),
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                )
            ]
        ),
    )

print(f"\nエンドポイント '{endpoint_name}' が準備完了しました！")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. エンドポイントの状態を確認
# MAGIC
# MAGIC エンドポイントが正常に稼働しているか確認します。

# COMMAND ----------

endpoint = w.serving_endpoints.get(endpoint_name)
print(f"エンドポイント名: {endpoint.name}")
print(f"状態: {endpoint.state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. エンドポイントに予測をリクエスト
# MAGIC
# MAGIC エンドポイントが準備できたら、**APIリクエスト**を送って予測を取得できます。
# MAGIC
# MAGIC ここでは2つの方法を紹介します:
# MAGIC 1. **Databricks SDK** を使う方法
# MAGIC 2. **REST API（requests ライブラリ）** を使う方法

# COMMAND ----------

# MAGIC %md
# MAGIC ### 方法1: Databricks SDK を使ったリクエスト
# MAGIC
# MAGIC SDK を使うと、認証の設定が自動的に行われるため、簡潔に書けます。

# COMMAND ----------

# テストデータから3件取得
test_data = X_test.head(3)
print("=== 入力データ ===")
display(test_data)

# COMMAND ----------

# SDK を使って予測をリクエスト
response = w.serving_endpoints.query(
    name=endpoint_name,
    dataframe_records=test_data.to_dict(orient="records"),
)

# 予測結果を表示
predictions = response.predictions
class_names = list(wine.target_names)

print("\n=== 予測結果 ===")
for i, pred in enumerate(predictions):
    class_name = class_names[int(pred)]
    print(f"  サンプル {i+1}: クラス {int(pred)} ({class_name})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 方法2: REST API を使ったリクエスト
# MAGIC
# MAGIC 外部のアプリケーション（Webアプリ、モバイルアプリなど）から呼び出す場合は、
# MAGIC REST API を使います。以下は `requests` ライブラリを使った例です。
# MAGIC
# MAGIC > **ポイント**: REST API を使えば、Python以外の言語（JavaScript、Java、Goなど）からも呼び出せます。
# MAGIC >
# MAGIC > **初心者の方へ**: REST API は「URLにデータを送って結果を受け取る」仕組みです。
# MAGIC > Webブラウザでページを開くのと同じ要領で、プログラムからデータを送受信します。

# COMMAND ----------

import os
import requests

# Databricks のトークンとURLを取得
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"

# REST APIで予測をリクエスト
url = f"{host}/serving-endpoints/{endpoint_name}/invocations"
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# リクエストボディ（JSON形式）
payload = {
    "dataframe_records": test_data.to_dict(orient="records"),
}

resp = requests.post(url, headers=headers, json=payload)
print(f"ステータスコード: {resp.status_code}")
print(f"レスポンス: {resp.json()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. バッチ予測の例
# MAGIC
# MAGIC テストデータ全体に対して一括で予測を実行し、結果を確認します。

# COMMAND ----------

# テストデータ全体で予測
response = w.serving_endpoints.query(
    name=endpoint_name,
    dataframe_records=X_test.to_dict(orient="records"),
)

# 結果をDataFrameにまとめる
results = X_test.copy()
results["predicted_class"] = [int(p) for p in response.predictions]
results["predicted_name"] = results["predicted_class"].map(
    {i: name for i, name in enumerate(wine.target_names)}
)
results["actual_class"] = y_test.values
results["actual_name"] = pd.Series(y_test.values).map(
    {i: name for i, name in enumerate(wine.target_names)}
).values
results["correct"] = results["predicted_class"] == results["actual_class"]

print(f"正解率: {results['correct'].mean():.4f}")
display(results[["predicted_name", "actual_name", "correct"]].head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **モデルの登録** — MLflowとUnity Catalogによるモデル管理
# MAGIC 2. **エンドポイントの作成** — Databricks SDK でModel Servingを設定
# MAGIC 3. **APIリクエスト** — SDK と REST API の2通りの方法で予測を取得
# MAGIC 4. **バッチ予測** — 複数データの一括予測
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - 次のノートブック **`09_databricks_app.py`** で、Databricks Apps を使ってブラウザからモデルを呼び出す Streamlit アプリをデプロイしましょう
# MAGIC - ハンズオン完了後は **`10_cleanup.py`** でリソースをクリーンアップしてください
# MAGIC
# MAGIC > **認定試験との関連** (ML Associate):
# MAGIC > - **Model Deployment (12%)**: バッチ/リアルタイム/ストリーミングの違い、カスタムモデルのエンドポイントデプロイ手順
# MAGIC > - **Databricks Machine Learning (38%)**: MLflow でのモデル登録、シグネチャの推論

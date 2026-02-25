# Databricks notebook source
# MAGIC %md
# MAGIC # MLflow による実験管理とモデルライフサイクル
# MAGIC
# MAGIC このノートブックでは **MLflow** の主要機能を体系的に学びます。
# MAGIC
# MAGIC **MLflow とは？**
# MAGIC > 機械学習の実験管理、モデル管理、デプロイを支援するプラットフォームです。
# MAGIC > Databricks に組み込まれており、追加設定なしで利用できます。
# MAGIC
# MAGIC ## MLflow の4つの主要コンポーネント
# MAGIC
# MAGIC | コンポーネント | 説明 |
# MAGIC |---|---|
# MAGIC | **Tracking** | パラメータ・メトリクス・モデルの記録 |
# MAGIC | **Models** | モデルのパッケージング・フォーマット |
# MAGIC | **Model Registry** | モデルのバージョン管理・ステージ管理 |
# MAGIC | **Projects** | 実行環境の再現性（コード＋環境の定義） |
# MAGIC
# MAGIC > **初心者の方へ: MLflow の全体像**
# MAGIC > ```
# MAGIC > ① 実験を記録する (Tracking)
# MAGIC >    └─ パラメータ、メトリクス、グラフ、モデルを自動/手動で記録
# MAGIC >
# MAGIC > ② モデルを登録する (Model Registry)
# MAGIC >    └─ バージョン管理、champion/challenger のエイリアス付け
# MAGIC >
# MAGIC > ③ モデルをデプロイする (Model Serving ← 次のノートブック)
# MAGIC >    └─ REST API として公開、アプリから呼び出し
# MAGIC > ```
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）
# MAGIC - Unity Catalog が有効なワークスペース
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. MLflow Tracking — 実験の記録

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.datasets import load_wine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score
import pandas as pd

wine = load_wine()
X = pd.DataFrame(wine.data, columns=wine.feature_names)
y = wine.target
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 手動ロギング vs 自動ロギング
# MAGIC
# MAGIC | 方法 | 説明 | 使い分け |
# MAGIC |---|---|---|
# MAGIC | `mlflow.log_param()` / `mlflow.log_metric()` | 手動で記録 | 細かいカスタム記録が必要な場合 |
# MAGIC | `mlflow.sklearn.autolog()` | 自動記録 | 素早く実験を始めたい場合 |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 複数モデルの手動ロギング比較

# COMMAND ----------

# 実験名を設定
experiment_name = f"/Users/{spark.conf.get('spark.databricks.workspaceUrl', 'user')}/wine_model_comparison"
mlflow.set_experiment(experiment_name)

models = {
    "RandomForest": RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42),
    "GradientBoosting": GradientBoostingClassifier(n_estimators=100, max_depth=3, random_state=42),
    "LogisticRegression": LogisticRegression(max_iter=1000, random_state=42),
}

results = []

for model_name, model in models.items():
    with mlflow.start_run(run_name=model_name):
        # 学習
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        # メトリクスの計算
        acc = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, average="weighted")

        # MLflowに手動ログ
        mlflow.log_param("model_type", model_name)
        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("f1_score", f1)
        mlflow.sklearn.log_model(model, "model")

        # タグの設定（メタ情報）
        mlflow.set_tag("dataset", "wine")
        mlflow.set_tag("task", "classification")

        results.append({"model": model_name, "accuracy": acc, "f1_score": f1})
        print(f"{model_name}: accuracy={acc:.4f}, f1={f1:.4f}")

# 結果をDataFrameで比較
results_df = pd.DataFrame(results).sort_values("f1_score", ascending=False)
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. MLflow Tracking — アーティファクトの記録
# MAGIC
# MAGIC メトリクスやパラメータに加えて、**アーティファクト**（ファイル）も記録できます。
# MAGIC グラフ、設定ファイル、データサンプルなどを一緒に保存すると便利です。

# COMMAND ----------

import matplotlib.pyplot as plt
from sklearn.metrics import ConfusionMatrixDisplay
import tempfile
import os

best_model = models["RandomForest"]
best_model.fit(X_train, y_train)
y_pred = best_model.predict(X_test)

with mlflow.start_run(run_name="RandomForest_with_artifacts"):
    mlflow.log_metric("accuracy", accuracy_score(y_test, y_pred))
    mlflow.sklearn.log_model(best_model, "model")

    # 混同行列をアーティファクトとして保存
    fig, ax = plt.subplots(figsize=(8, 6))
    ConfusionMatrixDisplay.from_predictions(y_test, y_pred, display_labels=wine.target_names, ax=ax)
    ax.set_title("Confusion Matrix")

    with tempfile.TemporaryDirectory() as tmp:
        fig_path = os.path.join(tmp, "confusion_matrix.png")
        fig.savefig(fig_path, bbox_inches="tight")
        mlflow.log_artifact(fig_path, "figures")
        print(f"混同行列をアーティファクトとして記録しました")

    plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Model Registry — モデルのバージョン管理
# MAGIC
# MAGIC **Model Registry** は、本番環境にデプロイするモデルを管理する仕組みです。
# MAGIC
# MAGIC Unity Catalog Model Registry では:
# MAGIC - モデルのバージョンを自動的に管理（v1, v2, ...）
# MAGIC - エイリアス（`champion`, `challenger` など）でモデルにラベル付け
# MAGIC - アクセス権をUnity Catalog で一元管理

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "カタログ名")
dbutils.widgets.text("schema", "default", "スキーマ名")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
model_name = f"{catalog}.{schema}.wine_best_model"

# Unity Catalog をレジストリに設定
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# 最良モデルを登録
from mlflow.models.signature import infer_signature

with mlflow.start_run(run_name="register_best_model"):
    signature = infer_signature(X_train, best_model.predict(X_train))
    model_info = mlflow.sklearn.log_model(
        sk_model=best_model,
        artifact_path="model",
        signature=signature,
        input_example=X_train.head(3),
        registered_model_name=model_name,
    )
    print(f"モデルを登録しました: {model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### エイリアスの設定
# MAGIC
# MAGIC **エイリアス**を使って、モデルの役割をラベル付けできます。
# MAGIC
# MAGIC | エイリアス | 意味 |
# MAGIC |---|---|
# MAGIC | `champion` | 現在の本番モデル |
# MAGIC | `challenger` | 本番候補（テスト中） |
# MAGIC
# MAGIC > **初心者の方へ**: エイリアスは「ニックネーム」のようなものです。
# MAGIC > バージョン番号（v1, v2...）だと「どれが本番？」がわかりにくいですが、
# MAGIC > `champion` というエイリアスを付けておけば、常に「本番モデル」が明確になります。
# MAGIC >
# MAGIC > 新しいモデルを `challenger` にして性能比較し、良ければ `champion` に昇格させる
# MAGIC > というワークフローが一般的です。

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# 最新バージョンを取得
versions = client.search_model_versions(f"name='{model_name}'")
latest_version = max(int(v.version) for v in versions)

# エイリアスを設定
client.set_registered_model_alias(model_name, "champion", str(latest_version))
print(f"バージョン {latest_version} に 'champion' エイリアスを設定しました")

# COMMAND ----------

# エイリアスを使ってモデルをロード
champion_model = mlflow.sklearn.load_model(f"models:/{model_name}@champion")
predictions = champion_model.predict(X_test[:5])
print(f"Champion モデルの予測: {predictions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MLflow の実験UIで確認
# MAGIC
# MAGIC 左サイドバーの **「エクスペリメント」** から以下を確認できます:
# MAGIC
# MAGIC - **Run一覧**: 各実行のパラメータ・メトリクスを一覧表示
# MAGIC - **比較**: 複数のRunを選択してメトリクスを比較
# MAGIC - **アーティファクト**: 保存したファイル（混同行列など）を確認
# MAGIC - **モデルレジストリ**: 登録モデルのバージョンとエイリアス
# MAGIC
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC 1. **MLflow Tracking** — パラメータ・メトリクス・アーティファクトの記録
# MAGIC 2. **実験比較** — 複数モデルの性能を一元的に比較
# MAGIC 3. **Model Registry** — バージョン管理とエイリアスによるモデルライフサイクル管理
# MAGIC 4. **Unity Catalog連携** — アクセス権の一元管理
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - `08_model_serving.py` でモデルをAPIとして公開する方法を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (ML Associate):
# MAGIC > - **Databricks Machine Learning (38%)**: MLflow Client API でベスト Run を特定、手動ロギング、nested Run
# MAGIC > - **Databricks Machine Learning (38%)**: Model Registry でのモデル登録、エイリアス（champion/challenger）の遷移
# MAGIC > - **Databricks Machine Learning (38%)**: MLflow UI でのコード・実行時刻の確認

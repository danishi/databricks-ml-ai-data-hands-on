# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Feature Store
# MAGIC
# MAGIC このノートブックでは **Feature Store（特徴量ストア）** を学びます。
# MAGIC
# MAGIC **Feature Store とは？**
# MAGIC > 機械学習で使う特徴量（入力データ）を**一元管理**する仕組みです。
# MAGIC > チームで同じ特徴量を再利用でき、学習時と推論時で異なる特徴量を使うミスを防げます。
# MAGIC
# MAGIC > **初心者の方へ: なぜ Feature Store？**
# MAGIC >
# MAGIC > 例えば、チームAとチームBが同じ「顧客の年齢」特徴量を**別々に計算**していたら、
# MAGIC > 微妙に異なる値になるかもしれません。Feature Store に1回登録しておけば、
# MAGIC > 全チームが**同じ特徴量**を使えます。
# MAGIC >
# MAGIC > また、学習時に使った特徴量と推論時の特徴量が異なると、モデルの精度が落ちます。
# MAGIC > Feature Store はこの「学習-推論スキュー」を防ぎます。
# MAGIC
# MAGIC ## 学べること
# MAGIC - Feature Store の概念と利点
# MAGIC - Unity Catalog による特徴量テーブルの作成
# MAGIC - 特徴量テーブルを使ったモデル学習
# MAGIC - 特徴量ルックアップによる推論
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）
# MAGIC - Unity Catalog が有効なワークスペース
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Feature Store の概要
# MAGIC
# MAGIC ### なぜ Feature Store が必要？
# MAGIC
# MAGIC | 課題 | Feature Store による解決 |
# MAGIC |---|---|
# MAGIC | 特徴量の重複開発 | 一度作った特徴量をチームで共有・再利用 |
# MAGIC | 学習時と推論時の不一致 | 同じ定義の特徴量を使うことを保証 |
# MAGIC | データの血統追跡 | どのモデルがどの特徴量を使っているか追跡可能 |
# MAGIC | 特徴量の発見 | カタログで検索して既存の特徴量を発見 |
# MAGIC
# MAGIC > **試験ポイント: オンラインテーブル vs オフラインテーブル**
# MAGIC >
# MAGIC > | 種類 | 用途 | レイテンシ |
# MAGIC > |---|---|---|
# MAGIC > | **オフライン** | バッチ推論、学習 | 秒〜分（大量データ向け） |
# MAGIC > | **オンライン** | リアルタイム推論 | ミリ秒（低レイテンシ） |
# MAGIC >
# MAGIC > Unity Catalog の Feature Store はアカウントレベルで管理されるため、
# MAGIC > ワークスペースレベルよりも**アクセス制御とガバナンス**に優れています。
# MAGIC
# MAGIC ### Unity Catalog Feature Store のアーキテクチャ
# MAGIC
# MAGIC ```
# MAGIC Unity Catalog
# MAGIC └── カタログ (例: main)
# MAGIC     └── スキーマ (例: default)
# MAGIC         └── 特徴量テーブル (例: wine_features)
# MAGIC             ├── 主キー: wine_id
# MAGIC             ├── alcohol: 13.0
# MAGIC             ├── color_intensity: 5.1
# MAGIC             └── ...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. セットアップ

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from sklearn.datasets import load_wine
import pandas as pd
import mlflow
from pyspark.sql import functions as F

fe = FeatureEngineeringClient()

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "カタログ名")
dbutils.widgets.text("schema", "default", "スキーマ名")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

feature_table_name = f"{catalog}.{schema}.wine_features"
print(f"特徴量テーブル: {feature_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 特徴量テーブルの作成
# MAGIC
# MAGIC ワインデータセットの特徴量を Unity Catalog の特徴量テーブルとして登録します。
# MAGIC
# MAGIC > **主キー（Primary Key）**: 各レコードを一意に識別するID列です。
# MAGIC > 推論時にこのキーで特徴量を検索（ルックアップ）します。

# COMMAND ----------

# データの準備
wine = load_wine()
pdf = pd.DataFrame(wine.data, columns=wine.feature_names)
pdf["wine_id"] = range(len(pdf))  # 主キーを追加
pdf["target"] = wine.target

# 特徴量テーブル用のDataFrame（IDと特徴量のみ）
feature_pdf = pdf[["wine_id"] + list(wine.feature_names)]
feature_sdf = spark.createDataFrame(feature_pdf)

# 特徴量テーブルを作成
fe.create_table(
    name=feature_table_name,
    primary_keys=["wine_id"],
    df=feature_sdf,
    description="ワインの化学的特徴量（scikit-learn wine dataset）",
)

print(f"特徴量テーブル '{feature_table_name}' を作成しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 特徴量ルックアップを使ったモデル学習
# MAGIC
# MAGIC **FeatureLookup** を使うと、学習時に特徴量テーブルからデータを自動的に取得できます。
# MAGIC
# MAGIC ```
# MAGIC 学習データ: [wine_id, target]  ← IDと正解ラベルのみ
# MAGIC     ↓ FeatureLookup
# MAGIC 特徴量テーブル: [wine_id, alcohol, color_intensity, ...]
# MAGIC     ↓ 自動結合
# MAGIC 完全な学習データ: [wine_id, target, alcohol, color_intensity, ...]
# MAGIC ```

# COMMAND ----------

# 学習用データ（IDとラベルのみ）
train_pdf = pdf[["wine_id", "target"]]
train_sdf = spark.createDataFrame(train_pdf)

# FeatureLookupの定義
feature_lookups = [
    FeatureLookup(
        table_name=feature_table_name,
        lookup_key="wine_id",
    ),
]

# 特徴量ルックアップを使って学習データセットを作成
training_set = fe.create_training_set(
    df=train_sdf,
    feature_lookups=feature_lookups,
    label="target",
)

# pandas DataFrameに変換して確認
training_df = training_set.load_df()
display(training_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Feature Store 連携モデルの学習と記録

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# 学習データを準備
training_pdf = training_df.toPandas()
feature_cols = wine.feature_names
X = training_pdf[feature_cols]
y = training_pdf["target"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

# モデルを学習
model = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)
model.fit(X_train, y_train)
accuracy = accuracy_score(y_test, model.predict(X_test))
print(f"テスト精度: {accuracy:.4f}")

# COMMAND ----------

# Feature Store と連携してモデルをMLflowに記録
mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="wine_feature_store_model") as run:
    fe.log_model(
        model=model,
        artifact_path="model",
        flavor=mlflow.sklearn,
        training_set=training_set,
    )
    mlflow.log_metric("test_accuracy", accuracy)
    print(f"モデルをMLflowに記録しました (Run ID: {run.info.run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 特徴量ルックアップによる推論（バッチ）
# MAGIC
# MAGIC 推論時は**IDだけ渡せば**、Feature Storeから自動的に特徴量を取得して予測できます。

# COMMAND ----------

# 推論したいデータ（IDのみ）
inference_sdf = spark.createDataFrame(
    pd.DataFrame({"wine_id": [0, 10, 50, 100, 150]})
)

# Feature Storeから特徴量を自動取得して推論
predictions = fe.score_batch(
    model_uri=f"runs:/{run.info.run_id}/model",
    df=inference_sdf,
)

display(predictions.select("wine_id", "prediction"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC 1. **Feature Store** — 特徴量を一元管理して再利用
# MAGIC 2. **特徴量テーブル** — Unity Catalog でメタデータとともに管理
# MAGIC 3. **FeatureLookup** — IDから自動的に特徴量を結合
# MAGIC 4. **モデル連携** — Feature Storeと連携したモデルの記録・推論
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - `07_mlflow_experiment.py` で MLflow を深く学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (ML Associate):
# MAGIC > - **Databricks Machine Learning (38%)**: Feature Store テーブルの作成・書き込み・学習・推論
# MAGIC > - **Databricks Machine Learning (38%)**: FeatureLookup、オンライン/オフラインテーブルの違い、Unity Catalog 連携

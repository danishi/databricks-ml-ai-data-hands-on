# Databricks notebook source
# MAGIC %md
# MAGIC # Spark ML パイプライン
# MAGIC
# MAGIC このノートブックでは **Spark ML** を使った機械学習パイプラインを学びます。
# MAGIC
# MAGIC **なぜ Spark ML？**
# MAGIC > scikit-learn は単一マシンで動作しますが、データが大量になると処理しきれません。
# MAGIC > Spark ML はクラスタで**分散処理**できるため、大規模データにも対応できます。
# MAGIC
# MAGIC ```
# MAGIC scikit-learn:  1台のPC で処理       → 数万〜数百万行が限界
# MAGIC Spark ML:      複数台で分散処理      → 数億〜数十億行でもOK
# MAGIC
# MAGIC   PC1 ──┐
# MAGIC   PC2 ──┼──→ 結果を統合  ← これが分散処理
# MAGIC   PC3 ──┘
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - Spark ML の Estimator / Transformer / Pipeline の概念
# MAGIC - パイプラインの構築と実行
# MAGIC - Pandas API on Spark
# MAGIC - Pandas UDF による分散処理
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）のクラスター
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Spark ML の基本概念
# MAGIC
# MAGIC Spark ML は3つの主要な概念で構成されています:
# MAGIC
# MAGIC | 概念 | 説明 | 例 |
# MAGIC |---|---|---|
# MAGIC | **Transformer** | データを変換する | StandardScaler, OneHotEncoder |
# MAGIC | **Estimator** | データから学習してモデルを作る | RandomForestClassifier, LogisticRegression |
# MAGIC | **Pipeline** | 複数のステップを連結する | Transformer + Estimator を一本化 |
# MAGIC
# MAGIC ```
# MAGIC Pipeline の流れ:
# MAGIC  生データ → [Indexer] → [Encoder] → [Assembler] → [Scaler] → [Classifier] → 予測結果
# MAGIC            Transformer  Transformer  Transformer  Transformer   Estimator
# MAGIC ```

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
    StandardScaler,
)
from pyspark.ml.classification import RandomForestClassifier as SparkRF
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from sklearn.datasets import load_wine
import pandas as pd

print("ライブラリのインポート完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. データの準備（Spark DataFrame）

# COMMAND ----------

# ワインデータをSpark DataFrameに変換
wine = load_wine()
pdf = pd.DataFrame(wine.data, columns=wine.feature_names)
pdf["label"] = wine.target

sdf = spark.createDataFrame(pdf)
train_df, test_df = sdf.randomSplit([0.7, 0.3], seed=42)

print(f"学習データ: {train_df.count()}件, テストデータ: {test_df.count()}件")
display(train_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. パイプラインの構築
# MAGIC
# MAGIC 前処理からモデル学習までを**1つのパイプライン**にまとめます。
# MAGIC
# MAGIC > **パイプラインのメリット**:
# MAGIC > - 前処理とモデルをセットで管理できる
# MAGIC > - 新しいデータにも同じ前処理を自動適用
# MAGIC > - MLflow でパイプライン全体を保存・再利用可能
# MAGIC
# MAGIC > **初心者の方へ: Pipeline とは何か？**
# MAGIC >
# MAGIC > 工場の「製造ライン」をイメージしてください。
# MAGIC > 原材料（生データ）がベルトコンベアに乗って、各工程（Transformer）で加工され、
# MAGIC > 最終工程（Estimator）で完成品（予測モデル）になります。
# MAGIC >
# MAGIC > `pipeline.fit()` で製造ラインを構築し、
# MAGIC > `pipeline_model.transform()` で新しいデータを同じラインに流すと自動的に予測が得られます。

# COMMAND ----------

# ステップ1: 特徴量をベクトルにまとめる
feature_cols = wine.feature_names
assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")

# ステップ2: 標準化
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withMean=True,
    withStd=True,
)

# ステップ3: ランダムフォレスト分類器
rf = SparkRF(
    labelCol="label",
    featuresCol="features",
    numTrees=100,
    maxDepth=5,
    seed=42,
)

# パイプラインの構築
pipeline = Pipeline(stages=[assembler, scaler, rf])

print("パイプラインのステージ:")
for i, stage in enumerate(pipeline.getStages()):
    print(f"  {i+1}. {type(stage).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. パイプラインの学習と評価

# COMMAND ----------

# パイプラインを学習データで実行
pipeline_model = pipeline.fit(train_df)

# テストデータで予測
predictions = pipeline_model.transform(test_df)
display(predictions.select("label", "prediction", "probability").limit(10))

# COMMAND ----------

# 精度を評価
evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy",
)
accuracy = evaluator.evaluate(predictions)
print(f"テスト精度: {accuracy:.4f}")

# F1スコアも確認
f1_evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="f1",
)
f1 = f1_evaluator.evaluate(predictions)
print(f"F1スコア: {f1:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. CrossValidator によるパイプラインチューニング
# MAGIC
# MAGIC Spark ML の `CrossValidator` でパイプライン全体のハイパーパラメータチューニングができます。

# COMMAND ----------

# パラメータグリッドの定義
param_grid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [50, 100, 200])
    .addGrid(rf.maxDepth, [3, 5, 10])
    .build()
)

# CrossValidatorの設定
cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=3,
    seed=42,
)

# チューニングを実行
cv_model = cv.fit(train_df)

# 最良モデルで評価
best_predictions = cv_model.transform(test_df)
best_accuracy = evaluator.evaluate(best_predictions)
print(f"CrossValidator後の最良テスト精度: {best_accuracy:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pandas API on Spark
# MAGIC
# MAGIC **Pandas API on Spark** は、pandasの構文でSparkの分散処理を利用できる機能です。
# MAGIC 既存のpandasコードをほぼそのまま大規模データに適用できます。
# MAGIC
# MAGIC > **初心者の方へ**: pandasに慣れている方は `.pandas_api()` で変換するだけで
# MAGIC > いつもの pandas 構文が使えます。裏側では Spark の分散処理が動いています。
# MAGIC >
# MAGIC > **Apache Arrow** という技術が Pandas ↔ Spark のデータ変換を高速化しています。
# MAGIC
# MAGIC ```python
# MAGIC # pandas
# MAGIC import pandas as pd
# MAGIC df = pd.read_csv("data.csv")
# MAGIC
# MAGIC # Pandas API on Spark（同じ構文で分散処理！）
# MAGIC import pyspark.pandas as ps
# MAGIC df = ps.read_csv("data.csv")
# MAGIC ```

# COMMAND ----------

import pyspark.pandas as ps

# pandas API on Spark でデータを操作
psdf = sdf.pandas_api()

# pandasと同じ構文で集計
print("=== クラスごとの平均値 ===")
display(psdf.groupby("label")[["alcohol", "color_intensity"]].mean())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pandas UDF（ユーザー定義関数）
# MAGIC
# MAGIC **Pandas UDF** を使うと、pandasの関数をSpark DataFrameに**分散適用**できます。
# MAGIC scikit-learnなどの単一ノードライブラリをSparkの分散環境で活用できます。
# MAGIC
# MAGIC > **なぜ Pandas UDF が便利？**
# MAGIC >
# MAGIC > 既存の pandas / scikit-learn のコードを書き直さずに、Spark で並列実行できます。
# MAGIC > 例: 学習済みモデルの `.predict()` を Pandas UDF にすれば、大量データの予測を分散実行可能。
# MAGIC >
# MAGIC > **大量データの場合**: Iterator UDF を使うと、データをバッチごとに処理できて効率的です。

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

@pandas_udf(DoubleType())
def normalize_column(series: pd.Series) -> pd.Series:
    """列を0〜1の範囲に正規化するPandas UDF"""
    return (series - series.min()) / (series.max() - series.min())

# Spark DataFrameに適用
sdf_normalized = sdf.withColumn("alcohol_normalized", normalize_column("alcohol"))
display(sdf_normalized.select("alcohol", "alcohol_normalized").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC 1. **Spark ML の概念** — Estimator / Transformer / Pipeline
# MAGIC 2. **パイプライン** — 前処理とモデルを一本化
# MAGIC 3. **CrossValidator** — パイプライン全体のハイパーパラメータチューニング
# MAGIC 4. **Pandas API on Spark** — pandasの構文で分散処理
# MAGIC 5. **Pandas UDF** — pandas関数のSpark分散適用
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - `05_automl.py` で Databricks AutoML を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (ML Associate):
# MAGIC > - **Model Development (31%)**: Spark ML の Estimator/Transformer/Pipeline パターン、randomSplit、CrossValidator
# MAGIC > - **Model Development (31%)**: Pandas API on Spark、Apache Arrow、Pandas UDF の分散適用
# MAGIC > - **Model Deployment (12%)**: Spark による線形回帰・決定木のスケーリング

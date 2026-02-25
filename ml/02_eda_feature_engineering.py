# Databricks notebook source
# MAGIC %md
# MAGIC # 探索的データ分析（EDA）と特徴量エンジニアリング
# MAGIC
# MAGIC このノートブックでは、機械学習の前処理として欠かせない **EDA（Exploratory Data Analysis）** と
# MAGIC **特徴量エンジニアリング** を学びます。
# MAGIC
# MAGIC **EDA とは？**
# MAGIC > データの特徴を可視化・集計して理解する作業です。
# MAGIC > 「どんなデータなのか」「欠損値や外れ値はないか」「特徴量同士の関係は？」を調べます。
# MAGIC
# MAGIC **特徴量エンジニアリングとは？**
# MAGIC > モデルの入力データを加工・改善する作業です。
# MAGIC > 適切な前処理を行うことで、モデルの精度が大きく向上します。
# MAGIC
# MAGIC ### なぜ EDA と特徴量エンジニアリングが重要なのか？
# MAGIC
# MAGIC ```
# MAGIC 生のデータ                      きれいなデータ
# MAGIC ┌──────────────┐               ┌──────────────┐
# MAGIC │ 欠損値あり    │  EDA+前処理   │ 欠損値なし    │
# MAGIC │ 外れ値あり    │  ─────────→  │ スケール統一  │  → モデルの精度UP!
# MAGIC │ スケールばらばら│              │ 数値化済み    │
# MAGIC └──────────────┘               └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC > **よく言われること**: 「機械学習の成功の8割はデータの前処理で決まる」
# MAGIC
# MAGIC ## 学べること
# MAGIC - Spark DataFrameを使ったデータの読み込みと基本操作
# MAGIC - 欠損値・外れ値の検出と処理
# MAGIC - 特徴量のスケーリングとエンコーディング
# MAGIC - 特徴量の選択と相関分析
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）のクラスター
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. データの準備（Spark DataFrame）
# MAGIC
# MAGIC Databricksでは **Spark DataFrame** を使ってデータを操作するのが基本です。
# MAGIC pandasに似ていますが、大規模データを**分散処理**できるのが特徴です。
# MAGIC
# MAGIC | 特徴 | pandas DataFrame | Spark DataFrame |
# MAGIC |---|---|---|
# MAGIC | データ量 | メモリに収まる範囲 | 数TB以上も可 |
# MAGIC | 処理 | 単一マシン | クラスタで分散 |
# MAGIC | 構文 | `df.head()` | `df.show()` |

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import matplotlib.pyplot as plt

# サンプルデータの作成（住宅価格データを模擬）
np.random.seed(42)
n = 500

data = pd.DataFrame({
    "area": np.random.normal(80, 20, n).clip(20, 200),           # 面積（㎡）
    "rooms": np.random.choice([1, 2, 3, 4, 5], n, p=[0.1, 0.25, 0.35, 0.2, 0.1]),
    "age": np.random.exponential(15, n).clip(0, 50),              # 築年数
    "station_dist": np.random.exponential(10, n).clip(1, 60),     # 駅距離（分）
    "floor": np.random.choice(["1F", "2F", "3F", "4F以上"], n),   # 階数（カテゴリ）
    "structure": np.random.choice(["木造", "RC", "SRC"], n, p=[0.3, 0.4, 0.3]),
})

# 目的変数（価格）を特徴量から生成
data["price"] = (
    data["area"] * 30
    + data["rooms"] * 500
    - data["age"] * 20
    - data["station_dist"] * 50
    + np.random.normal(0, 300, n)
).clip(500, 10000)

# 欠損値を意図的に追加（実データでは欠損は必ず存在する）
mask = np.random.random(n) < 0.05
data.loc[mask, "age"] = np.nan
mask2 = np.random.random(n) < 0.03
data.loc[mask2, "station_dist"] = np.nan

# Spark DataFrameに変換
sdf = spark.createDataFrame(data)
print(f"データ件数: {sdf.count()}")
sdf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 基本統計量の確認
# MAGIC
# MAGIC `describe()` で各列の基本統計量（件数、平均、標準偏差、最小値、最大値）を確認します。
# MAGIC `summary()` を使うと四分位数（25%, 50%, 75%）も表示されます。
# MAGIC
# MAGIC > **初心者の方へ: 基本統計量の見方**
# MAGIC > - **mean（平均）**: データの中心的な値。外れ値の影響を受けやすい
# MAGIC > - **stddev（標準偏差）**: データのばらつき具合。大きいほどばらばら
# MAGIC > - **50%（中央値）**: データを小さい順に並べた真ん中の値。外れ値に強い
# MAGIC > - **min / max**: 最小値と最大値。異常に大きい/小さい値がないかチェック

# COMMAND ----------

display(sdf.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 欠損値の確認と処理
# MAGIC
# MAGIC **欠損値（null/NaN）** はほとんどのデータに存在します。
# MAGIC 欠損値を放置するとモデルがエラーになったり精度が下がるため、適切な処理が必要です。
# MAGIC
# MAGIC | 処理方法 | 説明 | 適した場面 |
# MAGIC |---|---|---|
# MAGIC | 削除 | 欠損行を削除 | 欠損が少ない場合 |
# MAGIC | 平均値で補完 | 平均値で埋める | 数値データ |
# MAGIC | 中央値で補完 | 中央値で埋める | 外れ値がある数値データ |
# MAGIC | 最頻値で補完 | 最も多い値で埋める | カテゴリデータ |

# COMMAND ----------

# 各列の欠損値数を確認
null_counts = sdf.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in sdf.columns
])
display(null_counts)

# COMMAND ----------

# 欠損値を中央値で補完（外れ値に強い）
from pyspark.ml.feature import Imputer

imputer = Imputer(
    inputCols=["age", "station_dist"],
    outputCols=["age", "station_dist"],
    strategy="median",  # "mean", "median", "mode" から選択
)

sdf_imputed = imputer.fit(sdf).transform(sdf)

# 補完後の欠損値数を確認
null_after = sdf_imputed.select([
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in ["age", "station_dist"]
])
print("補完後の欠損値数:")
display(null_after)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 外れ値の検出
# MAGIC
# MAGIC **外れ値**は他のデータから極端に離れた値です。
# MAGIC 外れ値があるとモデルの学習に悪影響を与えることがあります。
# MAGIC
# MAGIC 一般的な検出方法として **IQR（四分位範囲）法** を使います:
# MAGIC - Q1（25パーセンタイル）とQ3（75パーセンタイル）を計算
# MAGIC - IQR = Q3 - Q1
# MAGIC - Q1 - 1.5×IQR 未満、または Q3 + 1.5×IQR 超を外れ値とみなす

# COMMAND ----------

# 面積と価格の外れ値を検出
pdf = sdf_imputed.toPandas()

fig, axes = plt.subplots(1, 3, figsize=(15, 4))
for i, col in enumerate(["area", "station_dist", "price"]):
    axes[i].boxplot(pdf[col].dropna())
    axes[i].set_title(f"{col} の分布")
    axes[i].set_ylabel(col)
plt.tight_layout()
plt.show()

# COMMAND ----------

# IQR法で外れ値を除去する例
def remove_outliers_iqr(df, column, factor=1.5):
    """IQR法で外れ値を除去"""
    quantiles = df.approxQuantile(column, [0.25, 0.75], 0.01)
    q1, q3 = quantiles[0], quantiles[1]
    iqr = q3 - q1
    lower = q1 - factor * iqr
    upper = q3 + factor * iqr
    filtered = df.filter((F.col(column) >= lower) & (F.col(column) <= upper))
    removed = df.count() - filtered.count()
    print(f"{column}: {removed}件の外れ値を除去 (範囲: {lower:.1f} ~ {upper:.1f})")
    return filtered

sdf_clean = remove_outliers_iqr(sdf_imputed, "price")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 特徴量のスケーリング
# MAGIC
# MAGIC 特徴量の**スケール（値の範囲）** が大きく異なると、一部のアルゴリズムで問題が発生します。
# MAGIC
# MAGIC | 方法 | 説明 | 変換後の範囲 |
# MAGIC |---|---|---|
# MAGIC | StandardScaler | 平均0、標準偏差1に変換 | 約 -3 ~ +3 |
# MAGIC | MinMaxScaler | 最小0、最大1に変換 | 0 ~ 1 |
# MAGIC
# MAGIC > **注意**: ランダムフォレストや決定木はスケーリング不要です。
# MAGIC > ロジスティック回帰やSVMなどの距離ベースのアルゴリズムで重要です。
# MAGIC
# MAGIC > **初心者の方へ: なぜスケーリングが必要？**
# MAGIC >
# MAGIC > たとえば「面積（20〜200㎡）」と「部屋数（1〜5）」では数値の桁が違います。
# MAGIC > スケーリングをしないと、面積の方が数値が大きいだけで「重要」と誤解されることがあります。
# MAGIC > StandardScaler は全ての特徴量を**同じ尺度**に揃えて、公平に比較できるようにします。

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, StandardScaler

numeric_cols = ["area", "rooms", "age", "station_dist"]

# 数値列をベクトルにまとめる（Spark MLの入力形式）
assembler = VectorAssembler(inputCols=numeric_cols, outputCol="numeric_features")
sdf_assembled = assembler.transform(sdf_clean)

# StandardScalerで標準化
scaler = StandardScaler(
    inputCol="numeric_features",
    outputCol="scaled_features",
    withMean=True,
    withStd=True,
)
scaler_model = scaler.fit(sdf_assembled)
sdf_scaled = scaler_model.transform(sdf_assembled)

print("スケーリング前後の比較:")
display(sdf_scaled.select("numeric_features", "scaled_features").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. カテゴリ変数のエンコーディング
# MAGIC
# MAGIC 機械学習モデルは**数値**しか扱えないため、カテゴリ変数（文字列）を数値に変換する必要があります。
# MAGIC
# MAGIC | 方法 | 説明 | 適した場面 |
# MAGIC |---|---|---|
# MAGIC | StringIndexer | カテゴリを連番（0, 1, 2...）に変換 | 順序のあるカテゴリ |
# MAGIC | OneHotEncoder | 各カテゴリを独立した列に展開（0/1） | 順序のないカテゴリ |
# MAGIC
# MAGIC > **具体例でイメージしよう: OneHotEncoder**
# MAGIC > ```
# MAGIC > 元データ:  floor = "2F"
# MAGIC >
# MAGIC > StringIndexer後:  floor_index = 1
# MAGIC >
# MAGIC > OneHotEncoder後:  floor_ohe = [0, 1, 0, 0]
# MAGIC >                                1F  2F  3F  4F以上
# MAGIC > ```
# MAGIC > なぜ数値の連番（0, 1, 2, 3）ではダメなのか？ → モデルが「3F は 1F の3倍」と誤解するためです。
# MAGIC > OneHot にすると各カテゴリが独立した列になり、順序関係がなくなります。
# MAGIC >
# MAGIC > **ただし注意**: 決定木系のモデル（ランダムフォレスト、XGBoostなど）は
# MAGIC > StringIndexer だけで十分なことが多く、OneHot にすると逆に効率が悪くなることがあります。

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, OneHotEncoder

# StringIndexer: 文字列 → 数値インデックス
floor_indexer = StringIndexer(inputCol="floor", outputCol="floor_index")
struct_indexer = StringIndexer(inputCol="structure", outputCol="structure_index")

sdf_indexed = floor_indexer.fit(sdf_clean).transform(sdf_clean)
sdf_indexed = struct_indexer.fit(sdf_indexed).transform(sdf_indexed)

# OneHotEncoder: インデックス → ワンホットベクトル
encoder = OneHotEncoder(
    inputCols=["floor_index", "structure_index"],
    outputCols=["floor_ohe", "structure_ohe"],
)
sdf_encoded = encoder.fit(sdf_indexed).transform(sdf_indexed)

print("エンコーディング結果:")
display(sdf_encoded.select("floor", "floor_index", "floor_ohe", "structure", "structure_index", "structure_ohe").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 相関分析
# MAGIC
# MAGIC 特徴量同士の**相関**を確認します。
# MAGIC
# MAGIC - **正の相関（1に近い）**: 一方が増えると他方も増える
# MAGIC - **負の相関（-1に近い）**: 一方が増えると他方は減る
# MAGIC - **無相関（0に近い）**: 関係がない
# MAGIC
# MAGIC > 強く相関する特徴量が複数あると**多重共線性**の問題が発生する場合があります。

# COMMAND ----------

# pandasで相関行列を計算・可視化
pdf_clean = sdf_clean.toPandas()
corr_cols = ["area", "rooms", "age", "station_dist", "price"]
corr_matrix = pdf_clean[corr_cols].corr()

fig, ax = plt.subplots(figsize=(8, 6))
im = ax.imshow(corr_matrix, cmap="RdBu_r", vmin=-1, vmax=1)
ax.set_xticks(range(len(corr_cols)))
ax.set_yticks(range(len(corr_cols)))
ax.set_xticklabels(corr_cols, rotation=45, ha="right")
ax.set_yticklabels(corr_cols)

# 相関係数を表示
for i in range(len(corr_cols)):
    for j in range(len(corr_cols)):
        ax.text(j, i, f"{corr_matrix.iloc[i, j]:.2f}", ha="center", va="center")

plt.colorbar(im)
ax.set_title("特徴量の相関行列")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **Spark DataFrame** — データの読み込みと基本操作
# MAGIC 2. **欠損値処理** — Imputer を使った中央値補完
# MAGIC 3. **外れ値検出** — IQR法による外れ値の検出と除去
# MAGIC 4. **スケーリング** — StandardScaler による特徴量の標準化
# MAGIC 5. **エンコーディング** — StringIndexer / OneHotEncoder によるカテゴリ変数の変換
# MAGIC 6. **相関分析** — 特徴量間の関係性の可視化
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - `03_hyperparameter_tuning.py` でハイパーパラメータチューニングを学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (ML Associate):
# MAGIC > - **Data Processing (19%)**: summary()/describe()による統計量、欠損値の中央値/平均値/最頻値補完、外れ値検出
# MAGIC > - **Data Processing (19%)**: StringIndexer、OneHotEncoder、StandardScaler vs MinMaxScaler

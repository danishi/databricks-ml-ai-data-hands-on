# Databricks notebook source
# MAGIC %md
# MAGIC # scikit-learn による分類モデルの構築
# MAGIC
# MAGIC このノートブックでは、Databricks上でscikit-learnを使って分類モデルを構築する基本的な流れを学びます。
# MAGIC
# MAGIC ## 学べること
# MAGIC - Databricks上でのデータの読み込みと前処理
# MAGIC - scikit-learnによるモデル学習
# MAGIC - MLflowによる実験管理（自動ロギング）
# MAGIC - モデルの評価と可視化
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）のクラスターを使用してください
# MAGIC - クラスターサイズはシングルノード（Single Node）で十分です
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ライブラリのインポート
# MAGIC
# MAGIC Databricks Runtime ML にはscikit-learn、pandas、MLflowが事前にインストールされています。

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.datasets import load_wine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    ConfusionMatrixDisplay,
)
import mlflow
import matplotlib.pyplot as plt

print("ライブラリのインポートが完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. データの準備
# MAGIC
# MAGIC scikit-learnに付属のワインデータセットを使用します。
# MAGIC 3種類のワインを化学的特徴から分類するタスクです。

# COMMAND ----------

# ワインデータセットの読み込み
wine = load_wine()
df = pd.DataFrame(wine.data, columns=wine.feature_names)
df["target"] = wine.target
df["target_name"] = df["target"].map(
    {i: name for i, name in enumerate(wine.target_names)}
)

print(f"データ件数: {len(df)}")
print(f"特徴量の数: {len(wine.feature_names)}")
print(f"クラス: {list(wine.target_names)}")
display(df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### データの分布を確認

# COMMAND ----------

# クラスごとの件数
print("クラスごとの件数:")
print(df["target_name"].value_counts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 学習データとテストデータに分割

# COMMAND ----------

X = df[wine.feature_names]
y = df["target"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)

print(f"学習データ: {len(X_train)} 件")
print(f"テストデータ: {len(X_test)} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MLflow の自動ロギングを有効化
# MAGIC
# MAGIC `mlflow.sklearn.autolog()` を使うと、モデルのパラメータ・メトリクス・モデル本体が自動的に記録されます。

# COMMAND ----------

mlflow.sklearn.autolog()
print("MLflow 自動ロギングが有効になりました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. モデルの学習
# MAGIC
# MAGIC ランダムフォレスト分類器を使ってモデルを学習します。

# COMMAND ----------

with mlflow.start_run(run_name="wine_random_forest") as run:
    # モデルの定義と学習
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=5,
        random_state=42,
    )
    model.fit(X_train, y_train)

    # テストデータで予測
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    print(f"テストデータの正解率: {accuracy:.4f}")
    print(f"\nMLflow Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. モデルの評価
# MAGIC
# MAGIC 分類レポートと混同行列を確認します。

# COMMAND ----------

# 分類レポート
print("=== 分類レポート ===")
print(
    classification_report(
        y_test, y_pred, target_names=wine.target_names
    )
)

# COMMAND ----------

# 混同行列の可視化
fig, ax = plt.subplots(figsize=(8, 6))
ConfusionMatrixDisplay.from_predictions(
    y_test,
    y_pred,
    display_labels=wine.target_names,
    cmap="Blues",
    ax=ax,
)
ax.set_title("混同行列（Confusion Matrix）")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 特徴量の重要度

# COMMAND ----------

# 特徴量の重要度を可視化
importances = model.feature_importances_
indices = np.argsort(importances)[::-1]

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(
    range(len(indices)),
    importances[indices],
    align="center",
)
ax.set_yticks(range(len(indices)))
ax.set_yticklabels([wine.feature_names[i] for i in indices])
ax.set_xlabel("重要度")
ax.set_title("特徴量の重要度（Feature Importance）")
ax.invert_yaxis()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. MLflow の実験結果を確認
# MAGIC
# MAGIC 左サイドバーの **「エクスペリメント」** アイコンをクリックすると、今回の実行結果（パラメータ・メトリクス・モデル）を確認できます。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **データの準備** — scikit-learnのデータセットをpandas DataFrameに変換
# MAGIC 2. **モデルの学習** — RandomForestClassifierによる分類
# MAGIC 3. **MLflow連携** — 自動ロギングによる実験管理
# MAGIC 4. **モデルの評価** — 正解率・分類レポート・混同行列・特徴量重要度
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - ハイパーパラメータ（`n_estimators`, `max_depth`）を変えて精度の変化を観察してみましょう
# MAGIC - MLflowのUI上で複数の実行結果を比較してみましょう

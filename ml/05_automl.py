# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks AutoML
# MAGIC
# MAGIC **AutoML** は、機械学習モデルの構築を自動化する Databricks の機能です。
# MAGIC
# MAGIC **AutoML とは？**
# MAGIC > データを渡すだけで、前処理・特徴量エンジニアリング・モデル選択・ハイパーパラメータチューニングを
# MAGIC > **自動的に** 実行してくれます。結果はMLflowに記録され、生成されたノートブックも確認できます。
# MAGIC
# MAGIC ## 学べること
# MAGIC - AutoML の分類・回帰・時系列予測
# MAGIC - AutoML の実行と結果の確認
# MAGIC - 生成されたノートブックの活用
# MAGIC - AutoML と手動モデリングの使い分け
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）のクラスター
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. AutoML の概要
# MAGIC
# MAGIC | タスク | 説明 | 例 |
# MAGIC |---|---|---|
# MAGIC | **分類** | カテゴリを予測 | スパム判定、品種分類 |
# MAGIC | **回帰** | 数値を予測 | 価格予測、売上予測 |
# MAGIC | **時系列予測** | 未来の値を予測 | 需要予測、株価予測 |
# MAGIC
# MAGIC AutoML は以下を**自動的に**行います:
# MAGIC 1. データの前処理（欠損値補完、エンコーディング）
# MAGIC 2. 複数アルゴリズムの試行（ランダムフォレスト、XGBoost、LightGBMなど）
# MAGIC 3. ハイパーパラメータの最適化
# MAGIC 4. 全試行結果のMLflowへの記録
# MAGIC 5. データ探索ノートブックの自動生成
# MAGIC
# MAGIC > **試験ポイント: AutoML が「やること」と「やらないこと」**
# MAGIC >
# MAGIC > | やること（自動） | やらないこと（手動で必要） |
# MAGIC > |---|---|
# MAGIC > | 欠損値の自動補完 | **モデルのデプロイ（Model Serving）** |
# MAGIC > | 特徴量エンジニアリング | カスタム前処理 |
# MAGIC > | アルゴリズム選択 | ドメイン固有の特徴量設計 |
# MAGIC > | ハイパーパラメータ最適化 | モデルの解釈・説明 |
# MAGIC > | 評価指標の計算 | A/Bテストの設計 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. データの準備

# COMMAND ----------

from sklearn.datasets import load_wine
import pandas as pd

wine = load_wine()
df = pd.DataFrame(wine.data, columns=wine.feature_names)
df["target"] = wine.target

# Spark DataFrameに変換（AutoMLの入力）
train_sdf = spark.createDataFrame(df)
display(train_sdf.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. AutoML の実行（分類）
# MAGIC
# MAGIC `databricks.automl.classify()` を呼ぶだけで自動的にモデルが構築されます。
# MAGIC
# MAGIC | パラメータ | 説明 |
# MAGIC |---|---|
# MAGIC | `dataset` | 学習データ（Spark DataFrame） |
# MAGIC | `target_col` | 目的変数の列名 |
# MAGIC | `primary_metric` | 最適化する指標（accuracy, f1 など） |
# MAGIC | `timeout_minutes` | 最大実行時間（分） |
# MAGIC | `max_trials` | 最大試行回数 |

# COMMAND ----------

import databricks.automl as automl

# AutoMLで分類モデルを自動構築
summary = automl.classify(
    dataset=train_sdf,
    target_col="target",
    primary_metric="f1",
    timeout_minutes=5,
    max_trials=10,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. AutoML の結果を確認
# MAGIC
# MAGIC AutoML は実行結果として以下を返します:
# MAGIC - **最良モデル**: 最も性能が良かったモデル
# MAGIC - **MLflow実験**: 全試行の記録
# MAGIC - **生成ノートブック**: 各試行のコードが確認可能

# COMMAND ----------

# 最良のモデルの情報
print(f"最良のMLflow Run ID: {summary.best_trial.mlflow_run_id}")
print(f"最良のメトリック (F1): {summary.best_trial.metrics}")

# COMMAND ----------

# 最良モデルで予測してみる
import mlflow

best_model = mlflow.sklearn.load_model(f"runs:/{summary.best_trial.mlflow_run_id}/model")

sample = df[wine.feature_names].head(5)
predictions = best_model.predict(sample)
print("サンプル予測結果:")
for i, (pred, actual) in enumerate(zip(predictions, df["target"].head(5))):
    print(f"  サンプル{i+1}: 予測={pred}, 実際={actual}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 生成されたノートブックの活用
# MAGIC
# MAGIC AutoML は各試行の**Pythonコード**をノートブックとして自動生成します。
# MAGIC
# MAGIC ```
# MAGIC 自動生成ノートブックの場所:
# MAGIC   /Users/<あなたのユーザー名>/databricks_automl/<実験名>/
# MAGIC ```
# MAGIC
# MAGIC **活用方法**:
# MAGIC - 生成されたコードを読んで、前処理やモデル構築の手法を学ぶ
# MAGIC - コードをカスタマイズして、より良いモデルを作る
# MAGIC - チームメンバーと共有して、ベースラインとして活用する

# COMMAND ----------

# 生成されたノートブックのURLを表示
print("=== 生成されたノートブック ===")
print(f"データ探索ノートブック: {summary.output_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. AutoML の使い分け
# MAGIC
# MAGIC | 場面 | 推奨 |
# MAGIC |---|---|
# MAGIC | 初めてのデータでベースラインを作りたい | **AutoML** |
# MAGIC | 短時間で良いモデルを見つけたい | **AutoML** |
# MAGIC | モデルの細部をカスタマイズしたい | **手動** |
# MAGIC | 特殊な前処理が必要 | **手動**（＋AutoMLのコードを参考に） |
# MAGIC | 実験的に複数手法を比較したい | **AutoML**で候補を出し、手動で改良 |
# MAGIC
# MAGIC > **ベストプラクティス**: まずAutoMLでベースラインを作り、その生成コードを元にカスタマイズする。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC 1. **AutoML** — データを渡すだけで自動的にモデルを構築
# MAGIC 2. **結果の確認** — MLflow で全試行結果を比較
# MAGIC 3. **生成ノートブック** — 自動生成コードを学習・カスタマイズに活用
# MAGIC 4. **使い分け** — ベースライン構築にAutoML、カスタマイズは手動で
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - `06_feature_store.py` で Unity Catalog Feature Store を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (ML Associate):
# MAGIC > - **Databricks Machine Learning (38%)**: AutoML が自動で行うステップ、AutoML が行わないこと（デプロイ）
# MAGIC > - **Databricks Machine Learning (38%)**: 生成されたノートブックの活用法、評価指標（分類→F1/accuracy、回帰→RMSE）

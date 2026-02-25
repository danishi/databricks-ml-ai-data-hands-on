# Databricks notebook source
# MAGIC %md
# MAGIC # ハイパーパラメータチューニング
# MAGIC
# MAGIC モデルの精度を向上させるための**ハイパーパラメータチューニング**を学びます。
# MAGIC
# MAGIC **ハイパーパラメータとは？**
# MAGIC > モデルの学習前に設定するパラメータです（例: 決定木の深さ、学習率）。
# MAGIC > データから自動で決まるパラメータ（重み等）とは異なり、人が事前に決める必要があります。
# MAGIC
# MAGIC ### イメージ: ハイパーパラメータチューニングとは？
# MAGIC
# MAGIC ```
# MAGIC 料理のレシピに例えると...
# MAGIC
# MAGIC  ハイパーパラメータ = レシピの調整項目
# MAGIC    ├── 決定木の本数 (n_estimators) = 煮込み時間
# MAGIC    ├── 決定木の深さ (max_depth)    = 火の強さ
# MAGIC    └── min_samples_split           = 食材の切り方
# MAGIC
# MAGIC  チューニング = 最も美味しくなる設定を探すこと
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - 交差検証（Cross Validation）の仕組み
# MAGIC - グリッドサーチとランダムサーチ
# MAGIC - Hyperopt を使ったベイズ最適化
# MAGIC - MLflow との連携による実験管理
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）のクラスター
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. セットアップ

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.datasets import load_wine
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
import mlflow

wine = load_wine()
X = pd.DataFrame(wine.data, columns=wine.feature_names)
y = wine.target
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)
print(f"学習: {len(X_train)}件, テスト: {len(X_test)}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 交差検証（Cross Validation）
# MAGIC
# MAGIC **交差検証**はモデルの性能をより正確に評価する手法です。
# MAGIC
# MAGIC ```
# MAGIC データ全体: [A] [B] [C] [D] [E]
# MAGIC
# MAGIC Fold 1: テスト=[A], 学習=[B][C][D][E] → 精度: 0.95
# MAGIC Fold 2: テスト=[B], 学習=[A][C][D][E] → 精度: 0.93
# MAGIC Fold 3: テスト=[C], 学習=[A][B][D][E] → 精度: 0.96
# MAGIC Fold 4: テスト=[D], 学習=[A][B][C][E] → 精度: 0.94
# MAGIC Fold 5: テスト=[E], 学習=[A][B][C][D] → 精度: 0.95
# MAGIC
# MAGIC 平均精度: 0.946 ± 0.01
# MAGIC ```
# MAGIC
# MAGIC > **メリット**: 1回のtrain/test分割よりもデータの偏りに強く、安定した評価が可能です。
# MAGIC >
# MAGIC > **デメリット**: 5分割なら5回学習するため、**計算時間が5倍**かかります。
# MAGIC > データが大きい場合や試行回数が多い場合はトレードオフを考慮しましょう。
# MAGIC >
# MAGIC > **試験ポイント**: グリッドサーチ × 交差検証で学習されるモデル数は
# MAGIC > 「パラメータの組み合わせ数 × fold数」です。例: 9通り × 3fold = **27回**の学習。

# COMMAND ----------

model = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42)

# 5分割交差検証
cv_scores = cross_val_score(model, X_train, y_train, cv=5, scoring="accuracy")

print(f"各Foldの精度: {cv_scores}")
print(f"平均精度: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. グリッドサーチ
# MAGIC
# MAGIC **グリッドサーチ**は、パラメータの全組み合わせを試す方法です。
# MAGIC 確実に最良の組み合わせが見つかりますが、組み合わせ数が多いと時間がかかります。

# COMMAND ----------

from sklearn.model_selection import GridSearchCV

param_grid = {
    "n_estimators": [50, 100, 200],
    "max_depth": [3, 5, 10],
}

grid_search = GridSearchCV(
    RandomForestClassifier(random_state=42),
    param_grid,
    cv=3,
    scoring="accuracy",
    verbose=1,
)
grid_search.fit(X_train, y_train)

print(f"\n最良のパラメータ: {grid_search.best_params_}")
print(f"最良の交差検証精度: {grid_search.best_score_:.4f}")
print(f"テスト精度: {grid_search.score(X_test, y_test):.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Hyperopt によるベイズ最適化
# MAGIC
# MAGIC **Hyperopt** は Databricks Runtime ML に組み込まれた高度なチューニングライブラリです。
# MAGIC
# MAGIC | 方法 | 探索戦略 | 特徴 |
# MAGIC |---|---|---|
# MAGIC | グリッドサーチ | 全組み合わせ | 確実だが遅い |
# MAGIC | ランダムサーチ | ランダム | 速いが運次第 |
# MAGIC | **ベイズ最適化** | **過去の結果を参考に次を選ぶ** | **効率的に良い値を発見** |
# MAGIC
# MAGIC Hyperoptは**Tree-structured Parzen Estimator（TPE）** というアルゴリズムで、
# MAGIC 過去の試行結果を学習して次に試すパラメータを賢く選びます。
# MAGIC
# MAGIC > **初心者の方へ: Hyperopt のイメージ**
# MAGIC >
# MAGIC > グリッドサーチ = 地図の全マスを1つずつ調べる（確実だが遅い）
# MAGIC > Hyperopt     = 「あのあたりが良さそう」と推測して効率よく探す（賢い宝探し）
# MAGIC >
# MAGIC > **SparkTrials** を使うと、複数の試行をクラスタの複数ノードで**並列実行**できます。
# MAGIC > シングルノード環境では `Trials()` を使います（今回のハンズオンではこちら）。

# COMMAND ----------

from hyperopt import fmin, tpe, hp, Trials, STATUS_OK

# 探索空間の定義
search_space = {
    "n_estimators": hp.choice("n_estimators", [50, 100, 150, 200, 300]),
    "max_depth": hp.quniform("max_depth", 2, 15, 1),
    "min_samples_split": hp.quniform("min_samples_split", 2, 10, 1),
    "min_samples_leaf": hp.quniform("min_samples_leaf", 1, 5, 1),
}


def objective(params):
    """Hyperoptが最小化する目的関数（負の精度を返す）"""
    params["max_depth"] = int(params["max_depth"])
    params["min_samples_split"] = int(params["min_samples_split"])
    params["min_samples_leaf"] = int(params["min_samples_leaf"])

    model = RandomForestClassifier(**params, random_state=42)
    cv_scores = cross_val_score(model, X_train, y_train, cv=3, scoring="accuracy")

    # MLflowで各試行を記録
    with mlflow.start_run(nested=True):
        mlflow.log_params(params)
        mlflow.log_metric("cv_accuracy_mean", cv_scores.mean())
        mlflow.log_metric("cv_accuracy_std", cv_scores.std())

    # Hyperoptは最小化するので、負の精度を返す
    return {"loss": -cv_scores.mean(), "status": STATUS_OK}


# Hyperoptで探索を実行
with mlflow.start_run(run_name="hyperopt_tuning"):
    trials = Trials()
    best_params = fmin(
        fn=objective,
        space=search_space,
        algo=tpe.suggest,
        max_evals=20,       # 試行回数
        trials=trials,
    )

print(f"\n最良のパラメータ: {best_params}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 最良パラメータでモデルを再学習

# COMMAND ----------

# best_paramsを実際の値に変換
n_estimators_choices = [50, 100, 150, 200, 300]
final_params = {
    "n_estimators": n_estimators_choices[int(best_params["n_estimators"])],
    "max_depth": int(best_params["max_depth"]),
    "min_samples_split": int(best_params["min_samples_split"]),
    "min_samples_leaf": int(best_params["min_samples_leaf"]),
    "random_state": 42,
}

best_model = RandomForestClassifier(**final_params)
best_model.fit(X_train, y_train)

test_accuracy = accuracy_score(y_test, best_model.predict(X_test))
print(f"最良パラメータ: {final_params}")
print(f"テスト精度: {test_accuracy:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC 1. **交差検証** — train/testの偏りに左右されない安定した評価
# MAGIC 2. **グリッドサーチ** — 全組み合わせを網羅的に探索
# MAGIC 3. **Hyperopt** — ベイズ最適化による効率的なパラメータ探索
# MAGIC 4. **MLflow連携** — 各試行結果の自動記録と比較
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - `04_spark_ml_pipeline.py` で Spark ML のパイプラインを学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (ML Associate):
# MAGIC > - **Model Development (31%)**: 交差検証、グリッドサーチ、Hyperopt + SparkTrials
# MAGIC > - **Model Development (31%)**: 試行回数とモデル精度の関係、並列化の理解

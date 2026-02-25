# Databricks notebook source
# MAGIC %md
# MAGIC # scikit-learn による分類モデルの構築
# MAGIC
# MAGIC このノートブックでは、Databricks上でscikit-learnを使って**分類モデル**を構築する基本的な流れを学びます。
# MAGIC **機械学習が初めての方でも安心して取り組める**よう、各ステップを丁寧に解説しています。
# MAGIC
# MAGIC **分類モデルとは？**
# MAGIC > 入力データをあらかじめ決められたカテゴリ（クラス）に分けるモデルです。
# MAGIC > 例: メールをスパム/非スパムに分類、画像を犬/猫に分類、など。
# MAGIC
# MAGIC 今回は「ワインの化学成分データから、ワインの品種（3種類）を予測する」タスクに取り組みます。
# MAGIC
# MAGIC ### このノートブックで体験する「機械学習の基本ワークフロー」
# MAGIC
# MAGIC ```
# MAGIC ① データを用意する → ② 学習データとテストデータに分ける → ③ モデルを学習させる
# MAGIC                                                            ↓
# MAGIC ⑤ 結果を記録・管理する（MLflow）← ④ モデルの性能を評価する
# MAGIC ```
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
# MAGIC > **初心者の方へ**: 各セルのコードは**上から順番に実行**してください。
# MAGIC > セルの左上にある ▶ ボタンをクリックするか、`Shift + Enter` で実行できます。
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ライブラリのインポート
# MAGIC
# MAGIC まず、必要なライブラリを読み込みます。
# MAGIC
# MAGIC **Databricks Runtime ML** には以下のライブラリが**事前にインストール**されているため、`pip install` は不要です:
# MAGIC
# MAGIC | ライブラリ | 用途 |
# MAGIC |---|---|
# MAGIC | `pandas` | 表形式データの操作（Excel的なもの） |
# MAGIC | `numpy` | 数値計算（配列・行列の演算） |
# MAGIC | `scikit-learn` | 機械学習アルゴリズムの実装 |
# MAGIC | `mlflow` | 実験管理・モデル管理 |
# MAGIC | `matplotlib` | グラフ・図の描画 |

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
# MAGIC > **用語メモ: import とは？**
# MAGIC >
# MAGIC > Pythonでは、他の人が作った便利な機能（ライブラリ）を `import` で読み込んで使います。
# MAGIC > 料理でいえば「調理器具を棚から出してキッチンに並べる」イメージです。
# MAGIC > ここでは、データ操作用（pandas）、機械学習用（scikit-learn）、実験管理用（mlflow）などを読み込みました。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. データの準備
# MAGIC
# MAGIC scikit-learnに付属の**ワインデータセット**を使用します。
# MAGIC
# MAGIC このデータセットは、イタリアの同じ地域で栽培された **3種類のワイン** の化学分析結果です。
# MAGIC 各ワインについて **13個の化学的特徴量**（アルコール度数、色の濃さなど）が記録されています。
# MAGIC
# MAGIC **目標**: 化学的特徴量から、ワインの品種（class_0 / class_1 / class_2）を予測するモデルを作る

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### データの中身を見てみましょう
# MAGIC
# MAGIC `display()` はDatabricks独自の関数で、DataFrameを見やすい表形式で表示してくれます。
# MAGIC
# MAGIC 各行が1つのワインサンプル、各列が特徴量（alcohol: アルコール度数、color_intensity: 色の濃さ、など）です。

# COMMAND ----------

display(df.head(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### データの分布を確認
# MAGIC
# MAGIC 各クラスのデータ件数を確認します。クラスごとのデータ件数に大きな偏りがないか確認することは、
# MAGIC モデルの品質を保つために重要です。

# COMMAND ----------

# クラスごとの件数
print("クラスごとの件数:")
print(df["target_name"].value_counts())
print("\n→ 3クラスとも概ね同じくらいの件数なので、データの偏りは少ないです")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 学習データとテストデータに分割
# MAGIC
# MAGIC モデルの性能を正しく評価するために、データを**学習データ**と**テストデータ**に分けます。
# MAGIC
# MAGIC - **学習データ（70%）**: モデルがパターンを学ぶためのデータ
# MAGIC - **テストデータ（30%）**: 学習に使っていないデータでモデルの性能を評価
# MAGIC
# MAGIC > **なぜ分割が必要？**
# MAGIC > 学習データだけで評価すると、「カンニング」のような状態になり、本当の性能がわかりません。
# MAGIC > 未知のデータに対してどれくらい正確に予測できるかを確認するために、テストデータを別に用意します。
# MAGIC
# MAGIC `stratify=y` を指定すると、各クラスの比率が学習・テスト両方で同じになるよう分割されます。

# COMMAND ----------

X = df[wine.feature_names]  # 特徴量（13個の化学成分）
y = df["target"]             # 正解ラベル（ワインの品種: 0, 1, 2）

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.3, random_state=42, stratify=y
)

print(f"学習データ: {len(X_train)} 件")
print(f"テストデータ: {len(X_test)} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC > **試してみよう**: `test_size=0.3` を `0.2` や `0.5` に変えて再実行してみましょう。
# MAGIC > テストデータの割合を変えると、学習に使えるデータ量と評価の信頼性のバランスが変わります。
# MAGIC > 一般的には 0.2〜0.3（20%〜30%をテスト用）がよく使われます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MLflow の自動ロギングを有効化
# MAGIC
# MAGIC **MLflow** は、機械学習の実験管理ツールです。Databricksに組み込まれています。
# MAGIC
# MAGIC `mlflow.sklearn.autolog()` を使うと、以下が**自動的に記録**されます:
# MAGIC - モデルのパラメータ（設定値）
# MAGIC - 性能メトリクス（正解率など）
# MAGIC - 学習済みモデル本体
# MAGIC
# MAGIC > **メリット**: 手動でログを書く必要がなく、実験の再現性が自動的に確保されます。
# MAGIC > 後から「どの設定で一番良い結果だったか？」を簡単に比較できます。
# MAGIC
# MAGIC > **初心者の方へ**: 「なぜ実験記録が大切？」
# MAGIC > 機械学習では「パラメータを変えて何度も実験する」のが日常です。
# MAGIC > ノートに手書きで記録していると管理が大変ですが、MLflowが**自動的に全部記録**してくれます。

# COMMAND ----------

mlflow.sklearn.autolog()
print("MLflow 自動ロギングが有効になりました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. モデルの学習
# MAGIC
# MAGIC **ランダムフォレスト（Random Forest）** を使ってモデルを学習します。
# MAGIC
# MAGIC ランダムフォレストは「複数の決定木を組み合わせて多数決で予測する」手法です。
# MAGIC 初心者にも扱いやすく、多くのタスクで良い性能を発揮します。
# MAGIC
# MAGIC | パラメータ | 説明 | 今回の値 |
# MAGIC |---|---|---|
# MAGIC | `n_estimators` | 決定木の本数 | 100 |
# MAGIC | `max_depth` | 各決定木の最大深さ | 5 |
# MAGIC | `random_state` | 乱数シード（再現性のため） | 42 |
# MAGIC
# MAGIC > **用語メモ: random_state（乱数シード）とは？**
# MAGIC >
# MAGIC > ランダムフォレストは名前の通りランダムな要素を含むアルゴリズムです。
# MAGIC > `random_state=42` を指定すると、毎回同じランダムな結果が得られます（再現性の確保）。
# MAGIC > 42 という数字に特別な意味はなく、好きな数字でOKです。

# COMMAND ----------

with mlflow.start_run(run_name="wine_random_forest") as run:
    # モデルの定義と学習
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=5,
        random_state=42,
    )
    model.fit(X_train, y_train)  # 学習データでパターンを学ぶ

    # テストデータで予測
    y_pred = model.predict(X_test)  # 未知データに対する予測
    accuracy = accuracy_score(y_test, y_pred)

    print(f"テストデータの正解率: {accuracy:.4f}")
    print(f"→ {accuracy*100:.1f}% のワインを正しく分類できました")
    print(f"\nMLflow Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. モデルの評価
# MAGIC
# MAGIC 正解率だけでなく、**分類レポート**と**混同行列**でモデルの性能を詳しく確認します。
# MAGIC
# MAGIC ### 分類レポートの見方
# MAGIC - **precision（適合率）**: そのクラスだと予測したもののうち、実際に正解だった割合
# MAGIC - **recall（再現率）**: 実際にそのクラスのデータのうち、正しく予測できた割合
# MAGIC - **f1-score**: precision と recall のバランスを取った指標
# MAGIC - **support**: 各クラスのテストデータ件数

# COMMAND ----------

# 分類レポート
print("=== 分類レポート ===")
print(
    classification_report(
        y_test, y_pred, target_names=wine.target_names
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 混同行列（Confusion Matrix）
# MAGIC
# MAGIC 混同行列は「予測結果と正解の関係」を表にしたものです。
# MAGIC
# MAGIC - **対角線（左上→右下）** の値が大きいほど、正しく分類できています
# MAGIC - **対角線以外** の値は誤分類を示します（例: class_0 を class_1 と間違えた件数）

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
# MAGIC
# MAGIC ランダムフォレストは、各特徴量が予測にどれだけ貢献したかを計算できます。
# MAGIC
# MAGIC 重要度が高い特徴量は、ワインの品種を分類するのに特に役立った化学成分です。
# MAGIC これにより、「何がワインの品種を決める重要な要因なのか」がわかります。

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
# MAGIC > **確認ポイント**:
# MAGIC > - Parameters タブ: n_estimators=100, max_depth=5 などの設定値
# MAGIC > - Metrics タブ: 正解率などの性能指標
# MAGIC > - Artifacts タブ: 保存されたモデル本体
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **データの準備** — scikit-learnのデータセットをpandas DataFrameに変換
# MAGIC 2. **データの分割** — 学習用とテスト用にデータを分けて正しく評価する方法
# MAGIC 3. **モデルの学習** — RandomForestClassifier による分類モデルの構築
# MAGIC 4. **MLflow連携** — 自動ロギングによる実験管理
# MAGIC 5. **モデルの評価** — 正解率・分類レポート・混同行列・特徴量重要度
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - ハイパーパラメータ（`n_estimators`, `max_depth`）を変えて精度の変化を観察してみましょう
# MAGIC - MLflowのUI上で複数の実行結果を比較してみましょう
# MAGIC - **次のノートブック `02_eda_feature_engineering.py`** でデータの前処理を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (ML Associate):
# MAGIC > - **Data Processing (19%)**: データの準備、train/test分割
# MAGIC > - **Model Development (31%)**: scikit-learnによるモデル構築、評価指標（precision, recall, f1）
# MAGIC > - **Databricks Machine Learning (38%)**: MLflow自動ロギング、ML Runtime

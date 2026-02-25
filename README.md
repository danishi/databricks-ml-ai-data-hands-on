# Databricks ML / 生成AI ハンズオン

Databricks 上で機械学習（ML）と生成AI を体系的に学べるハンズオン教材です。

**Databricks 認定資格の試験範囲をカバー**:
- [Databricks Certified Machine Learning Associate](https://www.databricks.com/learn/certification/machine-learning-associate)
- [Databricks Certified Generative AI Engineer Associate](https://www.databricks.com/learn/certification/generative-ai-engineer-associate)

## 対象者

- Databricks をこれから使い始める方
- 機械学習や生成AI に興味があるエンジニア・データサイエンティスト
- Databricks 認定資格の取得を目指す方
- Python の基本的な知識がある方

> **初心者の方へ**: 各ノートブックには「用語メモ」「試してみよう」「認定試験との関連」といった
> 補足解説が豊富に含まれています。上から順番にセルを実行するだけで、概念の理解から実践まで一通り体験できます。

## 必要な環境

- Databricks ワークスペース（Community Edition でも一部可※）
- **Databricks Runtime ML**（例: 16.x ML）のクラスター
- クラスターサイズは **シングルノード（Single Node）** で十分です

> ※ Community Edition では Model Serving、Databricks Apps、Vector Search、Feature Store、AutoML は利用できません。
> これらの機能を試す場合は有償ワークスペースが必要です。

## 使い方

### 1. リポジトリを Databricks にクローン

1. Databricks ワークスペースの左サイドバーで **「ワークスペース」** を選択
2. 右上の **「追加」** → **「Git フォルダー」** をクリック
3. このリポジトリの URL を入力してクローン

### 2. クラスターの作成

1. 左サイドバーの **「コンピューティング」** を選択
2. **「クラスターを作成」** をクリック
3. 以下の設定を推奨:
   - クラスターモード: **シングルノード**
   - Databricks Runtime: **Runtime ML** を選択（例: 16.x ML）
   - ノードタイプ: 最小構成でOK

### 3. ノートブックの実行

クローンしたフォルダー内のノートブック（`.py` ファイル）を開き、上から順にセルを実行してください。

## コンテンツ

### ML（機械学習）

| # | ノートブック | 内容 | 試験範囲 |
|---|---|---|---|
| 1 | `ml/01_scikit-learn_classification.py` | scikit-learn による分類モデル構築、MLflow 自動ロギング | ML基礎, MLflow |
| 2 | `ml/02_eda_feature_engineering.py` | EDA、欠損値処理、外れ値検出、スケーリング、エンコーディング | データ処理 |
| 3 | `ml/03_hyperparameter_tuning.py` | 交差検証、グリッドサーチ、Hyperopt ベイズ最適化 | モデル開発 |
| 4 | `ml/04_spark_ml_pipeline.py` | Spark ML Pipeline、Pandas API on Spark、Pandas UDF | Spark ML |
| 5 | `ml/05_automl.py` | Databricks AutoML による自動モデル構築 | Databricks ML |
| 6 | `ml/06_feature_store.py` | Unity Catalog Feature Store、特徴量ルックアップ | Databricks ML |
| 7 | `ml/07_mlflow_experiment.py` | MLflow Tracking / Model Registry / エイリアス | Databricks ML |
| 8 | `ml/08_model_serving.py` | Model Serving エンドポイント、REST API 推論 | モデルデプロイ |
| 9 | `ml/09_databricks_app.py` | Databricks Apps によるワイン分類予測アプリのデプロイ | モデルデプロイ |
| 10 | `ml/10_cleanup.py` | リソースのクリーンアップ | — |

### GenAI（生成AI）

| # | ノートブック | 内容 | 試験範囲 |
|---|---|---|---|
| 1 | `genai/01_foundation_model_apis.py` | Foundation Model APIs、プロンプトエンジニアリング、Embedding | 基盤モデル設計 |
| 2 | `genai/02_rag_chat.py` | RAG の仕組み（検索 + 生成）、コサイン類似度 | RAG データ準備 |
| 3 | `genai/03_vector_search_rag.py` | Vector Search、チャンク分割、Delta Sync Index | RAG データ準備 |
| 4 | `genai/04_agents_tool_use.py` | プロンプトチェイニング、ツール使用、エージェント | マルチステージ推論 |
| 5 | `genai/05_evaluation_governance.py` | LLM-as-a-Judge、ガードレール、責任あるAI | 評価・ガバナンス |
| 6 | `genai/06_databricks_app.py` | Databricks Apps による RAG チャットボットのデプロイ | Databricks ツール |
| 7 | `genai/07_cleanup.py` | リソースのクリーンアップ | — |

### App（Databricks Apps）

| ファイル | 内容 | 対応ノートブック |
|---|---|---|
| `app/app.py` | Model Serving を呼び出す **ワイン分類予測アプリ**（Streamlit） | `ml/09_databricks_app.py` |
| `app_rag/app.py` | 社内FAQドキュメントに基づく **RAGチャットボット**（Streamlit） | `genai/06_databricks_app.py` |

## 認定資格の試験範囲マッピング

### Databricks Certified Machine Learning Associate

| 試験セクション | 比重 | 対応するノートブック |
|---|---|---|
| **Databricks Machine Learning** | 38% | 05 (AutoML), 06 (Feature Store), 07 (MLflow) |
| **Data Processing** | 19% | 01 (データ準備), 02 (EDA・特徴量エンジニアリング) |
| **Model Development** | 31% | 01 (scikit-learn), 03 (Hyperopt), 04 (Spark ML Pipeline) |
| **Model Deployment** | 12% | 08 (Model Serving), 09 (Databricks Apps) |

### Databricks Certified Generative AI Engineer Associate

| 試験セクション | 対応するノートブック |
|---|---|
| **Design Applications with Foundation Models** | 01 (Foundation Model APIs, プロンプトエンジニアリング) |
| **Data Preparation for RAG** | 02 (RAG基礎), 03 (Vector Search, チャンク分割) |
| **Build Multi-stage Reasoning Applications** | 04 (エージェント, ツール使用, チェイニング) |
| **Evaluate and Optimize** | 05 (LLM-as-a-Judge, 評価指標) |
| **Governance and Security** | 05 (ガードレール, PII検出, 責任あるAI) |
| **Databricks Tools** | 01 (Foundation Model APIs), 03 (Vector Search), 06 (Databricks Apps) |

## 推奨する実行順序

### ML コース

```
1. ml/01_scikit-learn_classification.py  ← モデルの学習・評価の基礎
2. ml/02_eda_feature_engineering.py      ← データの前処理
3. ml/03_hyperparameter_tuning.py        ← モデルの最適化
4. ml/04_spark_ml_pipeline.py            ← Spark MLによる分散処理
5. ml/05_automl.py                       ← Databricks AutoML
6. ml/06_feature_store.py                ← Feature Store
7. ml/07_mlflow_experiment.py            ← MLflow 実験管理
8. ml/08_model_serving.py               ← モデルをAPIとして公開
9. ml/09_databricks_app.py              ← Databricks Appsでワイン分類アプリをデプロイ
10. ml/10_cleanup.py                     ← リソースの削除（終了時）
```

### GenAI コース

```
1. genai/01_foundation_model_apis.py    ← LLMの基本操作・Embedding
2. genai/02_rag_chat.py                 ← RAGの仕組みを理解
3. genai/03_vector_search_rag.py        ← Vector Searchによる本格RAG
4. genai/04_agents_tool_use.py          ← エージェントとツール使用
5. genai/05_evaluation_governance.py    ← 評価とガバナンス
6. genai/06_databricks_app.py           ← Databricks AppsでRAGチャットアプリをデプロイ
7. genai/07_cleanup.py                  ← リソースの削除（終了時）
```

> ML コースと GenAI コースは独立しており、どちらから始めても構いません。

## Databricks App のデプロイ方法

`app/` または `app_rag/` ディレクトリの Streamlit アプリを Databricks Apps としてデプロイする手順:

1. 左サイドバーの **「コンピューティング」** → **「アプリ」** を選択
2. **「アプリの作成」** をクリック
3. アプリ名を入力（例: `wine-classifier-app` / `rag-chat-app`）
4. ソースコードのパスとして、クローンしたリポジトリ内の `app/` または `app_rag/` フォルダを指定
5. **「デプロイ」** を実行

デプロイ後、表示されるURLにアクセスするとアプリが開きます。

> 詳細な手順は各コースのノートブック（`ml/09_databricks_app.py`、`genai/06_databricks_app.py`）を参照してください。

## ディレクトリ構成

```
databricks-ai-ml-hands-on/
├── README.md
├── ml/                                        # 機械学習ノートブック
│   ├── 01_scikit-learn_classification.py        # モデルの学習・評価
│   ├── 02_eda_feature_engineering.py            # EDA・特徴量エンジニアリング
│   ├── 03_hyperparameter_tuning.py              # ハイパーパラメータチューニング
│   ├── 04_spark_ml_pipeline.py                  # Spark ML パイプライン
│   ├── 05_automl.py                             # Databricks AutoML
│   ├── 06_feature_store.py                      # Feature Store
│   ├── 07_mlflow_experiment.py                  # MLflow 実験管理
│   ├── 08_model_serving.py                      # モデルサービング
│   ├── 09_databricks_app.py                     # Databricks Apps デプロイ
│   └── 10_cleanup.py                            # クリーンアップ
├── genai/                                     # 生成AIノートブック
│   ├── 01_foundation_model_apis.py              # Foundation Model APIs
│   ├── 02_rag_chat.py                           # RAG（検索拡張生成）
│   ├── 03_vector_search_rag.py                  # Vector Search RAG
│   ├── 04_agents_tool_use.py                    # エージェントとツール使用
│   ├── 05_evaluation_governance.py              # 評価とガバナンス
│   ├── 06_databricks_app.py                     # Databricks Apps デプロイ
│   └── 07_cleanup.py                            # クリーンアップ
├── app/                                       # Databricks App: ワイン分類予測
│   ├── app.py
│   └── requirements.txt
└── app_rag/                                   # Databricks App: RAGチャット
    ├── app.py
    └── requirements.txt
```

## コスト目安

- **ML 01〜04**: シングルノードクラスターで各数分で完了
- **ML 05 (AutoML)**: タイムアウト設定に応じて数分〜10分程度
- **ML 06〜07**: シングルノードで数分
- **ML 08 (Model Serving)**: エンドポイント稼働時間に応じた課金（`scale_to_zero_enabled=True` で最適化）
- **GenAI 01〜05**: Foundation Model APIs は pay-per-token（少額）
- **GenAI 03 (Vector Search)**: Vector Search エンドポイント稼働時間に応じた課金
- **Databricks App**: アプリ稼働時間に応じた課金

> **重要**: ハンズオン終了後は必ずクリーンアップノートブック（`ml/10_cleanup.py`、`genai/07_cleanup.py`）を実行してください。

## ライセンス

MIT License

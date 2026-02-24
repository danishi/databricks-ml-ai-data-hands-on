# Databricks ML / 生成AI ハンズオン

Databricks 上で機械学習（ML）と生成AI を低コストで試せる初心者向けハンズオン教材です。

## 対象者

- Databricks をこれから使い始める方
- 機械学習や生成AI に興味があるエンジニア・データサイエンティスト
- Python の基本的な知識がある方

## 必要な環境

- Databricks ワークスペース（Community Edition でも可※）
- **Databricks Runtime ML**（例: 16.x ML）のクラスター
- クラスターサイズは **シングルノード（Single Node）** で十分です

> ※ Community Edition では Model Serving や Databricks Apps は利用できません。
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

| # | ノートブック | 内容 |
|---|---|---|
| 1 | `ml/01_scikit-learn_classification.py` | scikit-learn でワインの分類モデルを構築。MLflow で実験管理。 |
| 2 | `ml/02_model_serving.py` | 学習済みモデルを Model Serving で REST API として公開。Unity Catalog へのモデル登録。 |
| 3 | `ml/03_cleanup.py` | ハンズオンで作成したリソース（エンドポイント・モデル）のクリーンアップ。 |

### GenAI（生成AI）

| # | ノートブック | 内容 |
|---|---|---|
| 1 | `genai/01_foundation_model_apis.py` | Foundation Model APIs で LLM を呼び出し。感情分析・構造化出力・Embedding など。 |
| 2 | `genai/02_rag_chat.py` | RAG（検索拡張生成）の仕組みをステップバイステップで学習。ドキュメント検索＋LLM回答生成。 |
| 3 | `genai/03_cleanup.py` | 生成AIハンズオンのリソースクリーンアップ。 |

### App（Databricks Apps）

| ディレクトリ | 内容 |
|---|---|
| `app/` | Model Serving エンドポイントを呼び出す **ワイン分類予測アプリ**（Streamlit）。 |
| `app_rag/` | 社内FAQドキュメントに基づく **RAGチャットボット**（Streamlit）。 |

## 推奨する実行順序

### ML コース

```
1. ml/01_scikit-learn_classification.py  ← モデルの学習・評価
2. ml/02_model_serving.py               ← モデルをAPIとして公開
3. app/ をDatabricks Appsとしてデプロイ    ← ブラウザからモデルを呼び出すUI
4. ml/03_cleanup.py                     ← リソースの削除（終了時）
```

### GenAI コース

```
1. genai/01_foundation_model_apis.py    ← LLMの基本操作・Embedding
2. genai/02_rag_chat.py                 ← RAGの仕組みを理解
3. app_rag/ をDatabricks Appsとしてデプロイ ← RAGチャットUI
4. genai/03_cleanup.py                  ← リソースの削除（終了時）
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

## ディレクトリ構成

```
databricks-ai-ml-hands-on/
├── README.md
├── ml/                                    # 機械学習ノートブック
│   ├── 01_scikit-learn_classification.py    # モデルの学習・評価
│   ├── 02_model_serving.py                  # モデルサービング
│   └── 03_cleanup.py                        # リソースのクリーンアップ
├── genai/                                 # 生成AIノートブック
│   ├── 01_foundation_model_apis.py          # Foundation Model APIs
│   ├── 02_rag_chat.py                       # RAG（検索拡張生成）
│   └── 03_cleanup.py                        # リソースのクリーンアップ
├── app/                                   # Databricks App: ワイン分類予測
│   ├── app.py
│   └── requirements.txt
└── app_rag/                               # Databricks App: RAGチャット
    ├── app.py
    └── requirements.txt
```

## コスト目安

- **ML 01（分類モデル）**: シングルノードクラスターで数分で完了します
- **ML 02（モデルサービング）**: Model Serving エンドポイントの稼働時間に応じた課金。`scale_to_zero_enabled=True` 設定により、未使用時の課金を抑えられます
- **GenAI 01〜02（LLM・RAG）**: Foundation Model APIs は pay-per-token のため、このハンズオンの範囲では少額で収まります
- **Databricks App**: アプリの稼働時間に応じた課金。不要になったら削除してください

> **重要**: ハンズオン終了後は必ずクリーンアップノートブック（`ml/03_cleanup.py`、`genai/03_cleanup.py`）を実行し、
> エンドポイントとアプリを削除してください。

## ライセンス

MIT License

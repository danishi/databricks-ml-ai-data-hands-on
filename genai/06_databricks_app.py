# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Apps: RAG チャットボットのデプロイ
# MAGIC
# MAGIC このノートブックでは、これまでのノートブックで学んだ RAG（検索拡張生成）の仕組みを使った
# MAGIC **チャットボットアプリ** を **Databricks Apps** としてデプロイする方法を学びます。
# MAGIC
# MAGIC ## Databricks Apps とは？
# MAGIC
# MAGIC > **Databricks Apps** は、Streamlit などのフレームワークで作った Web アプリを
# MAGIC > Databricks ワークスペース内でホスティングできる機能です。
# MAGIC > アプリから Foundation Model APIs を **セキュアに** 呼び出すことができます。
# MAGIC
# MAGIC ```
# MAGIC 従来の方法:
# MAGIC   Web アプリ → 外部サーバーにデプロイ → Databricks API を認証付きで呼び出し
# MAGIC                (AWS, GCP 等が必要)      (トークン管理が面倒)
# MAGIC
# MAGIC Databricks Apps:
# MAGIC   Web アプリ → Databricks にデプロイ → 認証は自動 → すぐに使える！
# MAGIC                (追加インフラ不要)     (WorkspaceClient が自動認証)
# MAGIC ```
# MAGIC
# MAGIC ## このノートブックで学べること
# MAGIC - Databricks Apps の概要とメリット
# MAGIC - RAG チャットボットアプリのコード解説
# MAGIC - デプロイの手順
# MAGIC - アプリの動作確認とカスタマイズ
# MAGIC
# MAGIC ## 前提条件
# MAGIC - 有償の Databricks ワークスペース（Community Edition では利用不可）
# MAGIC - Foundation Model APIs が有効であること
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Databricks Apps の概要
# MAGIC
# MAGIC ### Databricks Apps でできること
# MAGIC
# MAGIC | 機能 | 説明 |
# MAGIC |---|---|
# MAGIC | **Streamlit アプリのホスティング** | Python だけでインタラクティブな Web アプリを公開 |
# MAGIC | **自動認証** | WorkspaceClient が自動的に認証（トークン管理不要） |
# MAGIC | **Foundation Model APIs 連携** | LLM を活用したアプリを構築 |
# MAGIC | **アクセス制御** | ワークスペースのユーザー管理と連動 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. アプリのファイル構成
# MAGIC
# MAGIC Databricks Apps に必要なファイルは最低 **2つ** だけです:
# MAGIC
# MAGIC ```
# MAGIC app_rag/                      ← デプロイ時に指定するフォルダ
# MAGIC ├── app.py                    ← メインのアプリコード（必須）
# MAGIC └── requirements.txt          ← 使用するライブラリ一覧（必須）
# MAGIC ```
# MAGIC
# MAGIC ### requirements.txt の内容
# MAGIC
# MAGIC ```
# MAGIC databricks-sdk     ← Databricks API を呼ぶために必須
# MAGIC openai              ← Foundation Model APIs を OpenAI 互換で呼び出し
# MAGIC streamlit           ← Web UI フレームワーク
# MAGIC numpy               ← コサイン類似度の計算
# MAGIC ```
# MAGIC
# MAGIC > **初心者の方へ**: `requirements.txt` は「このアプリが動くのに必要なライブラリの一覧」です。
# MAGIC > Databricks Apps がデプロイ時に自動的にインストールしてくれます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. RAG チャットボットアプリのコード解説
# MAGIC
# MAGIC `app_rag/app.py` の主要な部分を見ていきましょう。
# MAGIC
# MAGIC ### アプリの全体像（RAG パイプライン）
# MAGIC
# MAGIC ```
# MAGIC ユーザーが質問を入力
# MAGIC      ↓
# MAGIC ① 質問をベクトル（数値配列）に変換（Embedding）
# MAGIC      ↓
# MAGIC ② FAQ ドキュメントとのコサイン類似度を計算し、関連する FAQ を検索
# MAGIC      ↓
# MAGIC ③ 検索結果 + 質問 を LLM に渡して回答を生成（Generation）
# MAGIC      ↓
# MAGIC 回答 + 参照元ドキュメントを表示
# MAGIC ```
# MAGIC
# MAGIC > `genai/02_rag_chat.py` で学んだ RAG の仕組みが、そのままアプリに組み込まれています。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks との接続（自動認証）
# MAGIC
# MAGIC Databricks Apps 上では、`WorkspaceClient()` から自動的にトークンを取得し、
# MAGIC Foundation Model APIs に接続します。
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from openai import OpenAI
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC client = OpenAI(
# MAGIC     api_key=w.config.token,
# MAGIC     base_url=f"{w.config.host}/serving-endpoints",
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Embedding による検索
# MAGIC
# MAGIC ```python
# MAGIC # ドキュメントと質問をベクトル化して類似度で検索
# MAGIC response = client.embeddings.create(
# MAGIC     model="databricks-gte-large-en",
# MAGIC     input=[query],
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### LLM による回答生成
# MAGIC
# MAGIC ```python
# MAGIC response = client.chat.completions.create(
# MAGIC     model="databricks-meta-llama-3-3-70b-instruct",
# MAGIC     messages=[
# MAGIC         {"role": "system", "content": system_prompt},
# MAGIC         {"role": "user", "content": f"参考情報:\n{context}\n\n質問:\n{query}"},
# MAGIC     ],
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Streamlit のチャット UI
# MAGIC
# MAGIC | コンポーネント | 用途 | コード例 |
# MAGIC |---|---|---|
# MAGIC | `st.chat_input()` | チャット入力 | `msg = st.chat_input("質問")` |
# MAGIC | `st.chat_message()` | メッセージ表示 | `with st.chat_message("user"):` |
# MAGIC | `st.sidebar` | サイドバー | `with st.sidebar:` |
# MAGIC | `st.expander()` | 折りたたみ | `with st.expander("詳細"):` |
# MAGIC | `st.spinner()` | 読み込み中表示 | `with st.spinner("処理中..."):` |
# MAGIC
# MAGIC > **詳しくは**: [Streamlit 公式ドキュメント](https://docs.streamlit.io/) を参照してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. アプリのコードを確認
# MAGIC
# MAGIC 実際のアプリコードを確認してみましょう。

# COMMAND ----------

# app_rag/app.py の内容を表示
with open("../app_rag/app.py", "r") as f:
    print(f.read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. デプロイ手順
# MAGIC
# MAGIC ### ステップ 1: Foundation Model APIs の確認
# MAGIC
# MAGIC Foundation Model APIs は pay-per-token のため、エンドポイントの事前作成は不要です。
# MAGIC 以下のセルで接続を確認します。

# COMMAND ----------

import os
from openai import OpenAI

token = os.environ.get(
    "DATABRICKS_TOKEN",
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
)
host = os.environ.get(
    "DATABRICKS_HOST",
    f"https://{spark.conf.get('spark.databricks.workspaceUrl')}",
)

client = OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")

try:
    response = client.chat.completions.create(
        model="databricks-meta-llama-3-3-70b-instruct",
        messages=[{"role": "user", "content": "Hello"}],
        max_tokens=16,
    )
    print("Foundation Model APIs の接続確認: OK")
    print(f"→ レスポンス: {response.choices[0].message.content}")
except Exception as e:
    print(f"Foundation Model APIs の接続確認: NG")
    print(f"→ エラー: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ステップ 2: Databricks Apps でアプリを作成・デプロイ
# MAGIC
# MAGIC 1. 左サイドバーの **「コンピューティング」** をクリック
# MAGIC 2. **「アプリ」** タブを選択
# MAGIC 3. **「アプリの作成」** ボタンをクリック
# MAGIC 4. 以下を設定:
# MAGIC    - **アプリ名**: `rag-chat-app`（任意の名前）
# MAGIC    - **説明**: 社内FAQ RAGチャットボット（任意）
# MAGIC 5. **「作成」** をクリック
# MAGIC 6. アプリの設定画面で **「ソースコード」** セクションを確認
# MAGIC 7. **ソースコードのパス** に、クローンしたリポジトリ内の `app_rag/` フォルダを指定
# MAGIC    - 例: `/Workspace/Users/<ユーザー名>/databricks-ai-ml-hands-on/app_rag`
# MAGIC 8. **「デプロイ」** ボタンをクリック
# MAGIC 9. デプロイが完了するまで数分待ちます
# MAGIC
# MAGIC ### ステップ 3: アプリにアクセス
# MAGIC
# MAGIC デプロイ完了後、表示される **URL** をクリックするとアプリが開きます。
# MAGIC
# MAGIC > **トラブルシューティング**:
# MAGIC > - 画面が表示されない → デプロイが完了するまで数分待つ
# MAGIC > - エラーが表示される → Foundation Model APIs が有効か確認
# MAGIC > - 回答が返らない → ワークスペースで Foundation Model APIs が利用可能か確認

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. アプリのカスタマイズ例
# MAGIC
# MAGIC ### 例: FAQ ドキュメントを追加する
# MAGIC
# MAGIC `app_rag/app.py` の `DOCUMENTS` リストに新しい辞書を追加すると、
# MAGIC FAQ の対象範囲が広がります。
# MAGIC
# MAGIC ```python
# MAGIC # 例: 新しいFAQドキュメントを追加
# MAGIC {
# MAGIC     "id": 9,
# MAGIC     "title": "新しいトピック",
# MAGIC     "content": "ここにドキュメントの内容を記載します...",
# MAGIC },
# MAGIC ```
# MAGIC
# MAGIC > **変更後の再デプロイ**: ソースコードを変更した後、
# MAGIC > アプリの設定画面で **「再デプロイ」** をクリックすると更新が反映されます。
# MAGIC
# MAGIC > **発展**: 実際の業務では、アプリ内のハードコードではなく
# MAGIC > `03_vector_search_rag.py` で学んだ Vector Search を使って
# MAGIC > 外部データベースからドキュメントを取得する構成にします。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. コストと注意事項
# MAGIC
# MAGIC | 項目 | コスト | 備考 |
# MAGIC |---|---|---|
# MAGIC | **Databricks Apps** | アプリ稼働時間に応じた課金 | 使わないときは停止推奨 |
# MAGIC | **Foundation Model APIs** | 従量課金（pay-per-token） | 少額（数円/リクエスト程度） |
# MAGIC
# MAGIC > **重要**: ハンズオン終了後は以下を忘れずに実行してください:
# MAGIC > 1. Databricks Apps を停止・削除（「コンピューティング」→「アプリ」から）
# MAGIC > 2. Vector Search のリソースを削除（`genai/07_cleanup.py` を実行）
# MAGIC > 3. クラスターを停止

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **Databricks Apps** — Streamlit アプリをワークスペース内でホスティング
# MAGIC 2. **RAG アプリの構成** — Embedding + 類似度検索 + LLM 生成のパイプライン
# MAGIC 3. **自動認証** — WorkspaceClient で Foundation Model APIs を簡単に呼び出し
# MAGIC 4. **デプロイ** — UI からフォルダを指定するだけの簡単操作
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - アプリに FAQ ドキュメントを追加して、対応範囲を広げてみましょう
# MAGIC - ハンズオン完了後は **`07_cleanup.py`** でリソースをクリーンアップしてください
# MAGIC
# MAGIC > **認定試験との関連** (GenAI Engineer Associate):
# MAGIC > - **Databricks Tools**: Databricks Apps による生成AIアプリケーションのデプロイ
# MAGIC > - **Design Applications with Foundation Models**: Foundation Model APIs を活用したアプリ構築

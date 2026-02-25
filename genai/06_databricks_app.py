# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Apps: RAG チャットボットのデプロイ
# MAGIC
# MAGIC このノートブックでは、`03_vector_search_rag.py` で作成した **Vector Search Index** を活用した
# MAGIC **RAG チャットボットアプリ** を **Databricks Apps** としてデプロイする方法を学びます。
# MAGIC
# MAGIC ## Databricks Apps とは？
# MAGIC
# MAGIC > **Databricks Apps** は、Streamlit・Gradio・Dash などのフレームワークで作った Web アプリを
# MAGIC > Databricks ワークスペース内でホスティングできる機能です。
# MAGIC > アプリから Foundation Model APIs や Vector Search を **セキュアに** 呼び出すことができます。
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
# MAGIC - Vector Search を活用した RAG チャットボットアプリのコード解説
# MAGIC - デプロイの手順
# MAGIC - アプリの動作確認とカスタマイズ
# MAGIC
# MAGIC ## 前提条件
# MAGIC - 有償の Databricks ワークスペース（Community Edition では利用不可）
# MAGIC - Foundation Model APIs が有効であること
# MAGIC - `genai/03_vector_search_rag.py` を実行済み（Vector Search Index が作成済みであること）
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
# MAGIC | **Web アプリのホスティング** | Streamlit / Gradio / Dash / Flask をサポート |
# MAGIC | **自動認証** | WorkspaceClient が自動的に認証（トークン管理不要） |
# MAGIC | **Foundation Model APIs 連携** | LLM・Embedding を活用したアプリを構築 |
# MAGIC | **Vector Search 連携** | ベクトル検索を組み込んだ RAG アプリを構築 |
# MAGIC | **アクセス制御** | ワークスペースのユーザー管理と連動 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. アプリのファイル構成
# MAGIC
# MAGIC Databricks Apps に必要なファイル構成です:
# MAGIC
# MAGIC ```
# MAGIC app_rag/                      ← デプロイ時に指定するフォルダ
# MAGIC ├── app.py                    ← メインのアプリコード
# MAGIC ├── app.yaml                  ← アプリの実行設定（エントリポイント等）
# MAGIC └── requirements.txt          ← 使用するライブラリ一覧
# MAGIC ```
# MAGIC
# MAGIC ### app.yaml の内容
# MAGIC
# MAGIC `app.yaml` はアプリの起動コマンドや環境変数を定義するファイルです。
# MAGIC
# MAGIC ```yaml
# MAGIC command: ['streamlit', 'run', 'app.py']
# MAGIC env:
# MAGIC   - name: 'STREAMLIT_GATHER_USAGE_STATS'
# MAGIC     value: 'false'
# MAGIC ```
# MAGIC
# MAGIC ### requirements.txt の内容
# MAGIC
# MAGIC ```
# MAGIC databricks-sdk     ← Databricks API（Vector Search 含む）を呼ぶために必須
# MAGIC openai              ← Foundation Model APIs を OpenAI 互換で呼び出し
# MAGIC streamlit           ← Web UI フレームワーク
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
# MAGIC ① Databricks Vector Search で関連ドキュメントを検索
# MAGIC      ↓
# MAGIC ② 検索結果 + 質問 を LLM に渡して回答を生成（Generation）
# MAGIC      ↓
# MAGIC 回答 + 参照元ドキュメントを表示
# MAGIC ```
# MAGIC
# MAGIC > `genai/03_vector_search_rag.py` で作成した Vector Search Index をアプリから直接呼び出します。
# MAGIC > `02_rag_chat.py` のメモリ上のベクトル検索と異なり、大量ドキュメントにもスケールします。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks との接続（自動認証）
# MAGIC
# MAGIC Databricks Apps 上では、`WorkspaceClient()` を引数なしで呼ぶだけで自動認証されます。
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from openai import OpenAI
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC openai_client = OpenAI(
# MAGIC     api_key=w.config.token,
# MAGIC     base_url=f"{w.config.host}/serving-endpoints",
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Vector Search による検索
# MAGIC
# MAGIC ```python
# MAGIC # Vector Search Index に対してテキストで直接検索
# MAGIC results = w.vector_search_indexes.query_index(
# MAGIC     index_name="main.default.rag_documents_index",
# MAGIC     columns=["chunk_id", "title", "content"],
# MAGIC     query_text=query,
# MAGIC     num_results=3,
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### LLM による回答生成
# MAGIC
# MAGIC ```python
# MAGIC response = openai_client.chat.completions.create(
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
# MAGIC ### ステップ 1: Vector Search Index と Foundation Model APIs の確認
# MAGIC
# MAGIC `genai/03_vector_search_rag.py` で作成した Vector Search Index が利用可能であること、
# MAGIC および Foundation Model APIs への接続を確認します。

# COMMAND ----------

# MAGIC %pip install -q openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
from openai import OpenAI
from databricks.sdk import WorkspaceClient

token = os.environ.get(
    "DATABRICKS_TOKEN",
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get(),
)
host = os.environ.get(
    "DATABRICKS_HOST",
    f"https://{spark.conf.get('spark.databricks.workspaceUrl')}",
)

client = OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")
w = WorkspaceClient()

# Foundation Model APIs の接続確認
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

# Vector Search Index の確認
index_name = "main.default.rag_documents_index"
try:
    idx = w.vector_search_indexes.get_index(index_name)
    print(f"Vector Search Index の確認: OK")
    print(f"→ インデックス名: {index_name}")
    if idx.status:
        print(f"→ 準備状態: {'Ready' if idx.status.ready else 'Not Ready'}")
except Exception as e:
    print(f"Vector Search Index の確認: NG")
    print(f"→ エラー: {e}")
    print("→ 先に genai/03_vector_search_rag.py を実行して Vector Search Index を作成してください。")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ステップ 2: Databricks Apps でアプリを作成
# MAGIC
# MAGIC 1. 左サイドバーの **「+ 新規」** をクリック
# MAGIC 2. メニューから **「アプリ」** を選択
# MAGIC 3. **「カスタムアプリを作成」** をクリック
# MAGIC 4. 以下を設定:
# MAGIC    - **アプリ名**: `rag-chat-app`（任意の名前、小文字・数字・ハイフンのみ）
# MAGIC    - **説明**: 社内FAQ RAGチャットボット（任意）
# MAGIC 5. **「次: 設定」** をクリック（または「アプリの作成」で詳細設定をスキップ）
# MAGIC 6. 必要に応じて以下を設定:
# MAGIC    - **アプリのリソース**: アプリがアクセスする Databricks リソース（サービングエンドポイント等）を追加
# MAGIC    - **コンピュートサイズ**: アプリの CPU・メモリを設定
# MAGIC 7. **「アプリの作成」** をクリック
# MAGIC
# MAGIC ### ステップ 3: アプリをデプロイ
# MAGIC
# MAGIC 1. アプリの詳細画面で **「デプロイ」** ボタンをクリック
# MAGIC 2. ワークスペース内の `app_rag/` フォルダを選択
# MAGIC    - 例: `/Workspace/Users/<ユーザー名>/databricks-ai-ml-hands-on/app_rag`
# MAGIC 3. **「選択」** → **「デプロイ」** をクリック
# MAGIC 4. デプロイが完了するまで数分待ちます
# MAGIC
# MAGIC ### ステップ 4: アプリにアクセス
# MAGIC
# MAGIC デプロイ完了後、アプリ詳細画面に表示される **URL** をクリックするとアプリが開きます。
# MAGIC
# MAGIC > **トラブルシューティング**:
# MAGIC > - 画面が表示されない → デプロイが完了するまで数分待つ
# MAGIC > - Vector Search のエラー → `genai/03_vector_search_rag.py` を実行済みか確認
# MAGIC > - 回答が返らない → Foundation Model APIs が有効か確認
# MAGIC > - デプロイ失敗 → アプリの詳細画面のログを確認

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. アプリのカスタマイズ例
# MAGIC
# MAGIC ### 例: FAQ ドキュメントを追加する
# MAGIC
# MAGIC このアプリは `genai/03_vector_search_rag.py` で作成した Vector Search Index を使っています。
# MAGIC ドキュメントを追加するには、ソーステーブル（`main.default.rag_documents`）にデータを追加し、
# MAGIC インデックスを同期します。
# MAGIC
# MAGIC ```python
# MAGIC # 新しいドキュメントを Delta Table に追加
# MAGIC new_docs = [{"chunk_id": "new_001", "doc_id": "policy_new", "title": "新しいトピック", "content": "..."}]
# MAGIC spark.createDataFrame(new_docs).write.mode("append").saveAsTable("main.default.rag_documents")
# MAGIC
# MAGIC # Vector Search Index を同期
# MAGIC w.vector_search_indexes.sync_index(index_name="main.default.rag_documents_index")
# MAGIC ```
# MAGIC
# MAGIC > **再デプロイ不要**: ドキュメントの追加は Delta Table と Vector Search Index の更新のみで、
# MAGIC > アプリの再デプロイは不要です。アプリは常に最新のインデックスから検索します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. コストと注意事項
# MAGIC
# MAGIC | 項目 | コスト | 備考 |
# MAGIC |---|---|---|
# MAGIC | **Databricks Apps** | アプリ稼働時間に応じた課金 | 使わないときは停止推奨 |
# MAGIC | **Foundation Model APIs** | 従量課金（pay-per-token） | 少額（数円/リクエスト程度） |
# MAGIC | **Vector Search** | エンドポイント稼働時間 | 使わないときは停止推奨 |
# MAGIC
# MAGIC > **重要**: ハンズオン終了後は以下を忘れずに実行してください:
# MAGIC > 1. Databricks Apps を停止・削除（サイドバー「コンピューティング」→「アプリ」タブから）
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
# MAGIC 2. **Vector Search 連携** — `03_vector_search_rag.py` で作成したインデックスをアプリから検索
# MAGIC 3. **自動認証** — WorkspaceClient で Foundation Model APIs と Vector Search を簡単に呼び出し
# MAGIC 4. **デプロイ** — サイドバー「+ 新規」→「アプリ」からフォルダを指定するだけの簡単操作
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - Delta Table にドキュメントを追加して、RAG の対応範囲を広げてみましょう
# MAGIC - ハンズオン完了後は **`07_cleanup.py`** でリソースをクリーンアップしてください
# MAGIC
# MAGIC > **認定試験との関連** (GenAI Engineer Associate):
# MAGIC > - **Databricks Tools**: Databricks Apps による生成AIアプリケーションのデプロイ
# MAGIC > - **Design Applications with Foundation Models**: Foundation Model APIs + Vector Search を活用したアプリ構築

# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Apps デプロイガイド
# MAGIC
# MAGIC このノートブックでは、**Databricks Apps** を使ってWebアプリケーションを
# MAGIC Databricks ワークスペース上にデプロイする方法をステップバイステップで学びます。
# MAGIC
# MAGIC ## Databricks Apps とは？
# MAGIC
# MAGIC > **Databricks Apps** は、Streamlit などのフレームワークで作ったWebアプリを
# MAGIC > Databricks ワークスペース内でホスティングできる機能です。
# MAGIC > アプリから Model Serving エンドポイントや Foundation Model APIs を
# MAGIC > **セキュアに**呼び出すことができます。
# MAGIC
# MAGIC ```
# MAGIC 従来の方法:
# MAGIC   Webアプリ → 外部サーバーにデプロイ → Databricks API を認証付きで呼び出し
# MAGIC                (AWS, GCP等が必要)      (トークン管理が面倒)
# MAGIC
# MAGIC Databricks Apps:
# MAGIC   Webアプリ → Databricks にデプロイ → 認証は自動 → すぐに使える！
# MAGIC                (追加インフラ不要)     (WorkspaceClient が自動認証)
# MAGIC ```
# MAGIC
# MAGIC ## このガイドで学べること
# MAGIC - Databricks Apps の概要とメリット
# MAGIC - アプリのファイル構成
# MAGIC - デプロイの手順（スクリーンショット付き解説）
# MAGIC - アプリの動作確認とトラブルシューティング
# MAGIC
# MAGIC ## 前提条件
# MAGIC - 有償の Databricks ワークスペース（Community Edition では利用不可）
# MAGIC - `ml/08_model_serving.py` を実行済み（ワイン分類アプリの場合）
# MAGIC - Foundation Model APIs が有効（RAGチャットアプリの場合）
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
# MAGIC | **Streamlit アプリのホスティング** | Pythonだけでインタラクティブなwebアプリを公開 |
# MAGIC | **自動認証** | WorkspaceClient が自動的に認証（トークン管理不要） |
# MAGIC | **Model Serving 連携** | エンドポイントをセキュアに呼び出し |
# MAGIC | **Foundation Model APIs 連携** | LLM を活用したアプリを構築 |
# MAGIC | **アクセス制御** | ワークスペースのユーザー管理と連動 |
# MAGIC
# MAGIC ### このリポジトリに含まれる2つのアプリ
# MAGIC
# MAGIC | アプリ | ディレクトリ | 内容 | 必要な事前準備 |
# MAGIC |---|---|---|---|
# MAGIC | **ワイン分類予測** | `app/` | スライダーで成分入力→品種予測 | `ml/08_model_serving.py` の実行 |
# MAGIC | **RAGチャット** | `app_rag/` | 社内FAQに基づくAIチャット | Foundation Model APIs の有効化 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. アプリのファイル構成
# MAGIC
# MAGIC Databricks Apps に必要なファイルは最低**2つ**だけです:
# MAGIC
# MAGIC ```
# MAGIC app/                          ← デプロイ時に指定するフォルダ
# MAGIC ├── app.py                    ← メインのアプリコード（必須）
# MAGIC └── requirements.txt          ← 使用するライブラリ一覧（必須）
# MAGIC ```
# MAGIC
# MAGIC ### app.py の基本構造
# MAGIC
# MAGIC ```python
# MAGIC import streamlit as st
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC # Databricks との接続（自動認証）
# MAGIC w = WorkspaceClient()
# MAGIC
# MAGIC # --- アプリの画面を作る ---
# MAGIC st.title("アプリのタイトル")
# MAGIC
# MAGIC # ユーザー入力を受け取る
# MAGIC user_input = st.text_input("質問を入力してください")
# MAGIC
# MAGIC # ボタンが押されたら処理を実行
# MAGIC if st.button("実行"):
# MAGIC     # Model Serving や Foundation Model APIs を呼び出す
# MAGIC     result = w.serving_endpoints.query(name="endpoint-name", ...)
# MAGIC     st.write(result)
# MAGIC ```
# MAGIC
# MAGIC ### requirements.txt の書き方
# MAGIC
# MAGIC ```
# MAGIC databricks-sdk     ← Databricks API を呼ぶために必須
# MAGIC streamlit           ← Web UI フレームワーク
# MAGIC pandas              ← データ操作（必要に応じて）
# MAGIC ```
# MAGIC
# MAGIC > **初心者の方へ**: `requirements.txt` は「このアプリが動くのに必要なライブラリの一覧」です。
# MAGIC > Databricks Apps がデプロイ時に自動的にインストールしてくれます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. デプロイ手順（ワイン分類予測アプリの例）
# MAGIC
# MAGIC ### ステップ 1: リポジトリをDatabricksにクローン
# MAGIC
# MAGIC 1. Databricks ワークスペースの左サイドバーで **「ワークスペース」** を選択
# MAGIC 2. 右上の **「追加」** → **「Git フォルダー」** をクリック
# MAGIC 3. このリポジトリのURLを入力してクローン
# MAGIC
# MAGIC > このステップがまだの場合は、READMEの「使い方」を参照してください。
# MAGIC
# MAGIC ### ステップ 2: Model Serving エンドポイントを作成
# MAGIC
# MAGIC `ml/08_model_serving.py` を実行して、`wine-classifier-endpoint` を作成してください。
# MAGIC
# MAGIC > エンドポイントの状態は「コンピューティング」→「モデルサービング」で確認できます。
# MAGIC > 「Ready」になっていればOKです。
# MAGIC
# MAGIC ### ステップ 3: Databricks Apps でアプリを作成
# MAGIC
# MAGIC 1. 左サイドバーの **「コンピューティング」** をクリック
# MAGIC 2. **「アプリ」** タブを選択
# MAGIC 3. **「アプリの作成」** ボタンをクリック
# MAGIC 4. 以下を設定:
# MAGIC    - **アプリ名**: `wine-classifier-app`（任意の名前）
# MAGIC    - **説明**: ワイン分類予測アプリ（任意）
# MAGIC 5. **「作成」** をクリック
# MAGIC
# MAGIC ### ステップ 4: ソースコードを指定してデプロイ
# MAGIC
# MAGIC 1. アプリの設定画面で **「ソースコード」** セクションを確認
# MAGIC 2. **ソースコードのパス**に、クローンしたリポジトリ内の `app/` フォルダを指定
# MAGIC    - 例: `/Repos/<ユーザー名>/databricks-ai-ml-hands-on/app`
# MAGIC 3. **「デプロイ」** ボタンをクリック
# MAGIC 4. デプロイが完了するまで数分待ちます
# MAGIC
# MAGIC ### ステップ 5: アプリにアクセス
# MAGIC
# MAGIC デプロイ完了後、表示される **URL** をクリックするとアプリが開きます。
# MAGIC
# MAGIC > **トラブルシューティング**:
# MAGIC > - エラー「エンドポイントが見つからない」→ `ml/08_model_serving.py` を先に実行
# MAGIC > - エラー「予測に失敗」→ エンドポイントが「Ready」状態か確認
# MAGIC > - 画面が表示されない → デプロイが完了するまで数分待つ

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. RAGチャットアプリのデプロイ
# MAGIC
# MAGIC RAGチャットアプリ（`app_rag/`）のデプロイ手順は基本的に同じです。
# MAGIC
# MAGIC | 設定 | ワイン分類アプリ | RAGチャットアプリ |
# MAGIC |---|---|---|
# MAGIC | **アプリ名** | wine-classifier-app | rag-chat-app |
# MAGIC | **ソースコードパス** | `.../app/` | `.../app_rag/` |
# MAGIC | **事前準備** | Model Serving エンドポイント | Foundation Model APIs（自動で利用可能） |
# MAGIC
# MAGIC > **RAGチャットアプリの特徴**:
# MAGIC > - Foundation Model APIs（pay-per-token）を使用するため、追加のエンドポイント作成は不要
# MAGIC > - 8つの社内FAQドキュメントがアプリ内にハードコードされています
# MAGIC > - 実際の業務では、Vector Search や外部データベースからドキュメントを取得する構成にします

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. アプリのカスタマイズ例
# MAGIC
# MAGIC デプロイしたアプリをカスタマイズしてみましょう。
# MAGIC
# MAGIC ### 例1: ワイン分類アプリにデフォルト値を変更
# MAGIC
# MAGIC `app/app.py` の `FEATURES` リスト内の `"default"` 値を変更すると、
# MAGIC スライダーの初期値が変わります。
# MAGIC
# MAGIC ### 例2: RAGチャットアプリにドキュメントを追加
# MAGIC
# MAGIC `app_rag/app.py` の `DOCUMENTS` リストに新しい辞書を追加すると、
# MAGIC FAQの対象範囲が広がります。
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Streamlit の基本的な UI パーツ
# MAGIC
# MAGIC Streamlit で使える主な UI コンポーネント:
# MAGIC
# MAGIC | コンポーネント | 用途 | コード例 |
# MAGIC |---|---|---|
# MAGIC | `st.title()` | タイトル表示 | `st.title("アプリ名")` |
# MAGIC | `st.text_input()` | テキスト入力欄 | `name = st.text_input("名前")` |
# MAGIC | `st.slider()` | スライダー | `val = st.slider("値", 0, 100, 50)` |
# MAGIC | `st.button()` | ボタン | `if st.button("実行"):` |
# MAGIC | `st.chat_input()` | チャット入力 | `msg = st.chat_input("質問")` |
# MAGIC | `st.dataframe()` | テーブル表示 | `st.dataframe(df)` |
# MAGIC | `st.spinner()` | 読み込み中表示 | `with st.spinner("処理中..."):` |
# MAGIC | `st.success()` | 成功メッセージ | `st.success("完了！")` |
# MAGIC | `st.error()` | エラーメッセージ | `st.error("失敗しました")` |
# MAGIC | `st.sidebar` | サイドバー | `with st.sidebar:` |
# MAGIC
# MAGIC > **詳しくは**: [Streamlit 公式ドキュメント](https://docs.streamlit.io/) を参照してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. コストと注意事項
# MAGIC
# MAGIC | 項目 | コスト | 備考 |
# MAGIC |---|---|---|
# MAGIC | **Databricks Apps** | アプリ稼働時間に応じた課金 | 使わないときは停止推奨 |
# MAGIC | **Model Serving** | エンドポイント稼働時間 | `scale_to_zero_enabled=True` で最適化 |
# MAGIC | **Foundation Model APIs** | 従量課金（pay-per-token） | 少額（数円/リクエスト程度） |
# MAGIC
# MAGIC > **重要**: ハンズオン終了後は以下を忘れずに実行してください:
# MAGIC > 1. Databricks Apps を停止・削除（「コンピューティング」→「アプリ」から）
# MAGIC > 2. Model Serving エンドポイントを削除（`ml/09_cleanup.py` を実行）
# MAGIC > 3. クラスターを停止

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC 1. **Databricks Apps** — Streamlit アプリをワークスペース内でホスティング
# MAGIC 2. **ファイル構成** — `app.py` + `requirements.txt` の2ファイルだけでOK
# MAGIC 3. **自動認証** — WorkspaceClient で Model Serving や LLM を簡単に呼び出し
# MAGIC 4. **デプロイ** — UI からフォルダを指定するだけの簡単操作
# MAGIC 5. **カスタマイズ** — コードを変更して再デプロイで更新
# MAGIC
# MAGIC > **次のステップ**:
# MAGIC > - アプリの UI を自分好みにカスタマイズしてみましょう
# MAGIC > - 新しいモデルやドキュメントを追加して、アプリの機能を拡張してみましょう
# MAGIC > - ハンズオン終了後は `ml/09_cleanup.py` と `genai/06_cleanup.py` でリソースをクリーンアップ

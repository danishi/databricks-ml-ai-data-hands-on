# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Apps: ワイン分類予測アプリのデプロイ
# MAGIC
# MAGIC このノートブックでは、前のノートブック（`08_model_serving.py`）で作成した
# MAGIC Model Serving エンドポイントを **Streamlit Web アプリ** から呼び出し、
# MAGIC **Databricks Apps** としてデプロイする方法を学びます。
# MAGIC
# MAGIC ## Databricks Apps とは？
# MAGIC
# MAGIC > **Databricks Apps** は、Streamlit などのフレームワークで作った Web アプリを
# MAGIC > Databricks ワークスペース内でホスティングできる機能です。
# MAGIC > アプリから Model Serving エンドポイントを **セキュアに** 呼び出すことができます。
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
# MAGIC - ワイン分類予測アプリのコード解説
# MAGIC - デプロイの手順
# MAGIC - アプリの動作確認とトラブルシューティング
# MAGIC
# MAGIC ## 前提条件
# MAGIC - 有償の Databricks ワークスペース（Community Edition では利用不可）
# MAGIC - `ml/08_model_serving.py` を実行済み（エンドポイントが稼働中であること）
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
# MAGIC | **Model Serving 連携** | エンドポイントをセキュアに呼び出し |
# MAGIC | **アクセス制御** | ワークスペースのユーザー管理と連動 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. アプリのファイル構成
# MAGIC
# MAGIC Databricks Apps に必要なファイルは最低 **2つ** だけです:
# MAGIC
# MAGIC ```
# MAGIC app/                          ← デプロイ時に指定するフォルダ
# MAGIC ├── app.py                    ← メインのアプリコード（必須）
# MAGIC └── requirements.txt          ← 使用するライブラリ一覧（必須）
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
# MAGIC ## 3. ワイン分類予測アプリのコード解説
# MAGIC
# MAGIC `app/app.py` の主要な部分を見ていきましょう。
# MAGIC
# MAGIC ### アプリの全体像
# MAGIC
# MAGIC ```
# MAGIC ブラウザ（このアプリ）
# MAGIC      ↓ スライダーで特徴量を入力
# MAGIC Databricks Apps（Streamlit サーバー）
# MAGIC      ↓ WorkspaceClient.serving_endpoints.query()
# MAGIC Model Serving エンドポイント（wine-classifier-endpoint）
# MAGIC      ↓ 学習済みモデルで予測
# MAGIC 予測結果（ワインの品種: class_0 / class_1 / class_2）
# MAGIC      ↓
# MAGIC ブラウザに結果を表示
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks との接続（自動認証）
# MAGIC
# MAGIC Databricks Apps 上では、`WorkspaceClient()` を引数なしで呼ぶだけで自動的に認証されます。
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC
# MAGIC w = WorkspaceClient()  # Databricks Apps では自動認証
# MAGIC ```
# MAGIC
# MAGIC ### Model Serving エンドポイントの呼び出し
# MAGIC
# MAGIC ```python
# MAGIC response = w.serving_endpoints.query(
# MAGIC     name="wine-classifier-endpoint",
# MAGIC     dataframe_records=[features],  # 特徴量の辞書
# MAGIC )
# MAGIC prediction = int(response.predictions[0])
# MAGIC ```
# MAGIC
# MAGIC ### Streamlit の UI パーツ
# MAGIC
# MAGIC | コンポーネント | 用途 | コード例 |
# MAGIC |---|---|---|
# MAGIC | `st.title()` | タイトル表示 | `st.title("アプリ名")` |
# MAGIC | `st.slider()` | スライダー | `val = st.slider("値", 0, 100, 50)` |
# MAGIC | `st.button()` | ボタン | `if st.button("実行"):` |
# MAGIC | `st.spinner()` | 読み込み中表示 | `with st.spinner("処理中..."):` |
# MAGIC | `st.success()` | 成功メッセージ | `st.success("完了！")` |
# MAGIC | `st.dataframe()` | テーブル表示 | `st.dataframe(df)` |
# MAGIC
# MAGIC > **詳しくは**: [Streamlit 公式ドキュメント](https://docs.streamlit.io/) を参照してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. アプリのコードを確認
# MAGIC
# MAGIC 実際のアプリコードを確認してみましょう。

# COMMAND ----------

# app/app.py の内容を表示
with open("../app/app.py", "r") as f:
    print(f.read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. デプロイ手順
# MAGIC
# MAGIC ### ステップ 1: Model Serving エンドポイントの確認
# MAGIC
# MAGIC `ml/08_model_serving.py` で作成した `wine-classifier-endpoint` が稼働中であることを確認します。

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

endpoint_name = "wine-classifier-endpoint"

try:
    endpoint = w.serving_endpoints.get(endpoint_name)
    print(f"エンドポイント '{endpoint_name}' の状態: {endpoint.state}")
    print("→ エンドポイントは存在します。デプロイを進められます。")
except Exception as e:
    print(f"エンドポイント '{endpoint_name}' が見つかりません。")
    print("→ 先に ml/08_model_serving.py を実行してエンドポイントを作成してください。")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ステップ 2: Databricks Apps でアプリを作成・デプロイ
# MAGIC
# MAGIC 1. 左サイドバーの **「コンピューティング」** をクリック
# MAGIC 2. **「アプリ」** タブを選択
# MAGIC 3. **「アプリの作成」** ボタンをクリック
# MAGIC 4. 以下を設定:
# MAGIC    - **アプリ名**: `wine-classifier-app`（任意の名前）
# MAGIC    - **説明**: ワイン分類予測アプリ（任意）
# MAGIC 5. **「作成」** をクリック
# MAGIC 6. アプリの設定画面で **「ソースコード」** セクションを確認
# MAGIC 7. **ソースコードのパス** に、クローンしたリポジトリ内の `app/` フォルダを指定
# MAGIC    - 例: `/Workspace/Users/<ユーザー名>/databricks-ai-ml-hands-on/app`
# MAGIC 8. **「デプロイ」** ボタンをクリック
# MAGIC 9. デプロイが完了するまで数分待ちます
# MAGIC
# MAGIC ### ステップ 3: アプリにアクセス
# MAGIC
# MAGIC デプロイ完了後、表示される **URL** をクリックするとアプリが開きます。
# MAGIC
# MAGIC > **トラブルシューティング**:
# MAGIC > - エラー「エンドポイントが見つからない」→ `ml/08_model_serving.py` を先に実行
# MAGIC > - エラー「予測に失敗」→ エンドポイントが「Ready」状態か確認
# MAGIC > - 画面が表示されない → デプロイが完了するまで数分待つ

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. アプリのカスタマイズ例
# MAGIC
# MAGIC デプロイしたアプリをカスタマイズしてみましょう。
# MAGIC
# MAGIC ### 例: スライダーのデフォルト値を変更
# MAGIC
# MAGIC `app/app.py` の `FEATURES` リスト内の `"default"` 値を変更すると、
# MAGIC スライダーの初期値が変わります。
# MAGIC
# MAGIC > **変更後の再デプロイ**: ソースコードを変更した後、
# MAGIC > アプリの設定画面で **「再デプロイ」** をクリックすると更新が反映されます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. コストと注意事項
# MAGIC
# MAGIC | 項目 | コスト | 備考 |
# MAGIC |---|---|---|
# MAGIC | **Databricks Apps** | アプリ稼働時間に応じた課金 | 使わないときは停止推奨 |
# MAGIC | **Model Serving** | エンドポイント稼働時間 | `scale_to_zero_enabled=True` で最適化 |
# MAGIC
# MAGIC > **重要**: ハンズオン終了後は以下を忘れずに実行してください:
# MAGIC > 1. Databricks Apps を停止・削除（「コンピューティング」→「アプリ」から）
# MAGIC > 2. Model Serving エンドポイントを削除（`ml/10_cleanup.py` を実行）
# MAGIC > 3. クラスターを停止

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **Databricks Apps** — Streamlit アプリをワークスペース内でホスティング
# MAGIC 2. **ファイル構成** — `app.py` + `requirements.txt` の2ファイルだけでOK
# MAGIC 3. **自動認証** — WorkspaceClient で Model Serving を簡単に呼び出し
# MAGIC 4. **デプロイ** — UI からフォルダを指定するだけの簡単操作
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - アプリの UI を自分好みにカスタマイズしてみましょう
# MAGIC - ハンズオン完了後は **`10_cleanup.py`** でリソースをクリーンアップしてください
# MAGIC
# MAGIC > **認定試験との関連** (ML Associate):
# MAGIC > - **Model Deployment (12%)**: モデルサービングエンドポイントをアプリケーションから利用する方法
# MAGIC > - **Databricks Machine Learning (38%)**: Databricks Apps によるモデルの活用

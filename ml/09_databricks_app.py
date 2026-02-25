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
# MAGIC > **Databricks Apps** は、Streamlit・Gradio・Dash などのフレームワークで作った Web アプリを
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
# MAGIC | **Web アプリのホスティング** | Streamlit / Gradio / Dash / Flask をサポート |
# MAGIC | **自動認証** | WorkspaceClient が自動的に認証（トークン管理不要） |
# MAGIC | **Model Serving 連携** | エンドポイントをセキュアに呼び出し |
# MAGIC | **アクセス制御** | ワークスペースのユーザー管理と連動 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. アプリのファイル構成
# MAGIC
# MAGIC Databricks Apps に必要なファイル構成です:
# MAGIC
# MAGIC ```
# MAGIC app/                          ← デプロイ時に指定するフォルダ
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
# MAGIC ### ステップ 2: Databricks Apps でアプリを作成
# MAGIC
# MAGIC 1. 左サイドバーの **「+ 新規」** をクリック
# MAGIC 2. メニューから **「アプリ」** を選択
# MAGIC 3. **「カスタムアプリを作成」** をクリック
# MAGIC 4. 以下を設定:
# MAGIC    - **アプリ名**: `wine-classifier-app`（任意の名前、小文字・数字・ハイフンのみ）
# MAGIC    - **説明**: ワイン分類予測アプリ（任意）
# MAGIC 5. **「次: 設定」** をクリック（または「アプリの作成」で詳細設定をスキップ）
# MAGIC 6. 必要に応じて以下を設定:
# MAGIC    - **アプリのリソース**: 「サービングエンドポイント」リソースとして `wine-classifier-endpoint` を追加
# MAGIC    - **コンピュートサイズ**: アプリの CPU・メモリを設定
# MAGIC 7. **「アプリの作成」** をクリック
# MAGIC
# MAGIC ### ステップ 3: アプリをデプロイ
# MAGIC
# MAGIC 1. アプリの詳細画面で **「デプロイ」** ボタンをクリック
# MAGIC 2. ワークスペース内の `app/` フォルダを選択
# MAGIC    - 例: `/Workspace/Users/<ユーザー名>/databricks-ai-ml-hands-on/app`
# MAGIC 3. **「選択」** → **「デプロイ」** をクリック
# MAGIC 4. デプロイが完了するまで数分待ちます
# MAGIC
# MAGIC ### ステップ 4: アプリにアクセス
# MAGIC
# MAGIC デプロイ完了後、アプリ詳細画面に表示される **URL** をクリックするとアプリが開きます。
# MAGIC
# MAGIC > **トラブルシューティング**:
# MAGIC > - エラー「エンドポイントが見つからない」→ `ml/08_model_serving.py` を先に実行
# MAGIC > - エラー「予測に失敗」→ エンドポイントが「Ready」状態か確認
# MAGIC > - 画面が表示されない → デプロイが完了するまで数分待つ
# MAGIC > - デプロイ失敗 → アプリの詳細画面のログを確認

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
# MAGIC > アプリの詳細画面で **「デプロイ」** をクリックして再デプロイすると更新が反映されます。

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
# MAGIC > 1. Databricks Apps を停止・削除（サイドバー「コンピューティング」→「アプリ」タブから）
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
# MAGIC 2. **ファイル構成** — `app.py` + `app.yaml` + `requirements.txt` の3ファイル構成
# MAGIC 3. **自動認証** — WorkspaceClient で Model Serving を簡単に呼び出し
# MAGIC 4. **デプロイ** — サイドバー「+ 新規」→「アプリ」からフォルダを指定するだけの簡単操作
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - アプリの UI を自分好みにカスタマイズしてみましょう
# MAGIC - ハンズオン完了後は **`10_cleanup.py`** でリソースをクリーンアップしてください
# MAGIC
# MAGIC > **認定試験との関連** (ML Associate):
# MAGIC > - **Model Deployment (12%)**: モデルサービングエンドポイントをアプリケーションから利用する方法
# MAGIC > - **Databricks Machine Learning (38%)**: Databricks Apps によるモデルの活用

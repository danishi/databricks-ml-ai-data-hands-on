# Databricks notebook source
# MAGIC %md
# MAGIC # クリーンアップ: 生成AI ハンズオンのリソース削除
# MAGIC
# MAGIC このノートブックでは、生成AIハンズオンで作成したリソースを削除します。
# MAGIC
# MAGIC ## 削除対象
# MAGIC
# MAGIC | リソース | 作成元 | 削除方法 |
# MAGIC |---|---|---|
# MAGIC | RAGチャットアプリ（Databricks App） | `app_rag/` をデプロイ | 手動削除（UIから） |
# MAGIC
# MAGIC > **補足**: 生成AIハンズオン（`01_foundation_model_apis.py` / `02_rag_chat.py`）では、
# MAGIC > Foundation Model APIs（pay-per-token）を使用しているため、
# MAGIC > エンドポイントやモデルの削除は不要です。課金はAPIの呼び出し量に応じて発生し、
# MAGIC > 使わなければ費用はかかりません。
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Databricks App の削除（手動）
# MAGIC
# MAGIC RAGチャットアプリをDatabricks Appsとしてデプロイした場合は、以下の手順で削除してください。
# MAGIC
# MAGIC 1. 左サイドバーの **「コンピューティング」** → **「アプリ」** を選択
# MAGIC 2. 削除したいアプリ（例: `rag-chat-app`）を選択
# MAGIC 3. 右上の **「...」メニュー** → **「削除」** をクリック
# MAGIC
# MAGIC > Databricks App は稼働時間に応じた課金が発生するため、不要になったら早めに削除しましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. クラスターの停止
# MAGIC
# MAGIC ハンズオンで使用したクラスターも忘れずに停止しましょう。
# MAGIC
# MAGIC 1. 左サイドバーの **「コンピューティング」** を選択
# MAGIC 2. 使用したクラスターの行で **「終了」** をクリック
# MAGIC
# MAGIC > **自動終了**: クラスター作成時に「非アクティブ後に自動終了」を設定していれば、
# MAGIC > 一定時間後に自動的に停止されます（デフォルト: 120分）。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## クリーンアップ完了
# MAGIC
# MAGIC | リソース | 状態 |
# MAGIC |---|---|
# MAGIC | Foundation Model APIs | 追加の削除不要（pay-per-token） |
# MAGIC | Databricks App（RAGチャット） | 手動削除（UIから） |
# MAGIC | クラスター | 手動停止 |
# MAGIC
# MAGIC > **MLハンズオンのリソース**（Model Servingエンドポイント、Unity Catalogモデル）を
# MAGIC > 削除する場合は `ml/03_cleanup.py` を実行してください。
# MAGIC
# MAGIC お疲れさまでした！

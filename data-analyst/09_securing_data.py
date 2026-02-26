# Databricks notebook source
# MAGIC %md
# MAGIC # データのセキュリティ
# MAGIC
# MAGIC このノートブックでは、Unity Catalog を使った**データのセキュリティとアクセス制御**を学びます。
# MAGIC データアナリストとして、適切な権限管理とセキュリティの仕組みを理解することは重要です。
# MAGIC
# MAGIC **データセキュリティとは？**
# MAGIC > 適切な人だけが適切なデータにアクセスできるようにする仕組みです。
# MAGIC > 「鍵のかかった書類棚」のように、権限のある人だけが中身を見られる状態を作ります。
# MAGIC
# MAGIC ### このノートブックで体験する流れ
# MAGIC
# MAGIC ```
# MAGIC ① Unity Catalog の権限モデル → ② テーブル/ビューの権限設定
# MAGIC                                           ↓
# MAGIC ④ 監査ログ ← ③ 動的ビューによる行/列フィルタ
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - Unity Catalog の権限モデル（所有者、権限、ロール）
# MAGIC - 三層名前空間でのアクセス制御
# MAGIC - GRANT / REVOKE による権限管理
# MAGIC - 動的ビューによる行レベル・列レベルのフィルタリング
# MAGIC - データの所有権とセキュリティのベストプラクティス
# MAGIC
# MAGIC ## 前提条件
# MAGIC - `02_managing_data.py` を実行済みであること
# MAGIC - Databricks Runtime（例: 16.x）のクラスターを使用してください
# MAGIC - Unity Catalog が有効なワークスペースを推奨
# MAGIC
# MAGIC ---

# COMMAND ----------

# 初期設定
catalog = "main"
schema = "default"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"使用するカタログ: {catalog}")
print(f"使用するスキーマ: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Unity Catalog の権限モデル
# MAGIC
# MAGIC Unity Catalog では、**三層名前空間**（カタログ / スキーマ / オブジェクト）の各レベルで
# MAGIC 権限を設定できます。上位レベルの権限は下位に**継承**されます。
# MAGIC
# MAGIC ```
# MAGIC カタログレベルの権限    → スキーマに継承 → テーブル/ビューに継承
# MAGIC (USE CATALOG)            (USE SCHEMA)     (SELECT, MODIFY 等)
# MAGIC ```
# MAGIC
# MAGIC ### 主要な権限
# MAGIC
# MAGIC | 権限 | 対象 | 説明 |
# MAGIC |---|---|---|
# MAGIC | `USE CATALOG` | カタログ | カタログ内のオブジェクトにアクセスする前提権限 |
# MAGIC | `USE SCHEMA` | スキーマ | スキーマ内のオブジェクトにアクセスする前提権限 |
# MAGIC | `SELECT` | テーブル/ビュー | データの読み取り |
# MAGIC | `MODIFY` | テーブル | データの挿入・更新・削除 |
# MAGIC | `CREATE TABLE` | スキーマ | テーブルの作成 |
# MAGIC | `CREATE VIEW` | スキーマ | ビューの作成 |
# MAGIC | `ALL PRIVILEGES` | すべて | すべての権限 |
# MAGIC
# MAGIC > **重要**: `SELECT` 権限があっても、`USE CATALOG` と `USE SCHEMA` が
# MAGIC > なければデータにアクセスできません。3つの権限がすべて必要です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 権限の付与と取り消し
# MAGIC
# MAGIC | 操作 | 構文 | 例 |
# MAGIC |---|---|---|
# MAGIC | **権限の付与** | `GRANT privilege ON object TO principal` | `GRANT SELECT ON TABLE t TO user` |
# MAGIC | **権限の取り消し** | `REVOKE privilege ON object FROM principal` | `REVOKE SELECT ON TABLE t FROM user` |
# MAGIC | **権限の確認** | `SHOW GRANTS ON object` | `SHOW GRANTS ON TABLE t` |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの現在の権限を確認
# MAGIC SHOW GRANTS ON TABLE da_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### GRANT / REVOKE の例
# MAGIC
# MAGIC ```sql
# MAGIC -- ユーザーに SELECT 権限を付与
# MAGIC GRANT SELECT ON TABLE da_customers TO `user@example.com`;
# MAGIC
# MAGIC -- グループに SELECT 権限を付与
# MAGIC GRANT SELECT ON TABLE da_customers TO `data_analysts`;
# MAGIC
# MAGIC -- スキーマ内の全テーブルへのアクセス権限
# MAGIC GRANT USE CATALOG ON CATALOG main TO `data_analysts`;
# MAGIC GRANT USE SCHEMA ON SCHEMA main.default TO `data_analysts`;
# MAGIC GRANT SELECT ON SCHEMA main.default TO `data_analysts`;
# MAGIC
# MAGIC -- 権限の取り消し
# MAGIC REVOKE SELECT ON TABLE da_customers FROM `user@example.com`;
# MAGIC ```
# MAGIC
# MAGIC > **認定試験のポイント**: GRANT / REVOKE の構文と、カタログ・スキーマ・テーブルの
# MAGIC > 各レベルでの権限設定方法を理解しておきましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. テーブルの所有権（Ownership）
# MAGIC
# MAGIC Unity Catalog のすべてのオブジェクトには**所有者（Owner）**がいます。
# MAGIC
# MAGIC | 概念 | 説明 |
# MAGIC |---|---|
# MAGIC | **所有者** | オブジェクトを作成したユーザーまたはグループ |
# MAGIC | **所有者の権限** | すべての操作が可能（GRANT含む） |
# MAGIC | **所有権の移譲** | `ALTER TABLE ... SET OWNER TO ...` |
# MAGIC
# MAGIC ```sql
# MAGIC -- 所有権の移譲例
# MAGIC ALTER TABLE da_customers SET OWNER TO `data_team`;
# MAGIC ```
# MAGIC
# MAGIC > **ベストプラクティス**: テーブルの所有者は個人ではなく**グループ**に設定しましょう。
# MAGIC > 個人が退職した場合でも、グループが所有者であれば管理を継続できます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 動的ビューによるアクセス制御
# MAGIC
# MAGIC **動的ビュー**を使うと、ユーザーの権限に応じて異なるデータを表示できます。
# MAGIC これにより、行レベルや列レベルでのアクセス制御が実現できます。
# MAGIC
# MAGIC ### 使用する関数
# MAGIC
# MAGIC | 関数 | 説明 | 戻り値 |
# MAGIC |---|---|---|
# MAGIC | `current_user()` | 現在のユーザー名 | メールアドレス |
# MAGIC | `is_member('group')` | 指定グループのメンバーか | true / false |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 現在のユーザー情報を確認
# MAGIC SELECT
# MAGIC   current_user() AS current_user;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 列レベルのフィルタリング: PIIデータのマスキング
# MAGIC -- 管理者グループ以外にはメールアドレスをマスクする
# MAGIC CREATE OR REPLACE VIEW da_customers_secure AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   CASE
# MAGIC     WHEN is_member('admins') THEN email
# MAGIC     ELSE CONCAT(LEFT(email, 2), '****@****')
# MAGIC   END AS email,
# MAGIC   city,
# MAGIC   segment,
# MAGIC   registration_date
# MAGIC FROM da_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- セキュアビューの確認（自分の権限でどのように見えるか）
# MAGIC SELECT * FROM da_customers_secure ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 行レベルのフィルタリング
# MAGIC
# MAGIC 動的ビューを使って、ユーザーに応じて表示する行を制限できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 行レベルのフィルタリング例
# MAGIC -- 各地域の担当者には、自分の地域のデータのみ表示する
# MAGIC CREATE OR REPLACE VIEW da_orders_regional AS
# MAGIC SELECT
# MAGIC   o.order_id,
# MAGIC   o.order_date,
# MAGIC   o.product_name,
# MAGIC   o.total_amount,
# MAGIC   c.city,
# MAGIC   c.name AS customer_name
# MAGIC FROM da_orders o
# MAGIC JOIN da_customers c ON o.customer_id = c.customer_id
# MAGIC WHERE
# MAGIC   -- 管理者は全データを表示
# MAGIC   is_member('admins')
# MAGIC   -- それ以外のユーザーは全データを表示（デモ用、実際はユーザーの地域でフィルタ）
# MAGIC   OR TRUE;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM da_orders_regional ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC > **認定試験のポイント**: 動的ビューによる行レベル/列レベルのセキュリティは
# MAGIC > 重要な出題範囲です。`is_member()` と `current_user()` の使い方を理解しましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. データのマスキングとPII保護
# MAGIC
# MAGIC **個人識別情報（PII）** を保護するためのテクニックです。
# MAGIC
# MAGIC | 手法 | 説明 | 例 |
# MAGIC |---|---|---|
# MAGIC | **マスキング** | データの一部を隠す | `tanaka@...` → `ta****@****` |
# MAGIC | **ハッシュ化** | 元のデータを復元不能に変換 | `SHA2(email, 256)` |
# MAGIC | **トークン化** | 別の値に置き換え | `email_001` |
# MAGIC | **匿名化** | 個人を特定できないようにする | 年齢を年代に変換 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PII保護のテクニック例
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   -- マスキング: 名前の一部を隠す
# MAGIC   CASE
# MAGIC     WHEN name IS NOT NULL THEN CONCAT(LEFT(name, 1), '***')
# MAGIC     ELSE NULL
# MAGIC   END AS masked_name,
# MAGIC   -- ハッシュ化: メールアドレスをハッシュ値に変換
# MAGIC   CASE
# MAGIC     WHEN email IS NOT NULL THEN SHA2(email, 256)
# MAGIC     ELSE NULL
# MAGIC   END AS hashed_email,
# MAGIC   -- 一般化: 都市名はそのまま（個人を特定しにくい）
# MAGIC   city,
# MAGIC   segment
# MAGIC FROM da_customers
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Unity Catalog のロールとグループ
# MAGIC
# MAGIC ### 主要なロール
# MAGIC
# MAGIC | ロール | 説明 |
# MAGIC |---|---|
# MAGIC | **Account Admin** | ワークスペース全体の管理者 |
# MAGIC | **Workspace Admin** | ワークスペースの設定・ユーザー管理 |
# MAGIC | **Metastore Admin** | Unity Catalog のメタストア管理 |
# MAGIC | **Catalog Owner** | カタログの管理者 |
# MAGIC | **Data Engineer** | テーブルの作成・更新 |
# MAGIC | **Data Analyst** | データの読み取り・分析 |
# MAGIC
# MAGIC ### グループベースのアクセス制御
# MAGIC
# MAGIC ```
# MAGIC [Account Admin]
# MAGIC      |
# MAGIC      ├── [data_engineers] グループ → CREATE TABLE, MODIFY
# MAGIC      ├── [data_analysts] グループ  → SELECT のみ
# MAGIC      └── [data_viewers] グループ   → SELECT（制限付きビュー経由）
# MAGIC ```
# MAGIC
# MAGIC > **ベストプラクティス**: 権限は**グループ単位**で管理し、個人への直接付与は避けましょう。
# MAGIC > これにより、メンバーの追加・削除時の権限管理が容易になります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. セキュリティのベストプラクティス
# MAGIC
# MAGIC | カテゴリ | ベストプラクティス |
# MAGIC |---|---|
# MAGIC | **最小権限の原則** | 必要最小限の権限のみ付与 |
# MAGIC | **グループベース管理** | 個人ではなくグループに権限を付与 |
# MAGIC | **PII保護** | タグでPIIカラムを識別し、マスキングビューを使用 |
# MAGIC | **所有権管理** | テーブルの所有者はグループに設定 |
# MAGIC | **監査** | 定期的に権限設定を確認 |
# MAGIC | **セキュアビュー** | 動的ビューで行/列レベルのアクセス制御 |
# MAGIC | **シークレット管理** | 資格情報は Secret Scope に保管 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Secret Scope — 資格情報の安全な管理
# MAGIC
# MAGIC **Secret Scope** は、APIキー、パスワード、接続文字列などの
# MAGIC 機密情報を安全に保管する仕組みです。
# MAGIC
# MAGIC | 概念 | 説明 |
# MAGIC |---|---|
# MAGIC | **Secret Scope** | シークレットのグループ（フォルダのようなもの） |
# MAGIC | **Secret** | 個々の機密情報（キーと値のペア） |
# MAGIC | **アクセス制御** | スコープごとにアクセス権限を設定 |
# MAGIC
# MAGIC ### Secret Scope の利用
# MAGIC
# MAGIC ```python
# MAGIC # シークレットの取得（コード内にパスワードを直接書かない）
# MAGIC # NG: password = "my_secret_password"
# MAGIC # OK: password = dbutils.secrets.get(scope="my_scope", key="db_password")
# MAGIC ```
# MAGIC
# MAGIC > **認定試験のポイント**: Secret Scope の目的（コード内に機密情報を記述しない）と
# MAGIC > 基本的な使い方を理解しておきましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. 監査ログ（Audit Logs）
# MAGIC
# MAGIC Databricks のすべての操作は**監査ログ**に記録されます。
# MAGIC
# MAGIC | 記録される情報 | 説明 |
# MAGIC |---|---|
# MAGIC | **誰が** | 操作を行ったユーザー |
# MAGIC | **何を** | 実行した操作（クエリ、権限変更等） |
# MAGIC | **いつ** | 操作の日時 |
# MAGIC | **どこで** | 対象のオブジェクト（テーブル等） |
# MAGIC | **結果** | 操作の成功/失敗 |
# MAGIC
# MAGIC ### 監査ログの活用
# MAGIC
# MAGIC - **コンプライアンス**: 規制要件に基づくデータアクセスの記録
# MAGIC - **セキュリティ監視**: 不正アクセスの検出
# MAGIC - **トラブルシューティング**: 問題発生時の原因調査
# MAGIC - **利用状況の分析**: どのデータがよく使われているかの把握

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **Unity Catalog の権限モデル** — 三層名前空間でのアクセス制御
# MAGIC 2. **GRANT / REVOKE** — 権限の付与と取り消し
# MAGIC 3. **テーブルの所有権** — 所有者の役割と所有権の移譲
# MAGIC 4. **動的ビュー** — 行レベル/列レベルのフィルタリング
# MAGIC 5. **PII保護** — マスキング、ハッシュ化、タグ付け
# MAGIC 6. **ロールとグループ** — グループベースのアクセス制御
# MAGIC 7. **Secret Scope** — 資格情報の安全な管理
# MAGIC 8. **監査ログ** — 操作の記録と監視
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - 動的ビューを使って、自分のデータにセキュリティを適用してみましょう
# MAGIC - ハンズオン終了後は **`10_cleanup.py`** でリソースをクリーンアップしましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Analyst Associate):
# MAGIC > - **Securing Data (8%)**: Unity Catalog 権限、動的ビュー、Secret Scope、監査ログ

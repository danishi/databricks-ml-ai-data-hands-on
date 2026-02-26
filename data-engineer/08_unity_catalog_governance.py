# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog とデータガバナンス
# MAGIC
# MAGIC このノートブックでは、Databricks の統合ガバナンスレイヤーである **Unity Catalog** と
# MAGIC データガバナンスの機能を学びます。
# MAGIC
# MAGIC **Unity Catalog とは？**
# MAGIC > Databricks ワークスペース全体で**データとAIアセットを統合管理**するガバナンスソリューションです。
# MAGIC > アクセス制御、監査ログ、データリネージを提供します。
# MAGIC
# MAGIC ### Unity Catalog の階層構造
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────┐
# MAGIC │                  Metastore                       │
# MAGIC │  (ワークスペースのトップレベルコンテナ)                │
# MAGIC │                                                 │
# MAGIC │  ┌──────────────────────────────────────┐      │
# MAGIC │  │            Catalog                    │      │
# MAGIC │  │  (データベースのグループ)                │      │
# MAGIC │  │                                      │      │
# MAGIC │  │  ┌───────────────────────────┐      │      │
# MAGIC │  │  │         Schema             │      │      │
# MAGIC │  │  │  (テーブルのグループ)        │      │      │
# MAGIC │  │  │                           │      │      │
# MAGIC │  │  │  ┌─────────────────────┐ │      │      │
# MAGIC │  │  │  │  Table / View /     │ │      │      │
# MAGIC │  │  │  │  Function / Model   │ │      │      │
# MAGIC │  │  │  └─────────────────────┘ │      │      │
# MAGIC │  │  └───────────────────────────┘      │      │
# MAGIC │  └──────────────────────────────────────┘      │
# MAGIC └─────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - Unity Catalog の階層構造（Metastore → Catalog → Schema → Table）
# MAGIC - テーブルの種類（マネージド vs 外部）
# MAGIC - アクセス権限の管理
# MAGIC - 外部ロケーション
# MAGIC - Delta Sharing
# MAGIC - データリネージ
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime（例: 16.x）のクラスター
# MAGIC - Unity Catalog が有効化されたワークスペース
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Unity Catalog の階層構造
# MAGIC
# MAGIC Unity Catalog は **3レベルの名前空間** を使用してデータを管理します。
# MAGIC
# MAGIC ### 3レベルの名前空間
# MAGIC
# MAGIC | レベル | 説明 | 例 |
# MAGIC |---|---|---|
# MAGIC | **Catalog** | データの最上位グループ | `main`, `production`, `development` |
# MAGIC | **Schema** | テーブルの論理グループ | `default`, `sales`, `analytics` |
# MAGIC | **Table/View** | 実際のデータオブジェクト | `orders`, `customers`, `daily_summary` |
# MAGIC
# MAGIC テーブルへのアクセスは **3部構成の名前**（`catalog.schema.table`）で行います。
# MAGIC
# MAGIC ```sql
# MAGIC -- 完全修飾名でテーブルを参照
# MAGIC SELECT * FROM main.sales.orders
# MAGIC --           ^^^^  ^^^^^  ^^^^^^
# MAGIC --           catalog schema table
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 利用可能なカタログの確認
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 現在のカタログとスキーマを確認
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- カタログ内のスキーマ一覧
# MAGIC SHOW SCHEMAS IN main

# COMMAND ----------

# MAGIC %sql
# MAGIC -- スキーマ内のテーブル一覧
# MAGIC SHOW TABLES IN main.default

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. カタログとスキーマの管理
# MAGIC
# MAGIC > **注意**: カタログの作成にはメタストアの管理者権限が必要です。
# MAGIC > 以下のコマンドは権限がある場合にのみ実行できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- カタログの作成（権限がある場合）
# MAGIC -- CREATE CATALOG IF NOT EXISTS dev_catalog;
# MAGIC
# MAGIC -- スキーマの作成
# MAGIC CREATE SCHEMA IF NOT EXISTS main.data_engineer_handson;
# MAGIC
# MAGIC -- 作成したスキーマを確認
# MAGIC DESCRIBE SCHEMA main.data_engineer_handson

# COMMAND ----------

# MAGIC %sql
# MAGIC -- デフォルトのカタログとスキーマを設定
# MAGIC USE CATALOG main;
# MAGIC USE SCHEMA data_engineer_handson;
# MAGIC
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: `USE CATALOG` と `USE SCHEMA` でデフォルトの名前空間を設定できます。
# MAGIC > デフォルトを設定すると、3部構成の完全修飾名を省略できます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. テーブルの種類
# MAGIC
# MAGIC Unity Catalog では2種類のテーブルがあります。
# MAGIC
# MAGIC | 種類 | データの保存場所 | DROP TABLE の動作 | 作成方法 |
# MAGIC |---|---|---|---|
# MAGIC | **マネージドテーブル** | Unity Catalog が管理するストレージ | データも削除される | `CREATE TABLE` |
# MAGIC | **外部テーブル** | ユーザー指定のストレージ | メタデータのみ削除 | `CREATE TABLE ... LOCATION` |
# MAGIC
# MAGIC ```
# MAGIC マネージドテーブル:                外部テーブル:
# MAGIC ┌──────────────────┐            ┌──────────────────┐
# MAGIC │ Unity Catalog     │            │ Unity Catalog     │
# MAGIC │ ┌──────────────┐ │            │ ┌──────────────┐ │
# MAGIC │ │ メタデータ     │ │            │ │ メタデータ     │ │
# MAGIC │ │ + データ本体  │ │            │ │ (参照のみ)    │ │
# MAGIC │ └──────────────┘ │            │ └──────────────┘ │
# MAGIC └──────────────────┘            └────────┬─────────┘
# MAGIC                                          │ 参照
# MAGIC                                 ┌────────▼─────────┐
# MAGIC                                 │ 外部ストレージ     │
# MAGIC                                 │ (S3/ADLS/GCS)    │
# MAGIC                                 │ データ本体はここ   │
# MAGIC                                 └──────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- マネージドテーブルの作成
# MAGIC CREATE OR REPLACE TABLE main.data_engineer_handson.managed_example (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   value DOUBLE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO main.data_engineer_handson.managed_example VALUES
# MAGIC   (1, 'テスト1', 100.0),
# MAGIC   (2, 'テスト2', 200.0),
# MAGIC   (3, 'テスト3', 300.0)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの種類を確認
# MAGIC DESCRIBE TABLE EXTENDED main.data_engineer_handson.managed_example

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: マネージドテーブルは `DROP TABLE` でデータも削除されますが、
# MAGIC > 外部テーブルは `DROP TABLE` してもデータは外部ストレージに残ります。
# MAGIC > Unity Catalog では**マネージドテーブルが推奨**されています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. アクセス権限の管理
# MAGIC
# MAGIC Unity Catalog では **GRANT / REVOKE** 文で細かいアクセス制御が可能です。
# MAGIC
# MAGIC ### 権限の種類
# MAGIC
# MAGIC | 権限 | 対象 | 説明 |
# MAGIC |---|---|---|
# MAGIC | `SELECT` | テーブル/ビュー | データの読み取り |
# MAGIC | `MODIFY` | テーブル | データの追加・更新・削除 |
# MAGIC | `CREATE TABLE` | スキーマ | テーブルの作成 |
# MAGIC | `CREATE SCHEMA` | カタログ | スキーマの作成 |
# MAGIC | `USE CATALOG` | カタログ | カタログの参照 |
# MAGIC | `USE SCHEMA` | スキーマ | スキーマの参照 |
# MAGIC | `ALL PRIVILEGES` | 任意 | すべての権限 |
# MAGIC
# MAGIC ### 権限の継承
# MAGIC
# MAGIC ```
# MAGIC Catalog に GRANT
# MAGIC    ↓ 継承
# MAGIC Schema に自動適用
# MAGIC    ↓ 継承
# MAGIC Table に自動適用
# MAGIC ```
# MAGIC
# MAGIC > **重要**: 上位レベルで付与された権限は下位レベルに**継承**されます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 権限の付与（例）
# MAGIC -- GRANT SELECT ON TABLE main.data_engineer_handson.managed_example TO `data_analysts`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA main.data_engineer_handson TO `data_analysts`;
# MAGIC -- GRANT USE CATALOG ON CATALOG main TO `data_analysts`;
# MAGIC
# MAGIC -- 現在のテーブルの権限を確認
# MAGIC SHOW GRANTS ON TABLE main.data_engineer_handson.managed_example

# COMMAND ----------

# MAGIC %md
# MAGIC ### サービスプリンシパル
# MAGIC
# MAGIC **サービスプリンシパル**は、自動化されたジョブやアプリケーションが使用する ID です。
# MAGIC
# MAGIC | 対象 | 説明 | 用途 |
# MAGIC |---|---|---|
# MAGIC | ユーザー | 個人のアカウント | 対話型の開発・分析 |
# MAGIC | グループ | ユーザーの集合 | チーム単位の権限管理 |
# MAGIC | サービスプリンシパル | マシン用のアカウント | 自動化ジョブ、CI/CD |
# MAGIC
# MAGIC > **試験ポイント**: 本番環境のジョブでは、個人アカウントではなく
# MAGIC > **サービスプリンシパル**を使用するのがベストプラクティスです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 外部ロケーション（External Locations）
# MAGIC
# MAGIC **外部ロケーション**は、Unity Catalog が管理する外部ストレージへのアクセスポイントです。
# MAGIC
# MAGIC ### 外部ロケーションの仕組み
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────┐
# MAGIC │  Unity Catalog    │
# MAGIC │                  │
# MAGIC │  External Location│
# MAGIC │  (アクセス制御)    │──→ Storage Credential ──→ クラウドストレージ
# MAGIC │                  │     (認証情報)              (S3/ADLS/GCS)
# MAGIC └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC | コンポーネント | 説明 |
# MAGIC |---|---|
# MAGIC | **Storage Credential** | クラウドストレージへの認証情報（IAM ロール等） |
# MAGIC | **External Location** | Storage Credential + パスの組み合わせ |
# MAGIC
# MAGIC ```sql
# MAGIC -- 外部ロケーションの作成例
# MAGIC CREATE EXTERNAL LOCATION my_location
# MAGIC URL 's3://my-bucket/data/'
# MAGIC WITH (STORAGE CREDENTIAL my_credential);
# MAGIC
# MAGIC -- 外部テーブルの作成
# MAGIC CREATE TABLE my_catalog.my_schema.external_table
# MAGIC LOCATION 's3://my-bucket/data/external_table/';
# MAGIC ```
# MAGIC
# MAGIC > **試験ポイント**: 外部テーブルを作成するには、対象パスをカバーする
# MAGIC > External Location と Storage Credential が必要です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delta Sharing
# MAGIC
# MAGIC **Delta Sharing** は、組織の境界を越えてデータを安全に共有するためのオープンプロトコルです。
# MAGIC
# MAGIC ### Delta Sharing の仕組み
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐    共有     ┌──────────────┐
# MAGIC │  Provider     │  ────────→ │  Recipient    │
# MAGIC │ (データ提供者)  │           │ (データ受領者)  │
# MAGIC │              │           │              │
# MAGIC │ • Share を作成 │           │ • 読み取り専用  │
# MAGIC │ • テーブルを追加│           │ • コピー不要   │
# MAGIC │ • 受領者を設定 │           │ • 常に最新データ│
# MAGIC └──────────────┘           └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Delta Sharing の種類
# MAGIC
# MAGIC | 種類 | 受領者の環境 | 認証方法 |
# MAGIC |---|---|---|
# MAGIC | **内部共有** | 同じ組織内の別ワークスペース | Unity Catalog |
# MAGIC | **外部共有** | 異なる組織 | トークンベース |
# MAGIC
# MAGIC ### Share の管理
# MAGIC
# MAGIC ```sql
# MAGIC -- Share の作成
# MAGIC CREATE SHARE IF NOT EXISTS my_share;
# MAGIC
# MAGIC -- Share にテーブルを追加
# MAGIC ALTER SHARE my_share ADD TABLE main.sales.orders;
# MAGIC
# MAGIC -- 受領者の作成
# MAGIC CREATE RECIPIENT IF NOT EXISTS partner_company;
# MAGIC
# MAGIC -- 受領者に Share を付与
# MAGIC GRANT SELECT ON SHARE my_share TO RECIPIENT partner_company;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: Delta Sharing では Provider（データ提供者）が Share を作成し、
# MAGIC > Recipient（受領者）に対して読み取りアクセスを付与します。
# MAGIC > 受領者はデータのコピーを持たず、常に最新のデータにアクセスできます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. データリネージ
# MAGIC
# MAGIC Unity Catalog は**データリネージ**（データの来歴）を自動的に追跡します。
# MAGIC
# MAGIC ```
# MAGIC データリネージの例:
# MAGIC
# MAGIC [CSV ファイル] → [orders_bronze] → [orders_silver] → [daily_sales_gold]
# MAGIC                                         ↓
# MAGIC                                [product_analysis_gold]
# MAGIC ```
# MAGIC
# MAGIC | 追跡対象 | 説明 |
# MAGIC |---|---|
# MAGIC | テーブルリネージ | テーブル間のデータフローを可視化 |
# MAGIC | カラムリネージ | カラムレベルでの変換を追跡 |
# MAGIC | ノートブックリネージ | どのノートブックがデータを生成したか |
# MAGIC
# MAGIC > リネージは Unity Catalog の UI（**リネージタブ**）で確認できます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Lakehouse Federation
# MAGIC
# MAGIC **Lakehouse Federation** を使うと、外部データベース（PostgreSQL、MySQL、SQL Server など）の
# MAGIC データを Unity Catalog から直接クエリできます。
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────┐
# MAGIC │          Unity Catalog                  │
# MAGIC │                                        │
# MAGIC │  ┌──────────┐  ┌──────────────────┐  │
# MAGIC │  │ Delta     │  │ Foreign Catalog   │  │
# MAGIC │  │ テーブル   │  │ (外部DB)          │  │
# MAGIC │  │          │  │ • PostgreSQL      │  │
# MAGIC │  │          │  │ • MySQL           │  │
# MAGIC │  │          │  │ • SQL Server      │  │
# MAGIC │  └──────────┘  └──────────────────┘  │
# MAGIC └───────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- 外部カタログの作成例
# MAGIC CREATE FOREIGN CATALOG postgres_catalog
# MAGIC USING CONNECTION my_postgres_connection;
# MAGIC
# MAGIC -- 外部データベースのテーブルを直接クエリ
# MAGIC SELECT * FROM postgres_catalog.public.users;
# MAGIC ```
# MAGIC
# MAGIC > **試験ポイント**: Lakehouse Federation では外部データベースのデータを
# MAGIC > Unity Catalog の名前空間で直接クエリできます。データのコピーは不要です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. ベストプラクティス
# MAGIC
# MAGIC | カテゴリ | ベストプラクティス |
# MAGIC |---|---|
# MAGIC | カタログ設計 | 環境別（dev/staging/prod）またはビジネスユニット別にカタログを分ける |
# MAGIC | スキーマ設計 | 機能やドメインごとにスキーマを分ける |
# MAGIC | テーブル種類 | マネージドテーブルを優先的に使用する |
# MAGIC | 権限管理 | 最小権限の原則を適用、グループベースで管理 |
# MAGIC | サービスプリンシパル | 本番ジョブには個人アカウントではなくサービスプリンシパルを使用 |
# MAGIC | データ共有 | Delta Sharing で安全にデータを共有 |

# COMMAND ----------

# ガバナンスの確認
print("=== データガバナンス チェックリスト ===")
print()
checklist = [
    ("Unity Catalog 有効化", "ワークスペースに Unity Catalog が紐づいているか"),
    ("カタログ設計", "環境（dev/prod）やビジネスユニットで適切に分離されているか"),
    ("アクセス制御", "最小権限の原則が適用されているか"),
    ("サービスプリンシパル", "本番ジョブは個人アカウントではなく SP を使っているか"),
    ("監査ログ", "誰がいつ何にアクセスしたかを追跡できるか"),
    ("リネージ", "データの来歴を追跡できるか"),
]

for item, desc in checklist:
    print(f"  □ {item}")
    print(f"    → {desc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **Unity Catalog の階層** — Metastore → Catalog → Schema → Table/View
# MAGIC 2. **テーブルの種類** — マネージドテーブルと外部テーブルの違い
# MAGIC 3. **アクセス権限** — GRANT/REVOKE による権限管理、サービスプリンシパル
# MAGIC 4. **外部ロケーション** — Storage Credential と External Location
# MAGIC 5. **Delta Sharing** — 組織間のデータ共有
# MAGIC 6. **データリネージ** — テーブル/カラムレベルのデータ追跡
# MAGIC 7. **Lakehouse Federation** — 外部データベースとの連携
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - **次のノートブック `09_cleanup.py`** でリソースのクリーンアップを行いましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Engineer Associate):
# MAGIC > - **Data Governance & Quality (11%)**: Unity Catalog の階層、テーブル種類、権限管理、外部ロケーション、Delta Sharing、Lakehouse Federation

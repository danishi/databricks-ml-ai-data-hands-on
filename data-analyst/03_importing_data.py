# Databricks notebook source
# MAGIC %md
# MAGIC # データのインポート
# MAGIC
# MAGIC このノートブックでは、Databricks にデータを取り込む**様々な方法**を学びます。
# MAGIC 実務では外部ソースからデータを取り込むことが分析の第一歩です。
# MAGIC
# MAGIC **データインポートとは？**
# MAGIC > 外部ファイル（CSV, JSON等）や外部システム（S3, データベース等）からDatabricksにデータを読み込むことです。
# MAGIC > 料理でいえば「食材（データ）を市場（外部ソース）から仕入れてキッチン（Databricks）に運ぶ」イメージです。
# MAGIC
# MAGIC ### このノートブックで体験する流れ
# MAGIC
# MAGIC ```
# MAGIC ① UIからのアップロード → ② SQLによるファイル読み込み → ③ Auto Loader
# MAGIC                                                              ↓
# MAGIC ⑤ Marketplace データ ← ④ Delta Sharing / 外部接続
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - UI を使ったファイルアップロード
# MAGIC - SQL によるファイルの読み込み（CSV, JSON, Parquet）
# MAGIC - Auto Loader の概要
# MAGIC - Delta Sharing と Lakehouse Federation の概要
# MAGIC - Databricks Marketplace からのデータ取得
# MAGIC
# MAGIC ## 前提条件
# MAGIC - `02_managing_data.py` を実行済みであること
# MAGIC - Databricks Runtime（例: 16.x）のクラスターを使用してください
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
# MAGIC ## 1. データインポート方法の全体像
# MAGIC
# MAGIC Databricks には以下のデータインポート方法があります:
# MAGIC
# MAGIC | 方法 | 説明 | ユースケース |
# MAGIC |---|---|---|
# MAGIC | **UI アップロード** | ブラウザからファイルをドラッグ&ドロップ | 小規模データ、アドホック分析 |
# MAGIC | **SQL ファイル読み込み** | `read_files` 関数でファイルを直接クエリ | ボリューム内のファイル読み込み |
# MAGIC | **COPY INTO** | 外部ストレージから差分データを取り込み | 定期的なバッチ取り込み |
# MAGIC | **Auto Loader** | 新しいファイルを自動検知して取り込み | 継続的なデータ取り込み |
# MAGIC | **Delta Sharing** | 組織間でデータを安全に共有 | 外部パートナーとのデータ共有 |
# MAGIC | **Lakehouse Federation** | 外部DBに直接クエリ | BigQuery, Snowflake 等への接続 |
# MAGIC | **Marketplace** | 公開データセットの取得 | サードパーティデータの活用 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. UI を使ったファイルアップロード
# MAGIC
# MAGIC 最も簡単なデータインポート方法です。ブラウザ上でファイルをアップロードしてテーブルを作成できます。
# MAGIC
# MAGIC ### 手順
# MAGIC
# MAGIC 1. 左サイドバーの **「+新規」** → **「データをアップロードまたは追加」** をクリック
# MAGIC 2. **「テーブルの作成またはボリュームにアップロード」** を選択
# MAGIC 3. CSV、TSV、JSON、Avro、Parquet などのファイルをドラッグ&ドロップ
# MAGIC 4. プレビューでデータを確認し、テーブル名を入力
# MAGIC 5. **「テーブルの作成」** をクリック
# MAGIC
# MAGIC > **認定試験のポイント**: UI からのデータアップロード手順は試験に出ることがあります。
# MAGIC > 対応ファイル形式と、アップロード先（テーブル or ボリューム）の違いを理解しましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SQL によるファイルの読み込み
# MAGIC
# MAGIC Volumes（ボリューム）に配置されたファイルを SQL で直接読み込めます。
# MAGIC
# MAGIC まず、サンプルの CSV データを Python で作成し、SQL で読み込む流れを体験しましょう。

# COMMAND ----------

# サンプルCSVデータを作成してテーブルに格納
import pandas as pd

# サンプルの売上データ
sales_data = pd.DataFrame({
    "date": ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19",
             "2024-01-20", "2024-01-21", "2024-01-22", "2024-01-23", "2024-01-24"],
    "store_id": ["S001", "S002", "S001", "S003", "S002",
                 "S001", "S003", "S002", "S001", "S003"],
    "product_category": ["Electronics", "Clothing", "Electronics", "Food", "Clothing",
                         "Food", "Electronics", "Food", "Clothing", "Electronics"],
    "revenue": [125000, 45000, 89000, 32000, 67000,
                28000, 156000, 41000, 53000, 112000],
    "quantity_sold": [15, 30, 10, 80, 45,
                      70, 12, 55, 35, 8]
})

# Spark DataFrameに変換してテーブルとして保存
spark_df = spark.createDataFrame(sales_data)
spark_df.write.mode("overwrite").saveAsTable("da_daily_sales")

print(f"売上データを da_daily_sales テーブルに保存しました（{len(sales_data)} 行）")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの確認
# MAGIC SELECT * FROM da_daily_sales ORDER BY date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### read_files 関数
# MAGIC
# MAGIC Unity Catalog の **Volumes** にファイルがある場合、`read_files` 関数で直接読み込めます。
# MAGIC
# MAGIC ```sql
# MAGIC -- 構文例（ボリュームにファイルがある場合）
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/catalog/schema/volume_name/file.csv',
# MAGIC   format => 'csv',
# MAGIC   header => 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC | パラメータ | 説明 | 例 |
# MAGIC |---|---|---|
# MAGIC | パス | ファイルの場所 | `/Volumes/main/default/my_volume/data.csv` |
# MAGIC | `format` | ファイル形式 | `csv`, `json`, `parquet`, `avro` |
# MAGIC | `header` | ヘッダー行の有無（CSV用） | `true` / `false` |
# MAGIC | `inferSchema` | スキーマの自動推定 | `true` / `false` |

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: Volumes（ボリューム）とは？**
# MAGIC >
# MAGIC > Unity Catalog で管理されるファイルストレージです。テーブル以外のファイル
# MAGIC > （CSV, 画像, モデルファイルなど）を安全に保管・共有できます。
# MAGIC > 従来の DBFS（Databricks File System）の後継です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. COPY INTO — バッチデータの差分取り込み
# MAGIC
# MAGIC `COPY INTO` は、外部ストレージから**新しいファイルだけ**を取り込むコマンドです。
# MAGIC 同じファイルを二重に取り込むことを防げます。
# MAGIC
# MAGIC ```sql
# MAGIC -- 構文例
# MAGIC COPY INTO my_table
# MAGIC FROM '/path/to/files'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');
# MAGIC ```
# MAGIC
# MAGIC | 特徴 | 説明 |
# MAGIC |---|---|
# MAGIC | **べき等性** | 同じファイルを重複して取り込まない |
# MAGIC | **対応形式** | CSV, JSON, Parquet, Avro, ORC, TEXT |
# MAGIC | **用途** | 定期的なバッチ取り込み |
# MAGIC
# MAGIC > **認定試験のポイント**: COPY INTO はファイルレベルのべき等性を持ちます。
# MAGIC > つまり、既に読み込んだファイルはスキップされます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Auto Loader — 継続的なファイル取り込み
# MAGIC
# MAGIC **Auto Loader** は、クラウドストレージ（S3, ADLS, GCS）に新しいファイルが追加されると
# MAGIC **自動的に検知して取り込む**機能です。
# MAGIC
# MAGIC ```
# MAGIC [S3/ADLS/GCS] → 新ファイル到着 → [Auto Loader が自動検知] → [Delta テーブルに書き込み]
# MAGIC ```
# MAGIC
# MAGIC | 特徴 | 説明 |
# MAGIC |---|---|
# MAGIC | **自動検知** | 新しいファイルを自動的に発見 |
# MAGIC | **スキーマ推定** | ファイルからスキーマを自動推定 |
# MAGIC | **スキーマ進化** | 新しいカラムが追加されても対応 |
# MAGIC | **スケーラブル** | 数百万ファイルでも効率的に処理 |
# MAGIC
# MAGIC ### Auto Loader vs COPY INTO
# MAGIC
# MAGIC | 比較項目 | Auto Loader | COPY INTO |
# MAGIC |---|---|---|
# MAGIC | **処理方式** | ストリーミング（継続的） | バッチ（手動実行） |
# MAGIC | **ファイル検知** | 自動（イベント通知） | 実行時にスキャン |
# MAGIC | **大量ファイル** | 効率的 | ファイル数が多いと遅くなる |
# MAGIC | **推奨ケース** | 継続的データ取り込み | アドホック・定期バッチ |
# MAGIC
# MAGIC > **認定試験のポイント**: Auto Loader と COPY INTO の使い分けは重要です。
# MAGIC > 継続的取り込みには Auto Loader、単発・定期バッチには COPY INTO が推奨されます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delta Sharing — 組織間のデータ共有
# MAGIC
# MAGIC **Delta Sharing** は、組織間でデータを安全に共有するためのオープンプロトコルです。
# MAGIC
# MAGIC ```
# MAGIC [データ提供者] --（Delta Sharing プロトコル）--> [データ利用者]
# MAGIC   Provider                                          Recipient
# MAGIC ```
# MAGIC
# MAGIC | 概念 | 説明 |
# MAGIC |---|---|
# MAGIC | **Provider（提供者）** | データを共有する側。共有するテーブルやスキーマを選択 |
# MAGIC | **Recipient（利用者）** | データを受け取る側。Databricks でなくても利用可能 |
# MAGIC | **Share** | 共有するデータセットの定義 |
# MAGIC
# MAGIC ### 特徴
# MAGIC - データをコピーせず共有（提供者のストレージ上のデータを直接参照）
# MAGIC - 利用者は Databricks 以外のプラットフォームでも利用可能
# MAGIC - アクセス制御と監査ログによるセキュリティ

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Lakehouse Federation — 外部データベースへの直接クエリ
# MAGIC
# MAGIC **Lakehouse Federation** を使うと、外部データベースにデータを移動させることなく
# MAGIC 直接SQLクエリを実行できます。
# MAGIC
# MAGIC | 対応する外部ソース | 説明 |
# MAGIC |---|---|
# MAGIC | **MySQL / PostgreSQL** | リレーショナルデータベース |
# MAGIC | **SQL Server** | Microsoft SQL Server |
# MAGIC | **BigQuery** | Google BigQuery |
# MAGIC | **Snowflake** | Snowflake Data Cloud |
# MAGIC | **Redshift** | Amazon Redshift |
# MAGIC
# MAGIC ### 利用の流れ
# MAGIC
# MAGIC 1. **Connection（接続）** を作成 — 外部DBへの接続情報を定義
# MAGIC 2. **Foreign Catalog（外部カタログ）** を作成 — 外部DBをUnity Catalogに登録
# MAGIC 3. **クエリ実行** — 通常のSQLと同じように外部DBのデータにクエリ
# MAGIC
# MAGIC ```sql
# MAGIC -- 外部カタログの利用例
# MAGIC SELECT * FROM external_catalog.external_schema.external_table;
# MAGIC ```
# MAGIC
# MAGIC > **認定試験のポイント**: Lakehouse Federation の目的（データ移動なしの分析）と
# MAGIC > 対応する外部ソースの種類を理解しておきましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Databricks Marketplace からのデータ取得
# MAGIC
# MAGIC **Databricks Marketplace** では、サードパーティのデータセットを
# MAGIC Unity Catalog に直接取り込むことができます。
# MAGIC
# MAGIC ### Marketplace でのデータ取得手順
# MAGIC
# MAGIC 1. 左サイドバーの **「マーケットプレイス」** をクリック
# MAGIC 2. カテゴリまたはキーワードで検索（例: 「weather」「demographics」）
# MAGIC 3. リスティングの詳細を確認
# MAGIC 4. **「Get instant access」** または **「Request access」** をクリック
# MAGIC 5. カタログとスキーマを選択して取り込み
# MAGIC
# MAGIC | リスティングタイプ | 説明 |
# MAGIC |---|---|
# MAGIC | **即時アクセス** | すぐにデータを利用可能 |
# MAGIC | **リクエストアクセス** | 提供者の承認後に利用可能 |
# MAGIC | **有料リスティング** | 購入手続き後に利用可能 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. 実践: 複数ソースのデータ統合
# MAGIC
# MAGIC 実際の分析では、複数のソースからデータを統合することが多いです。
# MAGIC ここでは、既存のテーブルと新しいデータを統合する例を示します。

# COMMAND ----------

# 店舗マスタデータの作成
store_data = pd.DataFrame({
    "store_id": ["S001", "S002", "S003"],
    "store_name": ["東京本店", "大阪支店", "名古屋支店"],
    "region": ["関東", "関西", "中部"],
    "manager": ["田中太郎", "鈴木花子", "佐藤次郎"]
})

spark_df = spark.createDataFrame(store_data)
spark_df.write.mode("overwrite").saveAsTable("da_stores")

print("店舗マスタを da_stores テーブルに保存しました")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 売上データと店舗マスタを結合して分析
# MAGIC SELECT
# MAGIC   s.store_name,
# MAGIC   s.region,
# MAGIC   d.product_category,
# MAGIC   SUM(d.revenue) AS total_revenue,
# MAGIC   SUM(d.quantity_sold) AS total_quantity
# MAGIC FROM da_daily_sales d
# MAGIC JOIN da_stores s ON d.store_id = s.store_id
# MAGIC GROUP BY s.store_name, s.region, d.product_category
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **データインポート方法の全体像** — UI, SQL, Auto Loader, Delta Sharing 等
# MAGIC 2. **UI アップロード** — ブラウザからのファイルアップロード手順
# MAGIC 3. **SQL による読み込み** — read_files, Volumes
# MAGIC 4. **COPY INTO** — バッチ取り込み（べき等性）
# MAGIC 5. **Auto Loader** — 継続的なファイル取り込み
# MAGIC 6. **Delta Sharing** — 組織間のデータ共有
# MAGIC 7. **Lakehouse Federation** — 外部DBへの直接クエリ
# MAGIC 8. **Marketplace** — サードパーティデータの取得
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - **次のノートブック `04_sql_queries.py`** で SQL クエリの実行を詳しく学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Analyst Associate):
# MAGIC > - **Importing Data (5%)**: UI アップロード、Auto Loader、Delta Sharing、Marketplace

# Databricks notebook source
# MAGIC %md
# MAGIC # データモデリング with Databricks SQL
# MAGIC
# MAGIC このノートブックでは、分析ワークロードに最適な**データモデリング手法**を学びます。
# MAGIC 適切なデータモデルを設計することで、クエリのパフォーマンスと保守性が向上します。
# MAGIC
# MAGIC **データモデリングとは？**
# MAGIC > データの構造（テーブルの分け方、テーブル間の関係）を設計することです。
# MAGIC > 建築でいえば「設計図を描く」工程に相当します。良い設計図がなければ、
# MAGIC > 使いにくい建物ができてしまうのと同じです。
# MAGIC
# MAGIC ### このノートブックで体験する流れ
# MAGIC
# MAGIC ```
# MAGIC ① スタースキーマ → ② スノーフレークスキーマ → ③ データボルト
# MAGIC                                                      ↓
# MAGIC                ⑤ 実践: モデル構築 ← ④ メダリオンアーキテクチャ
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - スタースキーマ（Star Schema）
# MAGIC - スノーフレークスキーマ（Snowflake Schema）
# MAGIC - データボルト（Data Vault）
# MAGIC - メダリオンアーキテクチャ（Bronze / Silver / Gold）
# MAGIC - 非正規化とスキーマ設計のベストプラクティス
# MAGIC
# MAGIC ## 前提条件
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
# MAGIC ## 1. データモデリングの基本概念
# MAGIC
# MAGIC ### 正規化 vs 非正規化
# MAGIC
# MAGIC | 概念 | 説明 | メリット | デメリット |
# MAGIC |---|---|---|---|
# MAGIC | **正規化** | データの重複を排除して分割 | 更新が容易、整合性が高い | JOINが多くなり分析が遅い |
# MAGIC | **非正規化** | 分析しやすいように統合 | JOINが少なく高速 | データの重複、更新コスト |
# MAGIC
# MAGIC > **分析用途のデータベース**では、一般的に**非正規化**（またはスタースキーマ）が推奨されます。
# MAGIC > 分析クエリは読み取りが中心で、JOINが少ないほど高速だからです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. スタースキーマ（Star Schema）
# MAGIC
# MAGIC **スタースキーマ**は、分析用データベースで最も一般的なモデルです。
# MAGIC 中心に**ファクトテーブル**（事実データ）、周りに**ディメンションテーブル**（分析軸）を配置します。
# MAGIC
# MAGIC ```
# MAGIC              [dim_date]
# MAGIC                  |
# MAGIC [dim_customer] - [fact_orders] - [dim_product]
# MAGIC                  |
# MAGIC              [dim_store]
# MAGIC ```
# MAGIC
# MAGIC | テーブル種類 | 説明 | 特徴 | 例 |
# MAGIC |---|---|---|---|
# MAGIC | **ファクトテーブル** | 業務イベント・数値データ | 行数が多い、外部キーを含む | 注文、売上、クリック |
# MAGIC | **ディメンションテーブル** | 分析の切り口 | 行数が少ない、属性情報が豊富 | 顧客、商品、日付、店舗 |
# MAGIC
# MAGIC ### スタースキーマのメリット
# MAGIC
# MAGIC - クエリがシンプル（JOINが1段階）
# MAGIC - パフォーマンスが良い
# MAGIC - ビジネスユーザーにも理解しやすい

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ディメンションテーブル: 日付ディメンション
# MAGIC CREATE OR REPLACE TABLE da_dim_date AS
# MAGIC SELECT
# MAGIC   date AS date_key,
# MAGIC   YEAR(date) AS year,
# MAGIC   MONTH(date) AS month,
# MAGIC   DAY(date) AS day,
# MAGIC   DAYOFWEEK(date) AS day_of_week,
# MAGIC   CASE DAYOFWEEK(date)
# MAGIC     WHEN 1 THEN '日曜'
# MAGIC     WHEN 2 THEN '月曜'
# MAGIC     WHEN 3 THEN '火曜'
# MAGIC     WHEN 4 THEN '水曜'
# MAGIC     WHEN 5 THEN '木曜'
# MAGIC     WHEN 6 THEN '金曜'
# MAGIC     WHEN 7 THEN '土曜'
# MAGIC   END AS day_name,
# MAGIC   QUARTER(date) AS quarter,
# MAGIC   CASE WHEN DAYOFWEEK(date) IN (1, 7) THEN true ELSE false END AS is_weekend
# MAGIC FROM (
# MAGIC   SELECT EXPLODE(SEQUENCE(DATE '2024-01-01', DATE '2024-12-31', INTERVAL 1 DAY)) AS date
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 日付ディメンションの確認（先頭10行）
# MAGIC SELECT * FROM da_dim_date LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ディメンションテーブル: 商品ディメンション
# MAGIC CREATE OR REPLACE TABLE da_dim_product AS
# MAGIC SELECT
# MAGIC   product_id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   price AS unit_price,
# MAGIC   CASE
# MAGIC     WHEN price >= 50000 THEN '高価格帯'
# MAGIC     WHEN price >= 10000 THEN '中価格帯'
# MAGIC     ELSE '低価格帯'
# MAGIC   END AS price_tier
# MAGIC FROM da_sample_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM da_dim_product;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ディメンションテーブル: 顧客ディメンション
# MAGIC CREATE OR REPLACE TABLE da_dim_customer AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COALESCE(name, '不明') AS name,
# MAGIC   COALESCE(city, '不明') AS city,
# MAGIC   COALESCE(segment, 'Standard') AS segment,
# MAGIC   registration_date
# MAGIC FROM da_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM da_dim_customer;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ファクトテーブル: 注文ファクト
# MAGIC CREATE OR REPLACE TABLE da_fact_orders AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date AS date_key,
# MAGIC   product_name,
# MAGIC   quantity,
# MAGIC   unit_price,
# MAGIC   total_amount,
# MAGIC   status
# MAGIC FROM da_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- スタースキーマを使った分析クエリ
# MAGIC -- JOINが1段階で、各ディメンションの属性でフィルタ・グループ化できる
# MAGIC SELECT
# MAGIC   d.year,
# MAGIC   d.month,
# MAGIC   d.day_name,
# MAGIC   dc.segment AS customer_segment,
# MAGIC   dp.category AS product_category,
# MAGIC   dp.price_tier,
# MAGIC   COUNT(f.order_id) AS order_count,
# MAGIC   SUM(f.total_amount) AS total_revenue
# MAGIC FROM da_fact_orders f
# MAGIC JOIN da_dim_date d ON f.date_key = d.date_key
# MAGIC JOIN da_dim_customer dc ON f.customer_id = dc.customer_id
# MAGIC JOIN da_dim_product dp ON f.product_name = dp.product_name
# MAGIC WHERE f.status = 'completed'
# MAGIC GROUP BY d.year, d.month, d.day_name, dc.segment, dp.category, dp.price_tier
# MAGIC ORDER BY d.year, d.month, total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: ファクトテーブルとディメンションテーブル**
# MAGIC >
# MAGIC > - **ファクト（事実）**: 「何が起きたか」を記録（注文した、クリックした、出荷した）
# MAGIC > - **ディメンション（次元）**: 「誰が、何を、いつ、どこで」という分析の切り口
# MAGIC >
# MAGIC > 例: 「田中さんが1月15日にノートPCを購入した」
# MAGIC > → ファクト: 注文イベント、ディメンション: 顧客（田中さん）、日付（1月15日）、商品（ノートPC）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. スノーフレークスキーマ（Snowflake Schema）
# MAGIC
# MAGIC **スノーフレークスキーマ**は、ディメンションテーブルをさらに正規化したモデルです。
# MAGIC
# MAGIC ```
# MAGIC                [dim_subcategory]
# MAGIC                      |
# MAGIC              [dim_product] ← [dim_brand]
# MAGIC                  |
# MAGIC [dim_customer] - [fact_orders] - [dim_date]
# MAGIC      |
# MAGIC [dim_region]
# MAGIC ```
# MAGIC
# MAGIC ### スタースキーマ vs スノーフレークスキーマ
# MAGIC
# MAGIC | 比較項目 | スタースキーマ | スノーフレークスキーマ |
# MAGIC |---|---|---|
# MAGIC | **ディメンションの構造** | 非正規化（1テーブル） | 正規化（複数テーブル） |
# MAGIC | **JOIN数** | 少ない | 多い |
# MAGIC | **クエリの複雑さ** | シンプル | やや複雑 |
# MAGIC | **ストレージ** | やや冗長 | 効率的 |
# MAGIC | **メンテナンス** | 更新時の影響範囲大 | 更新が容易 |
# MAGIC | **推奨用途** | BIツール、アドホック分析 | 大規模DWH |
# MAGIC
# MAGIC > **認定試験のポイント**: スタースキーマとスノーフレークスキーマの違い、
# MAGIC > それぞれのメリット・デメリットを理解しておきましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. データボルト（Data Vault）
# MAGIC
# MAGIC **データボルト**は、大規模なエンタープライズDWH向けのモデリング手法です。
# MAGIC
# MAGIC | コンポーネント | 説明 | 例 |
# MAGIC |---|---|---|
# MAGIC | **Hub（ハブ）** | ビジネスエンティティの一意キー | 顧客Hub、商品Hub |
# MAGIC | **Link（リンク）** | エンティティ間の関係 | 注文Link（顧客-商品の関係） |
# MAGIC | **Satellite（サテライト）** | 属性・記述データ（変更履歴付き） | 顧客属性、商品属性 |
# MAGIC
# MAGIC ```
# MAGIC [Sat_Customer_Details]     [Sat_Product_Details]
# MAGIC         |                          |
# MAGIC   [Hub_Customer] --- [Link_Order] --- [Hub_Product]
# MAGIC                          |
# MAGIC                  [Sat_Order_Details]
# MAGIC ```
# MAGIC
# MAGIC ### データボルトの特徴
# MAGIC
# MAGIC - ソースシステムの変更に強い
# MAGIC - 変更履歴を自然に保持
# MAGIC - 並列開発が容易
# MAGIC - 主にデータウェアハウスのステージング層で使用
# MAGIC
# MAGIC > **認定試験のポイント**: データボルトの3つのコンポーネント（Hub, Link, Satellite）の
# MAGIC > 役割と特徴を理解しておきましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. メダリオンアーキテクチャ（Bronze / Silver / Gold）
# MAGIC
# MAGIC **メダリオンアーキテクチャ**は、Databricks が推奨するデータレイクの層構造です。
# MAGIC データの品質を段階的に高めていく設計パターンです。
# MAGIC
# MAGIC ```
# MAGIC [外部ソース] → [Bronze（生データ）] → [Silver（クリーニング済み）] → [Gold（分析用集計）]
# MAGIC ```
# MAGIC
# MAGIC | 層 | 説明 | データ品質 | 用途 |
# MAGIC |---|---|---|---|
# MAGIC | **Bronze（ブロンズ）** | 生データをそのまま取り込み | 低（未加工） | データの保管、再処理用 |
# MAGIC | **Silver（シルバー）** | クリーニング・正規化済み | 中（品質保証済み） | アドホック分析、結合 |
# MAGIC | **Gold（ゴールド）** | ビジネス用集計テーブル | 高（分析最適化） | ダッシュボード、レポート |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze 層: 生データの取り込み（元データをそのまま保存）
# MAGIC CREATE OR REPLACE TABLE da_bronze_orders AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   current_timestamp() AS ingested_at,
# MAGIC   'source_system_a' AS data_source
# MAGIC FROM da_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver 層: クリーニング済みデータ
# MAGIC CREATE OR REPLACE TABLE da_silver_orders AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   product_name,
# MAGIC   quantity,
# MAGIC   unit_price,
# MAGIC   total_amount,
# MAGIC   status,
# MAGIC   ingested_at,
# MAGIC   data_source,
# MAGIC   current_timestamp() AS processed_at
# MAGIC FROM da_bronze_orders
# MAGIC WHERE total_amount >= 0
# MAGIC   AND quantity > 0
# MAGIC   AND order_id IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold 層: 分析用集計テーブル
# MAGIC CREATE OR REPLACE TABLE da_gold_daily_revenue AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   COUNT(DISTINCT order_id) AS order_count,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC   SUM(total_amount) AS total_revenue,
# MAGIC   AVG(total_amount) AS avg_order_value,
# MAGIC   current_timestamp() AS aggregated_at
# MAGIC FROM da_silver_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold 層のデータを確認
# MAGIC SELECT * FROM da_gold_daily_revenue ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### メダリオンアーキテクチャとデータモデルの関係
# MAGIC
# MAGIC | メダリオン層 | 適用されるモデル |
# MAGIC |---|---|
# MAGIC | **Bronze** | ソースシステムのスキーマをそのまま保持 |
# MAGIC | **Silver** | 正規化されたテーブル、データボルト |
# MAGIC | **Gold** | スタースキーマ、集計テーブル、非正規化ビュー |
# MAGIC
# MAGIC > **認定試験のポイント**: メダリオンアーキテクチャの各層の目的と、
# MAGIC > 各層に適したデータモデリング手法の関連を理解しておきましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. スキーマ設計のベストプラクティス
# MAGIC
# MAGIC | カテゴリ | ベストプラクティス |
# MAGIC |---|---|
# MAGIC | **命名規則** | テーブル名は意味のある名前（`fact_orders`, `dim_customer`） |
# MAGIC | **コメント** | テーブル・カラムにコメントを付与 |
# MAGIC | **データ型** | 適切なデータ型を選択（不要に大きな型を使わない） |
# MAGIC | **主キー** | ファクトテーブルには明確なキーを定義 |
# MAGIC | **外部キー** | ディメンションテーブルへの参照を明確に |
# MAGIC | **パーティション** | 大規模テーブルには適切なパーティション設計 |
# MAGIC | **クラスタリング** | フィルタ頻度の高いカラムで Liquid Clustering |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **正規化 vs 非正規化** — 分析用途では非正規化が推奨
# MAGIC 2. **スタースキーマ** — ファクトテーブル + ディメンションテーブル
# MAGIC 3. **スノーフレークスキーマ** — ディメンションの正規化
# MAGIC 4. **データボルト** — Hub, Link, Satellite の3コンポーネント
# MAGIC 5. **メダリオンアーキテクチャ** — Bronze / Silver / Gold の層構造
# MAGIC 6. **スキーマ設計のベストプラクティス** — 命名規則、コメント、パーティション
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - Gold 層のテーブルを使ったダッシュボードを作成してみましょう
# MAGIC - **次のノートブック `09_securing_data.py`** でデータセキュリティを学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Analyst Associate):
# MAGIC > - **Data Modeling with Databricks SQL (5%)**: スタースキーマ、スノーフレーク、データボルト、メダリオン

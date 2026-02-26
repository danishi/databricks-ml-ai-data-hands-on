# Databricks notebook source
# MAGIC %md
# MAGIC # SQL DDL/DML 操作とデバッグ
# MAGIC
# MAGIC このノートブックでは、Databricks 上での SQL の **DDL（Data Definition Language）** と
# MAGIC **DML（Data Manipulation Language）** 操作を体系的に学びます。
# MAGIC
# MAGIC **DDL とは？**
# MAGIC > データの「入れ物」を定義する操作です。テーブルやビューの作成・変更・削除など。
# MAGIC > 例: `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`
# MAGIC
# MAGIC **DML とは？**
# MAGIC > データそのものを操作する命令です。挿入・更新・削除など。
# MAGIC > 例: `INSERT`, `UPDATE`, `DELETE`, `MERGE`
# MAGIC
# MAGIC ### DDL と DML の関係
# MAGIC
# MAGIC ```
# MAGIC DDL（器を作る）           DML（中身を操作する）
# MAGIC ┌──────────────┐        ┌──────────────┐
# MAGIC │ CREATE TABLE │  ────→ │ INSERT       │  データを入れる
# MAGIC │ ALTER TABLE  │        │ UPDATE       │  データを変更する
# MAGIC │ DROP TABLE   │        │ DELETE       │  データを削除する
# MAGIC └──────────────┘        │ MERGE        │  条件付き更新/挿入
# MAGIC                         └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - テーブルの作成（CREATE TABLE / CTAS）
# MAGIC - データの挿入（INSERT / INSERT OVERWRITE）
# MAGIC - データの更新と削除（UPDATE / DELETE）
# MAGIC - MERGE（UPSERT）によるデータ同期
# MAGIC - Databricks Connect の概要
# MAGIC - デバッグツールの活用
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime（例: 16.x）のクラスター
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. DDL: テーブルの作成
# MAGIC
# MAGIC ### CREATE TABLE の種類
# MAGIC
# MAGIC | 構文 | 説明 | 用途 |
# MAGIC |---|---|---|
# MAGIC | `CREATE TABLE` | スキーマを定義してテーブル作成 | 空テーブルの作成 |
# MAGIC | `CREATE TABLE AS SELECT (CTAS)` | クエリ結果からテーブル作成 | データ変換結果の保存 |
# MAGIC | `CREATE OR REPLACE TABLE` | 既存テーブルを上書き作成 | 冪等なテーブル作成 |
# MAGIC | `CREATE TABLE IF NOT EXISTS` | 存在しない場合のみ作成 | 安全なテーブル作成 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの作成（スキーマ定義）
# MAGIC CREATE OR REPLACE TABLE default.products (
# MAGIC   product_id INT,
# MAGIC   product_name STRING,
# MAGIC   category STRING,
# MAGIC   price DECIMAL(10, 2),
# MAGIC   stock INT,
# MAGIC   created_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの詳細を確認
# MAGIC DESCRIBE TABLE EXTENDED default.products

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: マネージドテーブル vs 外部テーブル**
# MAGIC >
# MAGIC > - **マネージドテーブル**: Databricks がデータとメタデータの両方を管理。`DROP TABLE` でデータも削除される。
# MAGIC > - **外部テーブル**: メタデータのみ Databricks が管理。データは外部ストレージに残る。
# MAGIC > `LOCATION` を指定して作成すると外部テーブルになります。
# MAGIC >
# MAGIC > ```sql
# MAGIC > -- マネージドテーブル
# MAGIC > CREATE TABLE my_table (id INT);
# MAGIC >
# MAGIC > -- 外部テーブル
# MAGIC > CREATE TABLE my_table (id INT) LOCATION 's3://bucket/path';
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DML: INSERT によるデータ挿入
# MAGIC
# MAGIC | 構文 | 説明 |
# MAGIC |---|---|
# MAGIC | `INSERT INTO` | 既存データに追加 |
# MAGIC | `INSERT OVERWRITE` | 既存データを上書き |
# MAGIC | `INSERT INTO ... SELECT` | クエリ結果を挿入 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT INTO: データを追加
# MAGIC INSERT INTO default.products (product_id, product_name, category, price, stock)
# MAGIC VALUES
# MAGIC   (1, 'ノートPC Pro', 'PC', 150000.00, 50),
# MAGIC   (2, 'ワイヤレスマウス', '周辺機器', 3500.00, 200),
# MAGIC   (3, '4Kモニター', 'ディスプレイ', 45000.00, 30),
# MAGIC   (4, 'メカニカルキーボード', '周辺機器', 12000.00, 100),
# MAGIC   (5, 'USBハブ', '周辺機器', 2500.00, 300),
# MAGIC   (6, 'ノートPC Basic', 'PC', 80000.00, 80),
# MAGIC   (7, 'ゲーミングヘッドセット', '周辺機器', 8000.00, 150),
# MAGIC   (8, 'ウェブカメラ', '周辺機器', 5000.00, 120)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 挿入されたデータを確認
# MAGIC SELECT * FROM default.products ORDER BY product_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT OVERWRITE: テーブルのデータを完全に上書き
# MAGIC -- 注意: 既存データがすべて削除され、新しいデータに置き換わります
# MAGIC INSERT OVERWRITE default.products
# MAGIC SELECT * FROM default.products WHERE category = 'PC'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PC カテゴリのデータのみが残っていることを確認
# MAGIC SELECT * FROM default.products ORDER BY product_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データを元に戻す（再度全データを挿入）
# MAGIC INSERT OVERWRITE default.products (product_id, product_name, category, price, stock)
# MAGIC VALUES
# MAGIC   (1, 'ノートPC Pro', 'PC', 150000.00, 50),
# MAGIC   (2, 'ワイヤレスマウス', '周辺機器', 3500.00, 200),
# MAGIC   (3, '4Kモニター', 'ディスプレイ', 45000.00, 30),
# MAGIC   (4, 'メカニカルキーボード', '周辺機器', 12000.00, 100),
# MAGIC   (5, 'USBハブ', '周辺機器', 2500.00, 300),
# MAGIC   (6, 'ノートPC Basic', 'PC', 80000.00, 80),
# MAGIC   (7, 'ゲーミングヘッドセット', '周辺機器', 8000.00, 150),
# MAGIC   (8, 'ウェブカメラ', '周辺機器', 5000.00, 120)

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: `INSERT INTO` はデータを追加、`INSERT OVERWRITE` はデータを完全に置き換えます。
# MAGIC > `INSERT OVERWRITE` はパーティション単位での上書きが可能です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DML: UPDATE と DELETE
# MAGIC
# MAGIC Delta Lake では、従来のデータレイクでは困難だった `UPDATE` と `DELETE` を直接実行できます。
# MAGIC
# MAGIC > **Delta Lake の ACID トランザクション** により、途中で失敗しても
# MAGIC > データが不整合な状態になることはありません。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UPDATE: 価格を一括で10%値上げ（PC カテゴリのみ）
# MAGIC UPDATE default.products
# MAGIC SET price = price * 1.1
# MAGIC WHERE category = 'PC'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 更新結果を確認（PC カテゴリの価格が上がっている）
# MAGIC SELECT product_id, product_name, category, price
# MAGIC FROM default.products
# MAGIC ORDER BY product_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DELETE: 在庫が少ない商品を削除
# MAGIC DELETE FROM default.products
# MAGIC WHERE stock < 50

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 削除結果を確認（stock < 50 の商品が削除されている）
# MAGIC SELECT product_id, product_name, stock
# MAGIC FROM default.products
# MAGIC ORDER BY product_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MERGE（UPSERT）: 条件付き更新/挿入
# MAGIC
# MAGIC **MERGE** は「存在すれば更新、存在しなければ挿入」を1つの文で実行できる強力な操作です。
# MAGIC 「UPSERT」（UPDATE + INSERT）とも呼ばれます。
# MAGIC
# MAGIC ### MERGE の動作イメージ
# MAGIC
# MAGIC ```
# MAGIC ソースデータ（新データ）          ターゲットテーブル（既存データ）
# MAGIC ┌──────────────┐               ┌──────────────┐
# MAGIC │ product_id=2 │  ── MATCHED ──→ │ UPDATE       │  既存データを更新
# MAGIC │ product_id=9 │  ── NOT MATCHED → │ INSERT       │  新規データを挿入
# MAGIC └──────────────┘               └──────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ソースデータ（更新+新規）を作成
# MAGIC CREATE OR REPLACE TEMP VIEW product_updates AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (2, 'ワイヤレスマウス v2', '周辺機器', 3800.00, 250),  -- 既存商品の更新
# MAGIC   (4, 'メカニカルキーボード', '周辺機器', 13000.00, 90),   -- 既存商品の更新
# MAGIC   (9, 'ドッキングステーション', '周辺機器', 15000.00, 60), -- 新商品
# MAGIC   (10, 'ポータブルSSD', 'ストレージ', 10000.00, 200)      -- 新商品
# MAGIC AS updates(product_id, product_name, category, price, stock)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE（UPSERT）の実行
# MAGIC MERGE INTO default.products AS target
# MAGIC USING product_updates AS source
# MAGIC ON target.product_id = source.product_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.product_name = source.product_name,
# MAGIC     target.price = source.price,
# MAGIC     target.stock = source.stock
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (product_id, product_name, category, price, stock)
# MAGIC   VALUES (source.product_id, source.product_name, source.category, source.price, source.stock)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE 後の結果を確認
# MAGIC SELECT * FROM default.products ORDER BY product_id

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: `MERGE` は Delta Lake の重要な機能です。
# MAGIC > `WHEN MATCHED` と `WHEN NOT MATCHED` の条件を組み合わせて、
# MAGIC > 更新・挿入・削除を1つの文で実行できます。
# MAGIC >
# MAGIC > `WHEN MATCHED AND condition THEN DELETE` のように削除条件を追加することもできます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. CTAS と一時ビュー
# MAGIC
# MAGIC ### CTAS（CREATE TABLE AS SELECT）
# MAGIC
# MAGIC クエリ結果から直接テーブルを作成できます。ETL パイプラインでよく使われます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTAS: カテゴリ別集計テーブルを作成
# MAGIC CREATE OR REPLACE TABLE default.product_summary AS
# MAGIC SELECT
# MAGIC   category,
# MAGIC   COUNT(*) AS product_count,
# MAGIC   ROUND(AVG(price), 2) AS avg_price,
# MAGIC   SUM(stock) AS total_stock
# MAGIC FROM default.products
# MAGIC GROUP BY category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.product_summary ORDER BY product_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### ビューの種類
# MAGIC
# MAGIC | 種類 | 説明 | ライフサイクル |
# MAGIC |---|---|---|
# MAGIC | **ビュー** (`CREATE VIEW`) | 永続的なビュー（カタログに保存） | 明示的に削除するまで存続 |
# MAGIC | **一時ビュー** (`CREATE TEMP VIEW`) | セッション中のみ有効 | セッション終了で消滅 |
# MAGIC | **グローバル一時ビュー** | 全ノートブックから参照可能 | クラスター停止で消滅 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 一時ビューの作成
# MAGIC CREATE OR REPLACE TEMP VIEW expensive_products AS
# MAGIC SELECT * FROM default.products WHERE price > 10000;
# MAGIC
# MAGIC -- 一時ビューを参照
# MAGIC SELECT * FROM expensive_products ORDER BY price DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Databricks Connect
# MAGIC
# MAGIC **Databricks Connect** を使うと、ローカルの IDE（VS Code、PyCharm など）から
# MAGIC Databricks クラスターに接続してコードを実行できます。
# MAGIC
# MAGIC ### Databricks Connect の仕組み
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────┐         ┌──────────────────┐
# MAGIC │  ローカル IDE      │         │  Databricks      │
# MAGIC │  (VS Code 等)     │ ──────→ │  クラスター        │
# MAGIC │  • コード編集      │  接続   │  • Spark 実行     │
# MAGIC │  • デバッグ        │         │  • データアクセス   │
# MAGIC │  • テスト          │         │                  │
# MAGIC └──────────────────┘         └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### メリット
# MAGIC
# MAGIC | メリット | 説明 |
# MAGIC |---|---|
# MAGIC | ローカル開発 | IDE のデバッグ機能やコード補完が使える |
# MAGIC | バージョン管理 | Git との統合が容易 |
# MAGIC | テスト | ローカルでユニットテストを実行可能 |
# MAGIC | CI/CD | パイプラインに組み込みやすい |
# MAGIC
# MAGIC ### セットアップ手順（概要）
# MAGIC
# MAGIC ```bash
# MAGIC # 1. パッケージのインストール
# MAGIC pip install databricks-connect
# MAGIC
# MAGIC # 2. 接続の設定
# MAGIC databricks-connect configure
# MAGIC
# MAGIC # 3. 接続テスト
# MAGIC databricks-connect test
# MAGIC ```
# MAGIC
# MAGIC > **試験ポイント**: Databricks Connect を使うと、ローカル IDE から Databricks クラスターに接続し、
# MAGIC > Spark コードをリモート実行できます。開発・テスト・デバッグのワークフローを改善します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. デバッグツール
# MAGIC
# MAGIC Databricks にはデータエンジニアリングの問題を特定・解決するためのツールが用意されています。
# MAGIC
# MAGIC ### 主なデバッグツール
# MAGIC
# MAGIC | ツール | 説明 | アクセス方法 |
# MAGIC |---|---|---|
# MAGIC | **Spark UI** | ジョブの実行計画と進捗を確認 | クラスター → Spark UI |
# MAGIC | **ドライバーログ** | エラーメッセージやprint出力を確認 | クラスター → ドライバーログ |
# MAGIC | **クエリプロファイル** | SQL クエリの実行計画を可視化 | SQL エディタ → クエリプロファイル |
# MAGIC | **EXPLAIN** | クエリの論理/物理実行計画を表示 | `EXPLAIN SELECT ...` |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXPLAIN でクエリの実行計画を確認
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT p.product_name, p.price, s.product_count
# MAGIC FROM default.products p
# MAGIC JOIN default.product_summary s ON p.category = s.category
# MAGIC WHERE p.price > 5000

# COMMAND ----------

# Python でのデバッグテクニック

# 1. DataFrame の実行計画を確認
df = spark.table("default.products")
print("=== 論理実行計画 ===")
df.filter("price > 5000").select("product_name", "price").explain(True)

# COMMAND ----------

# 2. キャッシュを使ったデバッグ（中間結果を確認）
from pyspark.sql import functions as F

df_debug = (
    spark.table("default.products")
    .filter(F.col("price") > 5000)
    .groupBy("category")
    .agg(F.count("*").alias("count"), F.avg("price").alias("avg_price"))
)

# 中間結果を表示
print("=== デバッグ: 集計結果 ===")
display(df_debug)

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: Spark UI はジョブの実行状況を可視化し、
# MAGIC > ボトルネック（データスキュー、シャッフル過多など）の特定に役立ちます。
# MAGIC > `EXPLAIN` コマンドでクエリの実行計画を確認し、最適化のヒントを得られます。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **DDL** — CREATE TABLE、CTAS、ビューの作成
# MAGIC 2. **DML: INSERT** — INSERT INTO と INSERT OVERWRITE の違い
# MAGIC 3. **DML: UPDATE/DELETE** — Delta Lake の ACID トランザクション
# MAGIC 4. **MERGE (UPSERT)** — 条件付き更新/挿入の一括処理
# MAGIC 5. **Databricks Connect** — ローカル IDE からの接続開発
# MAGIC 6. **デバッグツール** — Spark UI、EXPLAIN、ドライバーログ
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - **次のノートブック `04_delta_lake.py`** で Delta Lake の高度な機能を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Engineer Associate):
# MAGIC > - **Development and Ingestion (30%)**: Databricks Connect、ノートブック機能、デバッグツール
# MAGIC > - **Data Processing & Transformations (31%)**: DDL/DML 操作、MERGE/UPSERT

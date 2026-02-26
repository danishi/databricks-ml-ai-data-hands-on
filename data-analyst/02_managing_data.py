# Databricks notebook source
# MAGIC %md
# MAGIC # データの管理
# MAGIC
# MAGIC このノートブックでは、Unity Catalog を使った**データの管理・クリーニング**を学びます。
# MAGIC データアナリストとして日常的に行う、テーブルの作成・変更・削除、データのクリーニング、
# MAGIC タグ付けなどの操作を実践します。
# MAGIC
# MAGIC **データ管理とは？**
# MAGIC > 分析に使えるよう、データを整理・品質管理し、他のユーザーが発見・利用しやすい状態に保つことです。
# MAGIC > 「きれいに整理された本棚」のように、必要なデータをすぐに見つけて使える環境を作ります。
# MAGIC
# MAGIC ### このノートブックで体験する流れ
# MAGIC
# MAGIC ```
# MAGIC ① テーブルの作成・変更 → ② データのクリーニング → ③ タグとコメントの付与
# MAGIC                                                          ↓
# MAGIC ⑤ 認定データセットの管理 ← ④ ビューの活用
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - テーブルの作成・変更・削除（DDL操作）
# MAGIC - データのクリーニング（NULL処理、型変換、重複除去）
# MAGIC - テーブルへのタグ・コメント付与
# MAGIC - ビューを使ったデータの整理
# MAGIC
# MAGIC ## 前提条件
# MAGIC - `01_databricks_platform.py` を実行済みであること
# MAGIC - Databricks Runtime（例: 16.x）のクラスターを使用してください
# MAGIC
# MAGIC > **初心者の方へ**: 各セルのコードは**上から順番に実行**してください。
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
# MAGIC ## 1. テーブルの作成（CREATE TABLE）
# MAGIC
# MAGIC EC サイトのサンプルデータを作成して、データ管理の操作を学びます。
# MAGIC
# MAGIC まず、**顧客テーブル**と**注文テーブル**を作成します。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 顧客テーブルの作成
# MAGIC CREATE OR REPLACE TABLE da_customers (
# MAGIC   customer_id INT,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   city STRING,
# MAGIC   registration_date DATE,
# MAGIC   segment STRING
# MAGIC )
# MAGIC COMMENT '顧客マスタテーブル（データアナリストハンズオン用）';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 顧客データの挿入（クリーニング対象のデータを含む）
# MAGIC INSERT OVERWRITE da_customers VALUES
# MAGIC   (1, '田中太郎', 'tanaka@example.com', '東京', '2023-01-15', 'Premium'),
# MAGIC   (2, '鈴木花子', 'suzuki@example.com', '大阪', '2023-03-20', 'Standard'),
# MAGIC   (3, '佐藤次郎', NULL, '名古屋', '2023-05-10', 'Premium'),
# MAGIC   (4, '高橋美咲', 'takahashi@example.com', NULL, '2023-07-01', 'Standard'),
# MAGIC   (5, '伊藤健一', 'ito@example.com', '東京', '2023-02-28', NULL),
# MAGIC   (6, '渡辺由美', 'watanabe@example.com', '福岡', '2023-09-15', 'Premium'),
# MAGIC   (7, '山本一郎', 'yamamoto@example.com', '東京', '2023-04-05', 'Standard'),
# MAGIC   (8, NULL, 'unknown@example.com', '札幌', '2023-06-12', 'Standard'),
# MAGIC   (9, '中村真理', 'nakamura@example.com', '大阪', '2023-08-20', 'Premium'),
# MAGIC   (10, '小林誠', 'kobayashi@example.com', '東京', NULL, 'Standard');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 注文テーブルの作成
# MAGIC CREATE OR REPLACE TABLE da_orders (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   product_name STRING,
# MAGIC   quantity INT,
# MAGIC   unit_price DECIMAL(10,2),
# MAGIC   total_amount DECIMAL(10,2),
# MAGIC   status STRING
# MAGIC )
# MAGIC COMMENT '注文トランザクションテーブル（データアナリストハンズオン用）';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE da_orders VALUES
# MAGIC   (1001, 1, '2024-01-05', 'ノートPC', 1, 89800.00, 89800.00, 'completed'),
# MAGIC   (1002, 2, '2024-01-10', 'ワイヤレスマウス', 2, 3980.00, 7960.00, 'completed'),
# MAGIC   (1003, 1, '2024-01-15', 'ヘッドセット', 1, 12800.00, 12800.00, 'completed'),
# MAGIC   (1004, 3, '2024-02-01', 'デスクチェア', 1, 29800.00, 29800.00, 'completed'),
# MAGIC   (1005, 5, '2024-02-10', 'ノートPC', 1, 89800.00, 89800.00, 'cancelled'),
# MAGIC   (1006, 4, '2024-02-15', 'USBハブ', 3, 2480.00, 7440.00, 'completed'),
# MAGIC   (1007, 2, '2024-03-01', 'ウェブカメラ', 1, 7980.00, 7980.00, 'completed'),
# MAGIC   (1008, 6, '2024-03-10', 'スタンディングデスク', 1, 45800.00, 45800.00, 'pending'),
# MAGIC   (1009, 7, '2024-03-15', 'モニターアーム', 2, 5980.00, 11960.00, 'completed'),
# MAGIC   (1010, 1, '2024-04-01', 'ワイヤレスマウス', 1, 3980.00, 3980.00, 'completed'),
# MAGIC   (1011, 9, '2024-04-10', 'ヘッドセット', 1, 12800.00, -12800.00, 'completed'),
# MAGIC   (1012, 3, '2024-04-15', 'USBハブ', 0, 2480.00, 0.00, 'completed');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 作成したテーブルを確認
# MAGIC SELECT * FROM da_customers ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM da_orders ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. データクリーニング — NULL値の処理
# MAGIC
# MAGIC 実務のデータには欠損値（NULL）が含まれることがよくあります。
# MAGIC NULL値の検出と処理方法を学びましょう。
# MAGIC
# MAGIC | 処理方法 | SQL関数 | 説明 |
# MAGIC |---|---|---|
# MAGIC | **NULL検出** | `IS NULL` / `IS NOT NULL` | NULLかどうかの判定 |
# MAGIC | **デフォルト値で置換** | `COALESCE(col, default)` | NULLの場合にデフォルト値を返す |
# MAGIC | **条件付き置換** | `IFNULL(col, default)` | NULLの場合にデフォルト値を返す |
# MAGIC | **NULLを除外** | `WHERE col IS NOT NULL` | NULLの行を除外してフィルタリング |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- NULL値を含む行を確認
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   name,
# MAGIC   email,
# MAGIC   city,
# MAGIC   segment,
# MAGIC   CASE WHEN name IS NULL THEN 'NULL' ELSE 'OK' END AS name_status,
# MAGIC   CASE WHEN email IS NULL THEN 'NULL' ELSE 'OK' END AS email_status,
# MAGIC   CASE WHEN city IS NULL THEN 'NULL' ELSE 'OK' END AS city_status,
# MAGIC   CASE WHEN segment IS NULL THEN 'NULL' ELSE 'OK' END AS segment_status
# MAGIC FROM da_customers
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 各カラムのNULL件数を集計
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS null_name,
# MAGIC   SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_email,
# MAGIC   SUM(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS null_city,
# MAGIC   SUM(CASE WHEN segment IS NULL THEN 1 ELSE 0 END) AS null_segment,
# MAGIC   SUM(CASE WHEN registration_date IS NULL THEN 1 ELSE 0 END) AS null_reg_date
# MAGIC FROM da_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- COALESCE を使ってNULL値をデフォルト値で置換して表示
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   COALESCE(name, '不明') AS name,
# MAGIC   COALESCE(email, 'no-email@example.com') AS email,
# MAGIC   COALESCE(city, '未設定') AS city,
# MAGIC   COALESCE(segment, 'Standard') AS segment
# MAGIC FROM da_customers
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: COALESCE とは？**
# MAGIC >
# MAGIC > `COALESCE(値1, 値2, 値3, ...)` は、左から順に見て**最初にNULLでない値**を返します。
# MAGIC > 例: `COALESCE(NULL, NULL, '代替値')` → `'代替値'`
# MAGIC > Excel の `IFERROR` に似た考え方です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. データクリーニング — 不正データの検出・除去
# MAGIC
# MAGIC NULL以外にも、以下のような不正データが存在することがあります:
# MAGIC
# MAGIC - **異常値**: マイナスの金額、ゼロの数量
# MAGIC - **不整合**: ステータスと値の矛盾
# MAGIC - **重複**: 同じレコードの複数登録

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 注文データの異常値を検出
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   product_name,
# MAGIC   quantity,
# MAGIC   total_amount,
# MAGIC   CASE
# MAGIC     WHEN total_amount < 0 THEN '異常: マイナス金額'
# MAGIC     WHEN quantity = 0 THEN '異常: 数量ゼロ'
# MAGIC     WHEN quantity < 0 THEN '異常: マイナス数量'
# MAGIC     ELSE '正常'
# MAGIC   END AS data_quality
# MAGIC FROM da_orders
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- クリーンなデータのみを抽出するビューを作成
# MAGIC CREATE OR REPLACE VIEW da_orders_clean AS
# MAGIC SELECT *
# MAGIC FROM da_orders
# MAGIC WHERE total_amount >= 0
# MAGIC   AND quantity > 0
# MAGIC   AND status = 'completed';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- クリーン済みデータの確認
# MAGIC SELECT * FROM da_orders_clean ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC > **試してみよう**: 他にどんなデータ品質チェックが考えられますか？
# MAGIC > 例えば `total_amount != quantity * unit_price` の行を検出してみましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. データ型の変換（CAST）
# MAGIC
# MAGIC データ型の変換は、データクリーニングの重要なステップです。
# MAGIC
# MAGIC | 変換方法 | 構文 | 例 |
# MAGIC |---|---|---|
# MAGIC | **CAST** | `CAST(col AS type)` | `CAST('123' AS INT)` |
# MAGIC | **::演算子** | `col::type` | `'123'::INT` |
# MAGIC | **専用関数** | `TO_DATE()`, `TO_TIMESTAMP()` | `TO_DATE('2024-01-01')` |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データ型変換の例
# MAGIC SELECT
# MAGIC   CAST(12345 AS STRING) AS int_to_string,
# MAGIC   CAST('99.99' AS DECIMAL(10,2)) AS string_to_decimal,
# MAGIC   CAST('2024-01-15' AS DATE) AS string_to_date,
# MAGIC   TO_DATE('20240115', 'yyyyMMdd') AS formatted_string_to_date,
# MAGIC   YEAR(CURRENT_DATE()) AS current_year,
# MAGIC   DATE_FORMAT(CURRENT_DATE(), 'yyyy/MM/dd') AS formatted_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. テーブルの変更（ALTER TABLE）
# MAGIC
# MAGIC 既存のテーブルにカラムを追加したり、テーブルのプロパティを変更できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- カラムの追加
# MAGIC ALTER TABLE da_customers ADD COLUMNS (
# MAGIC   phone_number STRING COMMENT '電話番号',
# MAGIC   updated_at TIMESTAMP COMMENT '最終更新日時'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 追加されたカラムを確認
# MAGIC DESCRIBE TABLE da_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. テーブルへのコメントとタグの付与
# MAGIC
# MAGIC テーブルやカラムに**コメント**や**タグ**を付けることで、
# MAGIC 他のユーザーがデータを発見・理解しやすくなります。
# MAGIC
# MAGIC > **認定試験のポイント**: タグとコメントを使ったデータ資産の管理は試験範囲です。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルにコメントを設定
# MAGIC COMMENT ON TABLE da_customers IS 'EC サイトの顧客マスタテーブル。個人情報を含むため取り扱い注意。';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- カラムにコメントを設定
# MAGIC ALTER TABLE da_customers ALTER COLUMN email COMMENT 'メールアドレス（一意制約あり）';
# MAGIC ALTER TABLE da_customers ALTER COLUMN segment COMMENT '顧客セグメント（Premium/Standard）';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- コメントが設定されたことを確認
# MAGIC DESCRIBE TABLE EXTENDED da_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### タグの付与
# MAGIC
# MAGIC Unity Catalog のタグを使うと、データ資産をカテゴリ分けして検索しやすくできます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルにタグを設定
# MAGIC ALTER TABLE da_customers SET TAGS ('department' = 'sales', 'data_quality' = 'reviewed');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- カラムにタグを設定（PII: 個人識別情報）
# MAGIC ALTER TABLE da_customers ALTER COLUMN email SET TAGS ('pii' = 'true');
# MAGIC ALTER TABLE da_customers ALTER COLUMN name SET TAGS ('pii' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: PII（Personally Identifiable Information）とは？**
# MAGIC >
# MAGIC > 個人を特定できる情報のことです。名前、メールアドレス、電話番号、住所などが該当します。
# MAGIC > PIIタグを付けておくと、セキュリティポリシーの適用やコンプライアンスの確認が容易になります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. CTAS（CREATE TABLE AS SELECT）
# MAGIC
# MAGIC 既存のテーブルからクエリ結果を使って新しいテーブルを作成する方法です。
# MAGIC データの集計結果やクリーニング済みデータを別テーブルとして保存するのに便利です。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 顧客ごとの注文サマリーテーブルを作成（CTAS）
# MAGIC CREATE OR REPLACE TABLE da_customer_order_summary AS
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.name,
# MAGIC   c.city,
# MAGIC   c.segment,
# MAGIC   COUNT(o.order_id) AS order_count,
# MAGIC   COALESCE(SUM(o.total_amount), 0) AS total_spent,
# MAGIC   COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
# MAGIC   MAX(o.order_date) AS last_order_date
# MAGIC FROM da_customers c
# MAGIC LEFT JOIN da_orders_clean o ON c.customer_id = o.customer_id
# MAGIC GROUP BY c.customer_id, c.name, c.city, c.segment;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 作成されたサマリーテーブルを確認
# MAGIC SELECT * FROM da_customer_order_summary
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC > **試してみよう**: `da_customer_order_summary` テーブルに対して
# MAGIC > `DESCRIBE DETAIL` を実行し、テーブルのメタデータを確認してみましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **テーブルの作成** — CREATE TABLE, INSERT, CREATE OR REPLACE
# MAGIC 2. **NULL値の処理** — IS NULL, COALESCE, IFNULL
# MAGIC 3. **不正データの検出と除去** — CASE式、フィルタリング、ビューの活用
# MAGIC 4. **データ型の変換** — CAST, ::演算子, TO_DATE
# MAGIC 5. **テーブルの変更** — ALTER TABLE ADD COLUMNS
# MAGIC 6. **コメントとタグ** — COMMENT ON, SET TAGS（データ発見性の向上）
# MAGIC 7. **CTAS** — クエリ結果からテーブルを作成
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - タグを付けたテーブルをCatalog Explorerで検索してみましょう
# MAGIC - **次のノートブック `03_importing_data.py`** でデータのインポート方法を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Analyst Associate):
# MAGIC > - **Managing Data (8%)**: テーブル管理、データクリーニング、タグ・リネージ

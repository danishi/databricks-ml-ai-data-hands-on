# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks SQL によるクエリの実行
# MAGIC
# MAGIC このノートブックでは、Databricks SQL を使った**データ分析クエリ**を体系的に学びます。
# MAGIC このセクションは試験全体の **20%** を占める最重要分野です。
# MAGIC
# MAGIC **Databricks SQL とは？**
# MAGIC > ANSI SQL 準拠のクエリ実行環境です。SQL ウェアハウスというコンピューティングリソース上で
# MAGIC > 高速にクエリを実行できます。標準的な SQL の知識がそのまま活かせます。
# MAGIC
# MAGIC ### このノートブックで体験する流れ
# MAGIC
# MAGIC ```
# MAGIC ① 集約関数 → ② JOIN操作 → ③ フィルタリング・ソート → ④ サブクエリ・CTE
# MAGIC                                                              ↓
# MAGIC ⑥ タイムトラベル ← ⑤ マテリアライズドビュー・ストリーミングテーブル
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - 集約関数（COUNT, SUM, AVG, GROUP BY）
# MAGIC - JOIN操作（INNER, LEFT, RIGHT, FULL, CROSS）
# MAGIC - フィルタリングとソート（WHERE, HAVING, ORDER BY, LIMIT）
# MAGIC - サブクエリと CTE（共通テーブル式）
# MAGIC - ウィンドウ関数
# MAGIC - マテリアライズドビューとストリーミングテーブル
# MAGIC - Delta Lake タイムトラベル
# MAGIC
# MAGIC ## 前提条件
# MAGIC - `02_managing_data.py` を実行済みであること（サンプルデータが必要）
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
# MAGIC ## 1. 集約関数（Aggregate Functions）
# MAGIC
# MAGIC データを集計・要約するための関数です。`GROUP BY` と組み合わせて使います。
# MAGIC
# MAGIC | 関数 | 説明 | 例 |
# MAGIC |---|---|---|
# MAGIC | `COUNT(*)` | 行数をカウント | 注文件数 |
# MAGIC | `COUNT(DISTINCT col)` | ユニークな値の数 | ユニーク顧客数 |
# MAGIC | `APPROX_COUNT_DISTINCT(col)` | 近似ユニーク数（高速） | 大規模データのユニーク数 |
# MAGIC | `SUM(col)` | 合計値 | 売上合計 |
# MAGIC | `AVG(col)` | 平均値 | 平均注文金額 |
# MAGIC | `MIN(col)` / `MAX(col)` | 最小値 / 最大値 | 最安値 / 最高値 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 基本的な集約: 注文データの概要
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_orders,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC   SUM(total_amount) AS total_revenue,
# MAGIC   AVG(total_amount) AS avg_order_value,
# MAGIC   MIN(total_amount) AS min_order,
# MAGIC   MAX(total_amount) AS max_order
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GROUP BY: 商品ごとの集計
# MAGIC SELECT
# MAGIC   product_name,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(quantity) AS total_quantity,
# MAGIC   SUM(total_amount) AS total_revenue,
# MAGIC   AVG(total_amount) AS avg_revenue
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY product_name
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- HAVING: 集約結果に対するフィルタリング
# MAGIC -- 注文が2回以上ある商品のみ表示
# MAGIC SELECT
# MAGIC   product_name,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(total_amount) AS total_revenue
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY product_name
# MAGIC HAVING COUNT(*) >= 2
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: WHERE と HAVING の違い**
# MAGIC >
# MAGIC > - `WHERE`: 集約**前**の個別行をフィルタリング
# MAGIC > - `HAVING`: 集約**後**のグループをフィルタリング
# MAGIC >
# MAGIC > 処理の順序: `FROM` → `WHERE` → `GROUP BY` → `HAVING` → `SELECT` → `ORDER BY`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- APPROX_COUNT_DISTINCT: 近似ユニークカウント（大規模データで高速）
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT customer_id) AS exact_unique_customers,
# MAGIC   APPROX_COUNT_DISTINCT(customer_id) AS approx_unique_customers
# MAGIC FROM da_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC > **認定試験のポイント**: `APPROX_COUNT_DISTINCT` は大規模データセットで
# MAGIC > `COUNT(DISTINCT)` よりも大幅に高速です。わずかな誤差（通常2%以内）を許容できる場合に使います。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. JOIN操作 — テーブルの結合
# MAGIC
# MAGIC 複数のテーブルを結合してデータを分析します。
# MAGIC
# MAGIC | JOIN種類 | 説明 | 結果 |
# MAGIC |---|---|---|
# MAGIC | **INNER JOIN** | 両方のテーブルに一致する行のみ | 一致する行だけ |
# MAGIC | **LEFT JOIN** | 左テーブルの全行＋右の一致行 | 左は全行、右はNULLあり |
# MAGIC | **RIGHT JOIN** | 右テーブルの全行＋左の一致行 | 右は全行、左はNULLあり |
# MAGIC | **FULL OUTER JOIN** | 両方の全行 | 一致しない行はNULL |
# MAGIC | **CROSS JOIN** | すべての組み合わせ | 行数 = 左×右 |
# MAGIC
# MAGIC ```
# MAGIC INNER JOIN:     LEFT JOIN:      RIGHT JOIN:     FULL OUTER JOIN:
# MAGIC   [A ∩ B]       [A + A∩B]       [A∩B + B]       [A + A∩B + B]
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INNER JOIN: 注文がある顧客のみ
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.name,
# MAGIC   c.city,
# MAGIC   o.order_id,
# MAGIC   o.product_name,
# MAGIC   o.total_amount
# MAGIC FROM da_customers c
# MAGIC INNER JOIN da_orders o ON c.customer_id = o.customer_id
# MAGIC WHERE o.status = 'completed'
# MAGIC ORDER BY c.customer_id, o.order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- LEFT JOIN: 注文がない顧客も含めて表示
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.name,
# MAGIC   c.city,
# MAGIC   COUNT(o.order_id) AS order_count,
# MAGIC   COALESCE(SUM(o.total_amount), 0) AS total_spent
# MAGIC FROM da_customers c
# MAGIC LEFT JOIN da_orders o ON c.customer_id = o.customer_id
# MAGIC   AND o.status = 'completed'
# MAGIC GROUP BY c.customer_id, c.name, c.city
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC > **試してみよう**: `LEFT JOIN` を `INNER JOIN` に変えて実行してみましょう。
# MAGIC > 注文がない顧客（order_count = 0）が結果から消えることを確認できます。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 集合演算（Set Operations）
# MAGIC
# MAGIC テーブルの行を結合する方法として、集合演算もあります。
# MAGIC
# MAGIC | 演算 | 説明 | 重複行 |
# MAGIC |---|---|---|
# MAGIC | `UNION` | 両方の結果を結合 | 重複を除去 |
# MAGIC | `UNION ALL` | 両方の結果を結合 | 重複を含む |
# MAGIC | `INTERSECT` | 両方に含まれる行 | 重複を除去 |
# MAGIC | `EXCEPT` | 左にあり右にない行 | 重複を除去 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UNION: Electronics と Furniture カテゴリの商品を統合
# MAGIC SELECT product_name, 'Electronics' AS source FROM da_sample_products WHERE category = 'Electronics'
# MAGIC UNION
# MAGIC SELECT product_name, 'Order History' AS source FROM da_orders WHERE product_name IN ('ノートPC', 'ヘッドセット');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. フィルタリングとソート
# MAGIC
# MAGIC データの絞り込みと並び替えは、分析の基本操作です。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 複合条件のフィルタリング
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   product_name,
# MAGIC   order_date,
# MAGIC   total_amount
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC   AND total_amount > 5000
# MAGIC   AND order_date BETWEEN '2024-01-01' AND '2024-03-31'
# MAGIC ORDER BY total_amount DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- IN句とLIKE句
# MAGIC SELECT *
# MAGIC FROM da_customers
# MAGIC WHERE city IN ('東京', '大阪')
# MAGIC   AND name LIKE '%田%';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. サブクエリと CTE（共通テーブル式）
# MAGIC
# MAGIC 複雑な分析を段階的に構築する方法です。
# MAGIC
# MAGIC **CTE（Common Table Expression）** は `WITH` 句を使って
# MAGIC 一時的な結果セットに名前を付ける方法です。可読性が高く推奨されています。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- サブクエリ: 平均注文金額以上の注文を抽出
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   product_name,
# MAGIC   total_amount
# MAGIC FROM da_orders
# MAGIC WHERE total_amount > (
# MAGIC   SELECT AVG(total_amount)
# MAGIC   FROM da_orders
# MAGIC   WHERE status = 'completed'
# MAGIC )
# MAGIC ORDER BY total_amount DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTE: 段階的に分析を構築
# MAGIC WITH customer_stats AS (
# MAGIC   -- Step 1: 顧客ごとの統計を計算
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     SUM(total_amount) AS total_spent,
# MAGIC     AVG(total_amount) AS avg_order_value
# MAGIC   FROM da_orders
# MAGIC   WHERE status = 'completed'
# MAGIC   GROUP BY customer_id
# MAGIC ),
# MAGIC customer_ranking AS (
# MAGIC   -- Step 2: 顧客をランク付け
# MAGIC   SELECT
# MAGIC     cs.*,
# MAGIC     CASE
# MAGIC       WHEN total_spent >= 50000 THEN 'VIP'
# MAGIC       WHEN total_spent >= 10000 THEN 'Regular'
# MAGIC       ELSE 'New'
# MAGIC     END AS customer_tier
# MAGIC   FROM customer_stats cs
# MAGIC )
# MAGIC -- Step 3: 顧客情報と結合して最終結果
# MAGIC SELECT
# MAGIC   c.name,
# MAGIC   c.city,
# MAGIC   cr.order_count,
# MAGIC   cr.total_spent,
# MAGIC   cr.avg_order_value,
# MAGIC   cr.customer_tier
# MAGIC FROM customer_ranking cr
# MAGIC JOIN da_customers c ON cr.customer_id = c.customer_id
# MAGIC ORDER BY cr.total_spent DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: CTE vs サブクエリ**
# MAGIC >
# MAGIC > - **CTE（WITH句）**: 読みやすく、複数回参照可能。複雑な分析に向いている
# MAGIC > - **サブクエリ**: 短い条件フィルタに便利。ネストが深くなると読みにくい
# MAGIC >
# MAGIC > 一般的にCTEの使用が推奨されています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ウィンドウ関数
# MAGIC
# MAGIC **ウィンドウ関数**は、集約しながらも個々の行を保持する強力な機能です。
# MAGIC
# MAGIC | 関数 | 説明 | 例 |
# MAGIC |---|---|---|
# MAGIC | `ROW_NUMBER()` | 行番号を付与 | 顧客ごとの最新注文 |
# MAGIC | `RANK()` | 順位（同順位あり、次は飛ばす） | 売上ランキング |
# MAGIC | `DENSE_RANK()` | 順位（同順位あり、次は飛ばさない） | 連続ランキング |
# MAGIC | `LAG() / LEAD()` | 前後の行の値を参照 | 前月比の計算 |
# MAGIC | `SUM() OVER()` | 累積合計 | 累積売上 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ROW_NUMBER: 顧客ごとの注文を日付順にナンバリング
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   order_id,
# MAGIC   product_name,
# MAGIC   order_date,
# MAGIC   total_amount,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS order_seq
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC ORDER BY customer_id, order_seq;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RANK: 注文金額ランキング
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   product_name,
# MAGIC   total_amount,
# MAGIC   RANK() OVER (ORDER BY total_amount DESC) AS revenue_rank,
# MAGIC   DENSE_RANK() OVER (ORDER BY total_amount DESC) AS dense_revenue_rank
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC ORDER BY revenue_rank;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 累積合計（Running Total）
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   order_date,
# MAGIC   total_amount,
# MAGIC   SUM(total_amount) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING) AS cumulative_revenue
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC > **試してみよう**: `PARTITION BY` を追加して、顧客ごとの累積合計を計算してみましょう。
# MAGIC > ```sql
# MAGIC > SUM(total_amount) OVER (PARTITION BY customer_id ORDER BY order_date)
# MAGIC > ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. テーブルの作成パターン
# MAGIC
# MAGIC 分析結果をテーブルとして保存するパターンです。
# MAGIC
# MAGIC | パターン | 構文 | 説明 |
# MAGIC |---|---|---|
# MAGIC | **CTAS** | `CREATE TABLE AS SELECT` | クエリ結果からテーブル作成 |
# MAGIC | **INSERT INTO** | `INSERT INTO table SELECT ...` | 既存テーブルにデータ追加 |
# MAGIC | **INSERT OVERWRITE** | `INSERT OVERWRITE table SELECT ...` | テーブルの全データを置換 |
# MAGIC | **MERGE INTO** | `MERGE INTO target USING source` | 条件に応じた挿入・更新・削除 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTAS: 月次売上サマリーテーブルの作成
# MAGIC CREATE OR REPLACE TABLE da_monthly_sales AS
# MAGIC SELECT
# MAGIC   DATE_TRUNC('month', order_date) AS order_month,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(total_amount) AS total_revenue,
# MAGIC   AVG(total_amount) AS avg_order_value,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY DATE_TRUNC('month', order_date)
# MAGIC ORDER BY order_month;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM da_monthly_sales;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. マテリアライズドビューとストリーミングテーブル
# MAGIC
# MAGIC ### マテリアライズドビュー
# MAGIC
# MAGIC 通常のビューは毎回クエリを実行しますが、**マテリアライズドビュー**は
# MAGIC クエリ結果を物理的に保存して高速なアクセスを可能にします。
# MAGIC
# MAGIC | 比較 | 標準ビュー | マテリアライズドビュー |
# MAGIC |---|---|---|
# MAGIC | **データ保持** | なし（毎回クエリ実行） | あり（結果を保存） |
# MAGIC | **パフォーマンス** | 元テーブルサイズに依存 | 事前計算で高速 |
# MAGIC | **データ鮮度** | 常に最新 | リフレッシュ時点のデータ |
# MAGIC | **ストレージ** | 不要 | 結果分の領域が必要 |
# MAGIC | **更新方法** | 自動（クエリ実行時） | `REFRESH` コマンド |
# MAGIC
# MAGIC ```sql
# MAGIC -- マテリアライズドビューの作成（参考）
# MAGIC CREATE MATERIALIZED VIEW mv_monthly_sales AS
# MAGIC SELECT ...
# MAGIC
# MAGIC -- リフレッシュ（データ更新）
# MAGIC REFRESH MATERIALIZED VIEW mv_monthly_sales;
# MAGIC ```
# MAGIC
# MAGIC > **注意**: マテリアライズドビューは SQL ウェアハウスまたは Delta Live Tables パイプラインで
# MAGIC > 作成する必要があります。ノートブッククラスターでは作成できない場合があります。
# MAGIC
# MAGIC ### ストリーミングテーブル
# MAGIC
# MAGIC **ストリーミングテーブル**は、データソースからの増分データを自動的に取り込むテーブルです。
# MAGIC
# MAGIC | 比較 | 通常のテーブル | ストリーミングテーブル |
# MAGIC |---|---|---|
# MAGIC | **データ取り込み** | 手動（INSERT, COPY INTO） | 自動（増分処理） |
# MAGIC | **更新タイミング** | 手動実行時 | スケジュールまたは継続的 |
# MAGIC | **用途** | バッチ分析 | リアルタイムに近い分析 |
# MAGIC
# MAGIC > **認定試験のポイント**: マテリアライズドビュー、標準ビュー、ストリーミングテーブルの
# MAGIC > 違いと使い分けは重要な出題範囲です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Delta Lake タイムトラベル
# MAGIC
# MAGIC Delta Lake の**タイムトラベル**機能を使うと、テーブルの過去のバージョンを参照できます。
# MAGIC
# MAGIC | 構文 | 説明 |
# MAGIC |---|---|
# MAGIC | `SELECT * FROM table VERSION AS OF 0` | 特定バージョンのデータ |
# MAGIC | `SELECT * FROM table TIMESTAMP AS OF '2024-01-01'` | 特定時点のデータ |
# MAGIC | `DESCRIBE HISTORY table` | テーブルの変更履歴 |
# MAGIC | `RESTORE TABLE table TO VERSION AS OF 0` | 特定バージョンに復元 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの変更履歴を確認
# MAGIC DESCRIBE HISTORY da_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 初期バージョン（バージョン0）のデータを参照
# MAGIC SELECT * FROM da_orders VERSION AS OF 0
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データを更新してバージョンを進める
# MAGIC UPDATE da_orders
# MAGIC SET status = 'shipped'
# MAGIC WHERE order_id = 1008;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 変更後の履歴を確認（新しいバージョンが作成された）
# MAGIC DESCRIBE HISTORY da_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 変更前のデータと比較
# MAGIC SELECT 'Before' AS version, status FROM da_orders VERSION AS OF 1 WHERE order_id = 1008
# MAGIC UNION ALL
# MAGIC SELECT 'After' AS version, status FROM da_orders WHERE order_id = 1008;

# COMMAND ----------

# MAGIC %md
# MAGIC > **認定試験のポイント**: タイムトラベルを使った監査（過去データの確認）と
# MAGIC > データの復旧（RESTORE）は出題範囲です。`VERSION AS OF` と `TIMESTAMP AS OF` の
# MAGIC > 両方の構文を理解しておきましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. 日付・文字列関数
# MAGIC
# MAGIC データ分析で頻繁に使う日付関数と文字列関数です。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 日付関数の例
# MAGIC SELECT
# MAGIC   CURRENT_DATE() AS today,
# MAGIC   CURRENT_TIMESTAMP() AS now,
# MAGIC   DATE_ADD(CURRENT_DATE(), 7) AS one_week_later,
# MAGIC   DATE_SUB(CURRENT_DATE(), 30) AS thirty_days_ago,
# MAGIC   DATEDIFF(CURRENT_DATE(), DATE '2024-01-01') AS days_since_2024,
# MAGIC   YEAR(CURRENT_DATE()) AS current_year,
# MAGIC   MONTH(CURRENT_DATE()) AS current_month,
# MAGIC   DAYOFWEEK(CURRENT_DATE()) AS day_of_week;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 文字列関数の例
# MAGIC SELECT
# MAGIC   UPPER('hello world') AS upper_case,
# MAGIC   LOWER('HELLO WORLD') AS lower_case,
# MAGIC   TRIM('  hello  ') AS trimmed,
# MAGIC   CONCAT('Hello', ' ', 'World') AS concatenated,
# MAGIC   SUBSTRING('Databricks', 1, 4) AS sub_string,
# MAGIC   LENGTH('Databricks') AS string_length,
# MAGIC   REPLACE('Hello World', 'World', 'Databricks') AS replaced;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **集約関数** — COUNT, SUM, AVG, MIN, MAX, APPROX_COUNT_DISTINCT
# MAGIC 2. **GROUP BY / HAVING** — グループ化と集約結果のフィルタリング
# MAGIC 3. **JOIN操作** — INNER, LEFT, RIGHT, FULL OUTER, CROSS JOIN
# MAGIC 4. **集合演算** — UNION, UNION ALL, INTERSECT, EXCEPT
# MAGIC 5. **フィルタリング** — WHERE, IN, LIKE, BETWEEN
# MAGIC 6. **サブクエリと CTE** — 複雑な分析の段階的構築
# MAGIC 7. **ウィンドウ関数** — ROW_NUMBER, RANK, DENSE_RANK, 累積合計
# MAGIC 8. **テーブル作成パターン** — CTAS, INSERT, MERGE
# MAGIC 9. **マテリアライズドビュー** — 事前計算による高速化
# MAGIC 10. **タイムトラベル** — 過去データの参照と復元
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - CTE やウィンドウ関数を組み合わせた複雑な分析に挑戦してみましょう
# MAGIC - **次のノートブック `05_query_analysis.py`** でクエリの分析と最適化を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Analyst Associate):
# MAGIC > - **Executing Queries using Databricks SQL and SQL Warehouses (20%)**: 集約、JOIN、マテリアライズドビュー、タイムトラベル

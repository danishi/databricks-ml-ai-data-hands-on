# Databricks notebook source
# MAGIC %md
# MAGIC # クエリの分析と最適化
# MAGIC
# MAGIC このノートブックでは、クエリの**パフォーマンス分析と最適化**を学びます。
# MAGIC 効率的なクエリを書くことは、データアナリストの重要なスキルです。
# MAGIC
# MAGIC **クエリ最適化とは？**
# MAGIC > 同じ結果を得るクエリでも、書き方によって実行速度が大きく異なります。
# MAGIC > 「目的地に着く」のは同じでも、「最短ルートを選ぶ」ことで時間と燃料を節約できるのと同じです。
# MAGIC
# MAGIC ### このノートブックで体験する流れ
# MAGIC
# MAGIC ```
# MAGIC ① Photon エンジン → ② クエリプロファイル分析 → ③ キャッシュの活用
# MAGIC                                                       ↓
# MAGIC ⑤ クエリのデバッグ ← ④ Liquid Clustering
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - Photon エンジンの特徴と利点
# MAGIC - クエリプロファイルとクエリ履歴の活用
# MAGIC - Delta Lake の監査と履歴
# MAGIC - キャッシュの仕組みと活用
# MAGIC - Liquid Clustering による最適化
# MAGIC - クエリのデバッグ方法
# MAGIC
# MAGIC ## 前提条件
# MAGIC - `04_sql_queries.py` を実行済みであること
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
# MAGIC ## 1. Photon エンジン
# MAGIC
# MAGIC **Photon** は、Databricks の次世代クエリエンジンです。C++で実装されており、
# MAGIC SQL クエリを大幅に高速化します。
# MAGIC
# MAGIC | 特徴 | 説明 |
# MAGIC |---|---|
# MAGIC | **ベクトル化実行** | データをバッチ単位で効率的に処理 |
# MAGIC | **コード生成** | クエリごとに最適化されたコードを生成 |
# MAGIC | **メモリ管理** | 効率的なメモリ使用で大規模データに対応 |
# MAGIC | **自動適用** | SQL ウェアハウスではデフォルトで有効 |
# MAGIC
# MAGIC ### Photon が効果を発揮するワークロード
# MAGIC
# MAGIC | ワークロード | Photon の効果 |
# MAGIC |---|---|
# MAGIC | **集約クエリ** (GROUP BY, SUM, COUNT) | 高い効果 |
# MAGIC | **JOIN操作** | 高い効果 |
# MAGIC | **フィルタリング** | 高い効果 |
# MAGIC | **データスキャン** | 高い効果 |
# MAGIC | **UDF（ユーザー定義関数）** | 効果なし（Sparkで実行） |
# MAGIC
# MAGIC > **認定試験のポイント**: Photon は SQL ウェアハウスではデフォルトで有効です。
# MAGIC > Photon が効果を発揮するワークロードの種類を理解しておきましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. クエリプロファイルの分析
# MAGIC
# MAGIC **Query Profile（クエリプロファイル）** は、クエリの実行計画と
# MAGIC パフォーマンスの詳細を可視化するツールです。
# MAGIC
# MAGIC ### クエリプロファイルの確認方法
# MAGIC
# MAGIC 1. SQL エディタでクエリを実行
# MAGIC 2. 結果の下にある **「クエリプロファイル」** タブをクリック
# MAGIC 3. 各ステージの実行時間、処理行数、データ量を確認
# MAGIC
# MAGIC ### クエリプロファイルで確認すべきポイント
# MAGIC
# MAGIC | 項目 | 説明 | 注意すべき状況 |
# MAGIC |---|---|---|
# MAGIC | **Scan（スキャン）** | 読み取ったデータ量 | 必要以上のデータを読んでいないか |
# MAGIC | **Filter（フィルタ）** | フィルタリングされた行数 | フィルタの効率は良いか |
# MAGIC | **Shuffle（シャッフル）** | ネットワーク転送量 | 過剰なシャッフルが発生していないか |
# MAGIC | **Spill（スピル）** | ディスク書き出し | メモリ不足で遅くなっていないか |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- EXPLAIN: クエリの実行計画を確認
# MAGIC EXPLAIN
# MAGIC SELECT
# MAGIC   c.name,
# MAGIC   COUNT(o.order_id) AS order_count,
# MAGIC   SUM(o.total_amount) AS total_spent
# MAGIC FROM da_customers c
# MAGIC JOIN da_orders o ON c.customer_id = o.customer_id
# MAGIC WHERE o.status = 'completed'
# MAGIC GROUP BY c.name
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: 実行計画（Execution Plan）とは？**
# MAGIC >
# MAGIC > データベースエンジンがクエリをどのような手順で実行するかの「計画書」です。
# MAGIC > 料理でいえば「レシピ（手順書）」に相当します。
# MAGIC > EXPLAIN で実行計画を見ることで、非効率な処理を発見できます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query Insights — パフォーマンスの問題を特定
# MAGIC
# MAGIC **Query Insights** は、パフォーマンスに問題があるクエリを自動的に検出し、
# MAGIC 改善方法を提案する機能です。
# MAGIC
# MAGIC ### Query Insights の確認方法
# MAGIC
# MAGIC 1. 左サイドバーの **「クエリ履歴」** をクリック
# MAGIC 2. 問題のあるクエリには警告アイコンが表示
# MAGIC 3. クエリを選択して **「Query Insights」** タブで詳細を確認
# MAGIC
# MAGIC ### 検出される問題の例
# MAGIC
# MAGIC | 問題 | 説明 | 改善方法 |
# MAGIC |---|---|---|
# MAGIC | **フルスキャン** | テーブル全体をスキャン | フィルタの追加、パーティション活用 |
# MAGIC | **データスキュー** | 特定のキーにデータが偏る | キーの見直し、ブロードキャスト |
# MAGIC | **メモリスピル** | メモリ不足でディスクに退避 | ウェアハウスサイズの拡大 |
# MAGIC | **非効率な JOIN** | 大きなテーブル同士のJOIN | フィルタの事前適用 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. クエリ履歴（Query History）
# MAGIC
# MAGIC **クエリ履歴**では、過去に実行されたクエリの一覧とその実行時間を確認できます。
# MAGIC
# MAGIC ### クエリ履歴の確認方法
# MAGIC
# MAGIC 1. 左サイドバーの **「クエリ履歴」** をクリック
# MAGIC 2. フィルタで期間、ユーザー、ウェアハウスを絞り込み
# MAGIC 3. 各クエリの実行時間、ステータス、結果行数を確認
# MAGIC
# MAGIC > **認定試験のポイント**: クエリ履歴の画面でどのような情報が確認でき、
# MAGIC > どのようにパフォーマンスの問題を特定するかが出題されます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delta Lake の監査と履歴
# MAGIC
# MAGIC Delta Lake のテーブルは、すべての変更操作が**トランザクションログ**に記録されます。
# MAGIC これにより、データの監査とトラブルシューティングが容易になります。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの変更履歴を詳細に確認
# MAGIC DESCRIBE HISTORY da_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 履歴で確認できる情報
# MAGIC
# MAGIC | カラム | 説明 |
# MAGIC |---|---|
# MAGIC | `version` | バージョン番号 |
# MAGIC | `timestamp` | 変更日時 |
# MAGIC | `operation` | 操作の種類（WRITE, UPDATE, DELETE, MERGE等） |
# MAGIC | `operationParameters` | 操作のパラメータ |
# MAGIC | `userIdentity` | 操作を実行したユーザー |
# MAGIC | `operationMetrics` | 変更された行数などのメトリクス |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 変更前後のデータを比較して監査
# MAGIC SELECT
# MAGIC   'Version 0' AS data_version,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   SUM(total_amount) AS total_revenue
# MAGIC FROM da_orders VERSION AS OF 0
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Current' AS data_version,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   SUM(total_amount) AS total_revenue
# MAGIC FROM da_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. キャッシュの仕組みと活用
# MAGIC
# MAGIC Databricks では複数のレベルでキャッシュが機能し、クエリの高速化に寄与します。
# MAGIC
# MAGIC | キャッシュ種類 | 説明 | 有効範囲 |
# MAGIC |---|---|---|
# MAGIC | **結果キャッシュ（Result Cache）** | クエリ結果をそのままキャッシュ | 同じクエリを再実行した場合 |
# MAGIC | **ディスクキャッシュ（Disk Cache）** | リモートストレージのデータをローカルに保存 | 同じデータを参照するクエリ |
# MAGIC | **Delta Cache** | Delta テーブルのデータをメモリ/SSDに保存 | Delta テーブルへのアクセス |
# MAGIC
# MAGIC ### 結果キャッシュの仕組み
# MAGIC
# MAGIC ```
# MAGIC 1回目のクエリ実行: [SQL] → [データスキャン] → [計算] → [結果]（キャッシュに保存）
# MAGIC 2回目の同じクエリ: [SQL] → [キャッシュから結果を返す]（超高速！）
# MAGIC ```
# MAGIC
# MAGIC > **注意**: テーブルのデータが変更されると、キャッシュは自動的に無効化されます。
# MAGIC > 常に最新のデータが返されることが保証されています。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 同じクエリを2回実行してキャッシュの効果を体験
# MAGIC -- 1回目: データスキャンが発生
# MAGIC SELECT
# MAGIC   product_name,
# MAGIC   SUM(total_amount) AS total_revenue,
# MAGIC   COUNT(*) AS order_count
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY product_name
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2回目: キャッシュから結果が返される（高速）
# MAGIC SELECT
# MAGIC   product_name,
# MAGIC   SUM(total_amount) AS total_revenue,
# MAGIC   COUNT(*) AS order_count
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY product_name
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC > **試してみよう**: クエリの実行時間を比較してみましょう。
# MAGIC > 2回目の方が高速になっていれば、キャッシュが効いています。
# MAGIC > （小さなデータセットでは差が小さい場合があります）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Liquid Clustering — 大規模テーブルの最適化
# MAGIC
# MAGIC **Liquid Clustering** は、テーブルのデータを特定のカラムでグループ化して
# MAGIC 保存する最適化手法です。従来の Z-ORDER に代わる新しい方法です。
# MAGIC
# MAGIC ### Liquid Clustering の仕組み
# MAGIC
# MAGIC ```
# MAGIC [最適化前]                    [Liquid Clustering 適用後]
# MAGIC ファイル1: A,C,B,A,C          ファイル1: A,A,A（Aのデータ）
# MAGIC ファイル2: B,A,C,B,A    →    ファイル2: B,B,B（Bのデータ）
# MAGIC ファイル3: C,A,B,C,A          ファイル3: C,C,C（Cのデータ）
# MAGIC ```
# MAGIC
# MAGIC WHERE 句で特定の値をフィルタリングする際、関連するファイルだけを読めばよいため高速です。
# MAGIC
# MAGIC | 比較項目 | Z-ORDER（従来） | Liquid Clustering（新） |
# MAGIC |---|---|---|
# MAGIC | **設定方法** | OPTIMIZE 実行時に指定 | テーブル作成時に定義 |
# MAGIC | **自動最適化** | 手動実行が必要 | データ書き込み時に自動適用 |
# MAGIC | **カラム変更** | 再実行が必要 | ALTER TABLE で変更可能 |
# MAGIC | **推奨** | レガシー | 新規テーブルに推奨 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Liquid Clustering を適用したテーブルの作成
# MAGIC CREATE OR REPLACE TABLE da_orders_clustered
# MAGIC CLUSTER BY (order_date, status)
# MAGIC AS SELECT * FROM da_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- クラスタリング情報の確認
# MAGIC DESCRIBE DETAIL da_orders_clustered;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- クラスタリングされたカラムでフィルタリング（効率的）
# MAGIC SELECT *
# MAGIC FROM da_orders_clustered
# MAGIC WHERE order_date >= '2024-03-01'
# MAGIC   AND status = 'completed';

# COMMAND ----------

# MAGIC %md
# MAGIC > **認定試験のポイント**: Liquid Clustering は、フィルタリング頻度が高いカラムに
# MAGIC > 設定するのが効果的です。Z-ORDER との違いも理解しておきましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. クエリのデバッグ
# MAGIC
# MAGIC クエリが期待通りの結果を返さない場合のデバッグ手法です。
# MAGIC
# MAGIC ### よくあるクエリの問題と対処法
# MAGIC
# MAGIC | 問題 | 原因 | 対処法 |
# MAGIC |---|---|---|
# MAGIC | **結果が0行** | WHERE条件が厳しすぎる | 条件を一つずつ外して確認 |
# MAGIC | **結果が多すぎる** | JOINでデータが膨張 | JOIN条件の確認、DISTINCTの追加 |
# MAGIC | **NULL値の予期しない挙動** | NULL比較の注意 | IS NULL / IS NOT NULLを使用 |
# MAGIC | **集約結果がおかしい** | GROUP BYの不足 | GROUP BYにカラムを追加 |
# MAGIC | **型の不一致** | 暗黙の型変換 | 明示的なCAST |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- デバッグ例: NULL の比較は = ではなく IS NULL を使う
# MAGIC -- NG: WHERE email = NULL（常にFALSE になる）
# MAGIC -- OK: WHERE email IS NULL
# MAGIC
# MAGIC SELECT 'NG pattern' AS pattern, COUNT(*) AS result_count
# MAGIC FROM da_customers WHERE email = NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'OK pattern' AS pattern, COUNT(*) AS result_count
# MAGIC FROM da_customers WHERE email IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: NULL の特殊な振る舞い**
# MAGIC >
# MAGIC > SQL では NULL は「値が不明」を意味します。「空文字」や「0」とは異なります。
# MAGIC > - `NULL = NULL` → FALSE（NULLを使った比較は常にNULLを返す）
# MAGIC > - `NULL <> 'hello'` → FALSE（同上）
# MAGIC > - `NULL IS NULL` → TRUE（IS NULL で正しく判定できる）
# MAGIC > - `COALESCE(NULL, 'default')` → `'default'`

# COMMAND ----------

# MAGIC %md
# MAGIC ### デバッグのステップ
# MAGIC
# MAGIC 1. **問題の特定**: 期待する結果と実際の結果の差異を明確にする
# MAGIC 2. **段階的な確認**: CTEやサブクエリの各段階で中間結果を確認
# MAGIC 3. **データの確認**: 元テーブルのデータ品質を確認（NULL、重複等）
# MAGIC 4. **実行計画の確認**: EXPLAIN で非効率な処理がないか確認
# MAGIC 5. **Databricks Assistant の活用**: AIにエラーメッセージを分析させる

# COMMAND ----------

# MAGIC %sql
# MAGIC -- デバッグ例: JOIN による行の膨張を検出
# MAGIC -- 正しい結合キーかどうかを確認
# MAGIC SELECT
# MAGIC   'da_customers' AS table_name,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_keys
# MAGIC FROM da_customers
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'da_orders' AS table_name,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_keys
# MAGIC FROM da_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. クエリ最適化のベストプラクティス
# MAGIC
# MAGIC | カテゴリ | ベストプラクティス |
# MAGIC |---|---|
# MAGIC | **SELECT** | `SELECT *` を避け、必要なカラムのみ選択 |
# MAGIC | **フィルタ** | WHERE条件をできるだけ早い段階で適用 |
# MAGIC | **JOIN** | 小さいテーブルを先に（右側に）配置 |
# MAGIC | **集約** | 可能な限り事前にフィルタリングしてから集約 |
# MAGIC | **DISTINCT** | 不要な DISTINCT は避ける |
# MAGIC | **ORDER BY** | 最終結果にのみ適用（中間段階では不要） |
# MAGIC | **LIMIT** | 探索的分析では LIMIT を活用 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 最適化のデモ: 非効率なクエリ → 効率的なクエリ
# MAGIC
# MAGIC -- 非効率: 全カラム取得 → JOIN → フィルタ
# MAGIC -- SELECT * FROM da_orders o JOIN da_customers c ON o.customer_id = c.customer_id WHERE o.status = 'completed'
# MAGIC
# MAGIC -- 効率的: 必要カラムのみ取得 → フィルタ → JOIN
# MAGIC SELECT
# MAGIC   o.order_id,
# MAGIC   o.product_name,
# MAGIC   o.total_amount,
# MAGIC   c.name,
# MAGIC   c.city
# MAGIC FROM da_orders o
# MAGIC JOIN da_customers c ON o.customer_id = c.customer_id
# MAGIC WHERE o.status = 'completed'
# MAGIC   AND o.total_amount > 5000
# MAGIC ORDER BY o.total_amount DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **Photon エンジン** — C++ベースの高速クエリエンジン、対応ワークロード
# MAGIC 2. **クエリプロファイル** — 実行計画の分析、ボトルネックの特定
# MAGIC 3. **Query Insights** — パフォーマンス問題の自動検出
# MAGIC 4. **クエリ履歴** — 過去のクエリの確認と分析
# MAGIC 5. **Delta Lake の監査** — トランザクションログ、変更履歴
# MAGIC 6. **キャッシュ** — 結果キャッシュ、ディスクキャッシュ、Delta Cache
# MAGIC 7. **Liquid Clustering** — テーブルの物理的な最適化
# MAGIC 8. **クエリのデバッグ** — NULL処理、JOIN膨張、段階的確認
# MAGIC 9. **最適化ベストプラクティス** — SELECT, WHERE, JOIN の効率的な書き方
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - SQL エディタでクエリプロファイルを実際に確認してみましょう
# MAGIC - **次のノートブック `06_dashboards_visualizations.py`** でダッシュボードの作成を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Analyst Associate):
# MAGIC > - **Analyzing Queries (15%)**: Photon、クエリプロファイル、キャッシュ、Liquid Clustering、デバッグ

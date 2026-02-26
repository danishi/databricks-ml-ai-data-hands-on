# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake の基礎
# MAGIC
# MAGIC このノートブックでは、Databricks のストレージレイヤーである **Delta Lake** の高度な機能を学びます。
# MAGIC
# MAGIC **Delta Lake とは？**
# MAGIC > Apache Parquet ファイルの上に **ACID トランザクション**、**スキーマ管理**、**タイムトラベル** などの
# MAGIC > 機能を追加したオープンソースのストレージレイヤーです。
# MAGIC > Databricks で作成されるテーブルはデフォルトで Delta 形式です。
# MAGIC
# MAGIC ### Delta Lake の主な特徴
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────┐
# MAGIC │                Delta Lake                     │
# MAGIC │                                              │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
# MAGIC │  │  ACID     │  │タイムトラベル│  │スキーマ管理│  │
# MAGIC │  │トランザクション│  │(履歴参照) │  │(進化/強制)│  │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘  │
# MAGIC │                                              │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
# MAGIC │  │ OPTIMIZE │  │  VACUUM  │  │ Liquid   │  │
# MAGIC │  │(ファイル最適化)│  │(不要ファイル削除)│  │Clustering│  │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘  │
# MAGIC │                                              │
# MAGIC │  ┌──────────────────────────────────────┐   │
# MAGIC │  │        Parquet ファイル + トランザクションログ │   │
# MAGIC │  └──────────────────────────────────────┘   │
# MAGIC └─────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - Delta Lake の ACID トランザクション
# MAGIC - タイムトラベル（履歴の参照と復元）
# MAGIC - OPTIMIZE（ファイルの最適化）
# MAGIC - VACUUM（不要ファイルの削除）
# MAGIC - Liquid Clustering（自動データ配置最適化）
# MAGIC - Z-Ordering
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime（例: 16.x）のクラスター
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Delta テーブルの作成と ACID トランザクション
# MAGIC
# MAGIC **ACID トランザクション**とは、データの一貫性を保証する仕組みです。
# MAGIC
# MAGIC | 特性 | 説明 | 具体例 |
# MAGIC |---|---|---|
# MAGIC | **A**tomicity（原子性） | 操作は全て成功 or 全て失敗 | INSERT が途中で失敗しても不整合にならない |
# MAGIC | **C**onsistency（一貫性） | データは常に正しい状態 | スキーマ違反のデータは弾かれる |
# MAGIC | **I**solation（分離性） | 同時アクセスでも干渉しない | 読み取りと書き込みが同時に安全 |
# MAGIC | **D**urability（耐久性） | コミットされたデータは永続 | 障害が起きてもデータは失われない |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta テーブルの作成
# MAGIC CREATE OR REPLACE TABLE default.sales_delta (
# MAGIC   sale_id INT,
# MAGIC   product STRING,
# MAGIC   amount DECIMAL(10, 2),
# MAGIC   region STRING,
# MAGIC   sale_date DATE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データの挿入
# MAGIC INSERT INTO default.sales_delta VALUES
# MAGIC   (1, 'ノートPC', 150000.00, '東京', '2024-01-15'),
# MAGIC   (2, 'マウス', 3500.00, '大阪', '2024-01-16'),
# MAGIC   (3, 'キーボード', 12000.00, '東京', '2024-01-17'),
# MAGIC   (4, 'モニター', 45000.00, '名古屋', '2024-01-18'),
# MAGIC   (5, 'USBメモリ', 2500.00, '福岡', '2024-01-19')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 追加データの挿入（バージョン2の作成）
# MAGIC INSERT INTO default.sales_delta VALUES
# MAGIC   (6, 'ヘッドセット', 8000.00, '東京', '2024-02-01'),
# MAGIC   (7, 'ウェブカメラ', 5000.00, '大阪', '2024-02-02')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データの更新（バージョン3の作成）
# MAGIC UPDATE default.sales_delta
# MAGIC SET amount = amount * 1.1
# MAGIC WHERE region = '東京'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.sales_delta ORDER BY sale_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. タイムトラベル（Time Travel）
# MAGIC
# MAGIC Delta Lake は全ての変更履歴をトランザクションログに記録しています。
# MAGIC これにより、**過去の任意の時点のデータを参照・復元**できます。
# MAGIC
# MAGIC ### タイムトラベルの方法
# MAGIC
# MAGIC | 方法 | 構文 |
# MAGIC |---|---|
# MAGIC | バージョン番号で指定 | `SELECT * FROM table VERSION AS OF 2` |
# MAGIC | タイムスタンプで指定 | `SELECT * FROM table TIMESTAMP AS OF '2024-01-01'` |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- トランザクションログの履歴を確認
# MAGIC DESCRIBE HISTORY default.sales_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- バージョン1のデータを参照（最初の INSERT 時点のデータ）
# MAGIC SELECT * FROM default.sales_delta VERSION AS OF 1 ORDER BY sale_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 現在のデータと比較（東京の amount が更新されている）
# MAGIC SELECT * FROM default.sales_delta ORDER BY sale_id

# COMMAND ----------

# Python からのタイムトラベル
df_v1 = spark.read.format("delta").option("versionAsOf", 1).table("default.sales_delta")
df_current = spark.table("default.sales_delta")

print(f"バージョン1の件数: {df_v1.count()}")
print(f"現在の件数: {df_current.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### RESTORE によるデータの復元
# MAGIC
# MAGIC `RESTORE` コマンドを使うと、テーブルを過去のバージョンに戻すことができます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- バージョン1に復元
# MAGIC RESTORE TABLE default.sales_delta TO VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 復元されたことを確認（5件に戻っている）
# MAGIC SELECT * FROM default.sales_delta ORDER BY sale_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 再度データを追加して次のデモに備える
# MAGIC INSERT INTO default.sales_delta VALUES
# MAGIC   (6, 'ヘッドセット', 8000.00, '東京', '2024-02-01'),
# MAGIC   (7, 'ウェブカメラ', 5000.00, '大阪', '2024-02-02'),
# MAGIC   (8, 'SSD', 12000.00, '名古屋', '2024-02-03'),
# MAGIC   (9, 'メモリ', 8000.00, '福岡', '2024-02-04'),
# MAGIC   (10, 'ケーブル', 1500.00, '東京', '2024-02-05')

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: タイムトラベルは `VERSION AS OF` または `TIMESTAMP AS OF` で利用できます。
# MAGIC > `DESCRIBE HISTORY` でテーブルの変更履歴を確認できます。
# MAGIC > `RESTORE TABLE` で過去のバージョンにロールバックできます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. OPTIMIZE（ファイルの最適化）
# MAGIC
# MAGIC Delta Lake に頻繁にデータを追加すると、多数の小さなファイル（**スモールファイル問題**）が発生します。
# MAGIC `OPTIMIZE` コマンドは小さなファイルを結合して、クエリ性能を改善します。
# MAGIC
# MAGIC ### スモールファイル問題
# MAGIC
# MAGIC ```
# MAGIC 最適化前（多数の小さなファイル）     最適化後（少数の大きなファイル）
# MAGIC ┌──┐┌──┐┌──┐┌──┐┌──┐┌──┐      ┌──────────────┐
# MAGIC │1KB││2KB││1KB││3KB││1KB││2KB│ →   │     10KB     │
# MAGIC └──┘└──┘└──┘└──┘└──┘└──┘      └──────────────┘
# MAGIC                                     (ファイル数削減 → I/O 削減)
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE の実行
# MAGIC OPTIMIZE default.sales_delta

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: OPTIMIZE はいつ実行すべき？**
# MAGIC >
# MAGIC > - ストリーミングデータの取り込み後
# MAGIC > - 多数の小さな INSERT の後
# MAGIC > - 日次/週次のメンテナンスジョブとして
# MAGIC > - クエリ性能が低下したと感じた時

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Z-Ordering
# MAGIC
# MAGIC **Z-Ordering** は、OPTIMIZE と組み合わせて使うデータ配置の最適化技術です。
# MAGIC 指定した列の値でデータを物理的に並べ替え、その列を使ったフィルタのクエリ性能を向上させます。
# MAGIC
# MAGIC ```
# MAGIC Z-Order なし（ランダム配置）        Z-Order あり（region でソート）
# MAGIC ┌─────────┐                       ┌─────────┐
# MAGIC │東京,大阪,│                       │東京,東京,│
# MAGIC │名古屋,東京│                       │東京,東京 │  → WHERE region = '東京' が高速
# MAGIC ├─────────┤                       ├─────────┤
# MAGIC │福岡,東京,│                       │大阪,大阪,│
# MAGIC │大阪,名古屋│                       │名古屋,福岡│
# MAGIC └─────────┘                       └─────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Z-Ordering の実行（region 列で最適化）
# MAGIC OPTIMIZE default.sales_delta ZORDER BY (region)

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: Z-Ordering は `OPTIMIZE ... ZORDER BY (列名)` で実行します。
# MAGIC > クエリで頻繁にフィルタやJOINに使われる列を指定すると効果的です。
# MAGIC > ただし、Liquid Clustering が利用可能な場合はそちらが推奨されます（次のセクション参照）。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Liquid Clustering
# MAGIC
# MAGIC **Liquid Clustering** は Z-Ordering の後継となる、より高度なデータ配置最適化機能です。
# MAGIC テーブル作成時にクラスタリングキーを指定するだけで、自動的にデータを最適配置します。
# MAGIC
# MAGIC ### Z-Ordering vs Liquid Clustering
# MAGIC
# MAGIC | 特徴 | Z-Ordering | Liquid Clustering |
# MAGIC |---|---|---|
# MAGIC | 設定タイミング | OPTIMIZE 実行時 | テーブル作成時（ALTER でも変更可能） |
# MAGIC | 自動最適化 | なし（手動実行） | `OPTIMIZE` で増分最適化 |
# MAGIC | キーの変更 | 毎回指定が必要 | `ALTER TABLE` でキー変更可能 |
# MAGIC | パーティション | 別途必要 | パーティションの代替として機能 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Liquid Clustering を使ったテーブル作成
# MAGIC CREATE OR REPLACE TABLE default.sales_clustered (
# MAGIC   sale_id INT,
# MAGIC   product STRING,
# MAGIC   amount DECIMAL(10, 2),
# MAGIC   region STRING,
# MAGIC   sale_date DATE
# MAGIC )
# MAGIC CLUSTER BY (region, sale_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データを挿入
# MAGIC INSERT INTO default.sales_clustered
# MAGIC SELECT * FROM default.sales_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE を実行すると Liquid Clustering が適用される
# MAGIC OPTIMIZE default.sales_clustered

# COMMAND ----------

# MAGIC %sql
# MAGIC -- クラスタリングキーの変更（ALTER TABLE で可能）
# MAGIC ALTER TABLE default.sales_clustered CLUSTER BY (region)

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: Liquid Clustering は `CLUSTER BY` で指定し、
# MAGIC > パーティショニングや Z-Ordering の代替として推奨されています。
# MAGIC > `ALTER TABLE ... CLUSTER BY` でキーを後から変更できる柔軟性があります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. VACUUM（不要ファイルの削除）
# MAGIC
# MAGIC Delta Lake では UPDATE や DELETE を実行しても、古いデータファイルはすぐには削除されません
# MAGIC （タイムトラベルのために残されます）。`VACUUM` コマンドで不要な古いファイルを削除できます。
# MAGIC
# MAGIC ```
# MAGIC VACUUM 前                          VACUUM 後
# MAGIC ┌────────────────┐               ┌────────────────┐
# MAGIC │ 現在のファイル    │               │ 現在のファイル    │
# MAGIC │ 古いファイル v1  │  VACUUM ──→   │ （古いファイルは   │
# MAGIC │ 古いファイル v2  │               │   削除済み）      │
# MAGIC └────────────────┘               └────────────────┘
# MAGIC ```
# MAGIC
# MAGIC > **注意**: VACUUM を実行すると、指定期間より古いバージョンへのタイムトラベルが
# MAGIC > **できなくなります**。デフォルトの保持期間は **7日間**（168時間）です。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM のドライラン（実際には削除しない）
# MAGIC -- デフォルトの保持期間（7日）より古いファイルが対象
# MAGIC VACUUM default.sales_delta DRY RUN

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: `VACUUM` のデフォルト保持期間は **7日（168時間）** です。
# MAGIC > 保持期間より短い期間を指定するとエラーになります（安全策）。
# MAGIC > `DRY RUN` を付けると、削除対象のファイルを確認できます（実際には削除しない）。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. スキーマの管理
# MAGIC
# MAGIC Delta Lake ではスキーマの**強制（Enforcement）** と **進化（Evolution）** を管理できます。
# MAGIC
# MAGIC | 機能 | 説明 | デフォルト |
# MAGIC |---|---|---|
# MAGIC | **スキーマ強制** | 定義と異なるスキーマのデータを拒否 | 有効 |
# MAGIC | **スキーマ進化** | 新しいカラムを自動的に追加 | `mergeSchema` オプションで有効化 |

# COMMAND ----------

# スキーマ強制のデモ: 異なるスキーマのデータを書き込もうとするとエラー
from pyspark.sql import Row

try:
    # 存在しないカラム（new_column）を含むデータを書き込む
    wrong_schema_data = [Row(sale_id=99, product="テスト", amount=1000.0, region="東京", sale_date="2024-03-01", new_column="extra")]
    spark.createDataFrame(wrong_schema_data).write.format("delta").mode("append").saveAsTable("default.sales_delta")
    print("書き込み成功（スキーマ進化が有効）")
except Exception as e:
    print(f"スキーマ強制により書き込みが拒否されました:\n{e}")

# COMMAND ----------

# mergeSchema オプションでスキーマ進化を有効化
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType
from datetime import date
from decimal import Decimal

new_schema = StructType([
    StructField("sale_id", IntegerType()),
    StructField("product", StringType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("region", StringType()),
    StructField("sale_date", DateType()),
    StructField("note", StringType()),
])

new_data = spark.createDataFrame([
    (11, "タブレット", Decimal("35000.00"), "東京", date(2024, 3, 1), "新商品")
], schema=new_schema)

new_data.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("default.sales_delta")
print("スキーマ進化（mergeSchema）により、note カラムが追加されました")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- スキーマが進化したことを確認
# MAGIC DESCRIBE TABLE default.sales_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM default.sales_delta ORDER BY sale_id DESC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: Delta Lake はデフォルトでスキーマを強制します。
# MAGIC > スキーマ進化は `.option("mergeSchema", "true")` で有効化できます。
# MAGIC > スキーマ進化では新しいカラムの追加は可能ですが、既存カラムの型変更はできません。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **ACID トランザクション** — データの一貫性を保証する仕組み
# MAGIC 2. **タイムトラベル** — `VERSION AS OF` / `TIMESTAMP AS OF` で過去データを参照
# MAGIC 3. **OPTIMIZE** — 小さなファイルを結合してクエリ性能を改善
# MAGIC 4. **Z-Ordering** — 指定列でデータを物理的に並べ替え
# MAGIC 5. **Liquid Clustering** — `CLUSTER BY` による自動データ配置最適化
# MAGIC 6. **VACUUM** — 不要な古いファイルを削除（デフォルト保持期間: 7日）
# MAGIC 7. **スキーマ管理** — スキーマ強制と進化
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - **次のノートブック `05_medallion_architecture.py`** でメダリオンアーキテクチャを学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Engineer Associate):
# MAGIC > - **Data Processing & Transformations (31%)**: Delta Lake の ACID トランザクション、タイムトラベル、OPTIMIZE、VACUUM、Liquid Clustering

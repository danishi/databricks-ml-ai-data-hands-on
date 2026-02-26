# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Data Intelligence Platform の概要
# MAGIC
# MAGIC このノートブックでは、Databricks Data Intelligence Platform の**主要コンポーネント**を学びます。
# MAGIC **データ分析が初めての方でも安心して取り組める**よう、各概念を丁寧に解説しています。
# MAGIC
# MAGIC **Databricks Data Intelligence Platform とは？**
# MAGIC > データの取り込み・管理・分析・可視化・AI活用までを**一つのプラットフォーム**で実現する統合環境です。
# MAGIC > 従来は複数のツールを組み合わせる必要がありましたが、Databricks ではすべてが統合されています。
# MAGIC
# MAGIC ### このノートブックで学ぶこと
# MAGIC
# MAGIC ```
# MAGIC ① プラットフォームの全体像 → ② Unity Catalog による統合ガバナンス
# MAGIC                                              ↓
# MAGIC ④ Marketplace でデータを発見 ← ③ Catalog Explorer で探索
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - Databricks Data Intelligence Platform の主要コンポーネント
# MAGIC - Unity Catalog の三層名前空間（カタログ / スキーマ / テーブル）
# MAGIC - Catalog Explorer によるデータ資産の探索
# MAGIC - Databricks Marketplace の活用
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks ワークスペースにアクセスできること
# MAGIC - Databricks Runtime（例: 16.x）のクラスターを使用してください
# MAGIC - Unity Catalog が有効なワークスペースを推奨
# MAGIC
# MAGIC > **初心者の方へ**: 各セルのコードは**上から順番に実行**してください。
# MAGIC > セルの左上にある ▶ ボタンをクリックするか、`Shift + Enter` で実行できます。
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. プラットフォームの主要コンポーネント
# MAGIC
# MAGIC Databricks Data Intelligence Platform は以下の主要コンポーネントで構成されています:
# MAGIC
# MAGIC | コンポーネント | 役割 | データアナリストとの関連 |
# MAGIC |---|---|---|
# MAGIC | **Delta Lake** | 高性能なデータストレージ形式（テーブル形式） | クエリの高速化、タイムトラベル、データの信頼性 |
# MAGIC | **Unity Catalog** | データガバナンス・アクセス制御の統合管理 | データの発見、権限管理、リネージ追跡 |
# MAGIC | **Databricks SQL** | SQLによるデータ分析・ダッシュボード作成 | 日常的なデータ分析の主要ツール |
# MAGIC | **Data Intelligence Engine** | AI によるデータ理解・最適化 | クエリの自動最適化、データの自動理解 |
# MAGIC | **Lakeflow Jobs** | ワークフローのオーケストレーション | データパイプラインのスケジュール実行 |
# MAGIC | **Delta Live Tables** | 宣言的なデータパイプライン構築 | ETLパイプラインの簡素化 |
# MAGIC | **Mosaic AI** | AI/MLモデルの構築・デプロイ | AI機能の活用（Genie、Assistant等） |
# MAGIC
# MAGIC > **用語メモ: Lakehouse（レイクハウス）とは？**
# MAGIC >
# MAGIC > **データレイク**（大量の生データを低コストで保管）と**データウェアハウス**（構造化データの高速分析）の
# MAGIC > 良いところを組み合わせたアーキテクチャです。Databricks はこの Lakehouse アーキテクチャを採用しています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delta Lake — データの信頼性を支えるストレージ形式
# MAGIC
# MAGIC **Delta Lake** は、Databricks のデフォルトのテーブル形式です。
# MAGIC 従来のデータレイク（Parquet ファイルなど）に以下の機能を追加します:
# MAGIC
# MAGIC | 機能 | 説明 | メリット |
# MAGIC |---|---|---|
# MAGIC | **ACID トランザクション** | データの読み書きが常に整合性を保つ | データ破損のリスク低減 |
# MAGIC | **タイムトラベル** | 過去の任意の時点のデータを参照可能 | 誤操作からの復旧、監査 |
# MAGIC | **スキーマ管理** | テーブル構造の変更を安全に管理 | 意図しないスキーマ変更を防止 |
# MAGIC | **統合バッチ・ストリーミング** | バッチとストリーミングを同じテーブルで処理 | アーキテクチャの簡素化 |
# MAGIC
# MAGIC Delta Lake のテーブルを作成して確認してみましょう。

# COMMAND ----------

# 初期設定: カタログとスキーマの指定
catalog = "main"
schema = "default"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"使用するカタログ: {catalog}")
print(f"使用するスキーマ: {schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta Lake テーブルの作成
# MAGIC CREATE TABLE IF NOT EXISTS da_sample_products (
# MAGIC   product_id INT,
# MAGIC   product_name STRING,
# MAGIC   category STRING,
# MAGIC   price DECIMAL(10,2),
# MAGIC   stock_quantity INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'データアナリストハンズオン用サンプル商品テーブル';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- サンプルデータの挿入
# MAGIC INSERT OVERWRITE da_sample_products VALUES
# MAGIC   (1, 'ノートPC', 'Electronics', 89800.00, 50),
# MAGIC   (2, 'ワイヤレスマウス', 'Electronics', 3980.00, 200),
# MAGIC   (3, 'デスクチェア', 'Furniture', 29800.00, 30),
# MAGIC   (4, 'モニターアーム', 'Furniture', 5980.00, 80),
# MAGIC   (5, 'USBハブ', 'Electronics', 2480.00, 150),
# MAGIC   (6, 'スタンディングデスク', 'Furniture', 45800.00, 20),
# MAGIC   (7, 'ウェブカメラ', 'Electronics', 7980.00, 100),
# MAGIC   (8, 'ヘッドセット', 'Electronics', 12800.00, 75);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの中身を確認
# MAGIC SELECT * FROM da_sample_products ORDER BY product_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Lake のテーブル詳細を確認
# MAGIC
# MAGIC `DESCRIBE DETAIL` コマンドで、テーブルのメタデータ（形式、場所、作成日時など）を確認できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL da_sample_products;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの履歴を確認（タイムトラベル）
# MAGIC DESCRIBE HISTORY da_sample_products;

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: ACID トランザクションとは？**
# MAGIC >
# MAGIC > - **A**tomicity（原子性）: 処理は「全部成功」か「全部取り消し」のどちらか
# MAGIC > - **C**onsistency（一貫性）: データは常に正しい状態を保つ
# MAGIC > - **I**solation（独立性）: 複数の処理が同時に行われても互いに影響しない
# MAGIC > - **D**urability（永続性）: 一度確定したデータは失われない
# MAGIC >
# MAGIC > 銀行のATMで例えると、「お金を引き出す途中でエラーが起きても、残高がおかしくならない」仕組みです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unity Catalog — 統合ガバナンスレイヤー
# MAGIC
# MAGIC **Unity Catalog** は、Databricks のすべてのデータ資産を一元管理するガバナンスレイヤーです。
# MAGIC
# MAGIC ### 三層名前空間（3-Level Namespace）
# MAGIC
# MAGIC Unity Catalog は以下の三層構造でデータを整理します:
# MAGIC
# MAGIC ```
# MAGIC カタログ（Catalog）
# MAGIC └── スキーマ（Schema / Database）
# MAGIC     ├── テーブル（Table）
# MAGIC     ├── ビュー（View）
# MAGIC     ├── ボリューム（Volume）
# MAGIC     └── 関数（Function）
# MAGIC ```
# MAGIC
# MAGIC | 階層 | 説明 | 例 |
# MAGIC |---|---|---|
# MAGIC | **カタログ** | 最上位の論理グループ（部門・環境ごと） | `production`, `development`, `main` |
# MAGIC | **スキーマ** | テーブルやビューのグループ | `sales`, `marketing`, `default` |
# MAGIC | **テーブル / ビュー** | 実際のデータオブジェクト | `customers`, `orders` |
# MAGIC
# MAGIC 完全修飾名: `catalog.schema.table`（例: `main.default.da_sample_products`）

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 利用可能なカタログの一覧
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 現在のカタログ内のスキーマ一覧
# MAGIC SHOW SCHEMAS IN main;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 現在のスキーマ内のテーブル一覧
# MAGIC SHOW TABLES IN main.default;

# COMMAND ----------

# MAGIC %md
# MAGIC ### マネージドテーブル vs 外部テーブル
# MAGIC
# MAGIC Unity Catalog では2種類のテーブルがあります:
# MAGIC
# MAGIC | 種類 | データの保管場所 | テーブル削除時のデータ | 用途 |
# MAGIC |---|---|---|---|
# MAGIC | **マネージドテーブル** | Unity Catalog が管理する場所 | **データも削除される** | 一般的な分析用テーブル |
# MAGIC | **外部テーブル** | ユーザーが指定した外部ストレージ | **データは残る** | 既存データへの参照、共有データ |
# MAGIC
# MAGIC > **認定試験のポイント**: マネージドテーブルと外部テーブルの違い（特にDROP TABLE時の挙動）は頻出です。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの種類を確認（MANAGED か EXTERNAL か）
# MAGIC DESCRIBE EXTENDED da_sample_products;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ビュー（View）の種類
# MAGIC
# MAGIC ビューはテーブルのように扱えますが、データ自体は保持しません（クエリの定義を保存）。
# MAGIC
# MAGIC | ビューの種類 | 説明 | 更新タイミング |
# MAGIC |---|---|---|
# MAGIC | **標準ビュー** | クエリ実行時に毎回元テーブルを参照 | クエリ実行時 |
# MAGIC | **一時ビュー** | セッション内のみ有効 | セッション内 |
# MAGIC | **マテリアライズドビュー** | クエリ結果を物理的に保存し高速化 | リフレッシュ時 |
# MAGIC
# MAGIC > **認定試験のポイント**: 標準ビュー、一時ビュー、マテリアライズドビューの違いと使い分けは重要な出題範囲です。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 標準ビューの作成
# MAGIC CREATE OR REPLACE VIEW da_electronics_view AS
# MAGIC SELECT product_id, product_name, price, stock_quantity
# MAGIC FROM da_sample_products
# MAGIC WHERE category = 'Electronics';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ビューの内容を確認（実行時に元テーブルを参照する）
# MAGIC SELECT * FROM da_electronics_view ORDER BY price DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 一時ビューの作成（セッション内のみ有効）
# MAGIC CREATE OR REPLACE TEMPORARY VIEW da_expensive_products_temp AS
# MAGIC SELECT product_name, category, price
# MAGIC FROM da_sample_products
# MAGIC WHERE price > 10000;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM da_expensive_products_temp;

# COMMAND ----------

# MAGIC %md
# MAGIC > **試してみよう**: ノートブックをデタッチして再アタッチした後に
# MAGIC > `SELECT * FROM da_expensive_products_temp` を実行してみましょう。
# MAGIC > 一時ビューはセッション終了で消えるため、エラーになります。
# MAGIC > 一方 `da_electronics_view` は永続ビューなので引き続き使えます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Catalog Explorer によるデータ探索
# MAGIC
# MAGIC **Catalog Explorer** は、Unity Catalog 内のデータ資産をGUIで探索できるツールです。
# MAGIC
# MAGIC ### Catalog Explorer でできること
# MAGIC
# MAGIC | 機能 | 説明 |
# MAGIC |---|---|
# MAGIC | **データ資産の閲覧** | カタログ / スキーマ / テーブルを階層的にブラウズ |
# MAGIC | **スキーマの確認** | テーブルのカラム名・データ型・コメントを確認 |
# MAGIC | **データプレビュー** | テーブルのサンプルデータを直接確認 |
# MAGIC | **リネージの追跡** | データがどこから来て、どこで使われているかを可視化 |
# MAGIC | **タグの管理** | データ資産にタグを付けて分類・検索を容易に |
# MAGIC | **権限の確認・設定** | アクセス権限の確認と付与 |
# MAGIC
# MAGIC ### Catalog Explorer の開き方
# MAGIC
# MAGIC 1. 左サイドバーの **「カタログ」** アイコンをクリック
# MAGIC 2. ツリー構造でカタログ → スキーマ → テーブルを展開
# MAGIC 3. テーブルを選択すると、スキーマ・サンプルデータ・詳細情報を確認できます

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルのカラム情報を確認（Catalog Explorer のスキーマタブと同じ情報）
# MAGIC DESCRIBE TABLE da_sample_products;

# COMMAND ----------

# MAGIC %md
# MAGIC ### データリネージ（Data Lineage）
# MAGIC
# MAGIC **データリネージ**は、データがどこから来て（上流）、どこで使われているか（下流）を追跡する機能です。
# MAGIC
# MAGIC ```
# MAGIC [ソーステーブル] → [変換処理] → [結果テーブル] → [ダッシュボード]
# MAGIC     上流（Upstream）                 下流（Downstream）
# MAGIC ```
# MAGIC
# MAGIC Catalog Explorer の **「リネージ」** タブで視覚的に確認できます。
# MAGIC
# MAGIC > **認定試験のポイント**: リネージを使って影響範囲を分析する方法や、
# MAGIC > データの出所を確認する手順は出題されることがあります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Databricks SQL — データアナリストの主要ツール
# MAGIC
# MAGIC **Databricks SQL** は、SQL を使ったデータ分析に特化した環境です。
# MAGIC
# MAGIC | 機能 | 説明 |
# MAGIC |---|---|
# MAGIC | **SQL エディタ** | SQL クエリの作成・実行・保存 |
# MAGIC | **SQL ウェアハウス** | クエリ実行用のコンピューティングリソース |
# MAGIC | **ダッシュボード** | データの可視化・レポート作成 |
# MAGIC | **アラート** | データの条件監視・通知 |
# MAGIC | **Databricks Assistant** | AI によるクエリ作成支援 |
# MAGIC
# MAGIC ### SQL ウェアハウスの種類
# MAGIC
# MAGIC | 種類 | 説明 | 特徴 |
# MAGIC |---|---|---|
# MAGIC | **Serverless** | Databricks が管理するインフラ | 起動が速い、管理不要 |
# MAGIC | **Pro** | 高度な機能を備えたウェアハウス | サーバーレスクエリ連携 |
# MAGIC | **Classic** | 従来型のウェアハウス | カスタマイズ性が高い |
# MAGIC
# MAGIC > **用語メモ: SQL ウェアハウスとは？**
# MAGIC >
# MAGIC > SQL クエリを実行するための「エンジン」です。車でいえばエンジンに相当します。
# MAGIC > ウェアハウスが起動していないとクエリを実行できません。
# MAGIC > **Serverless** は「必要な時だけ自動で起動・停止する」ため、コスト効率が良いです。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks Assistant
# MAGIC
# MAGIC **Databricks Assistant** は、AIを使ってSQL クエリの作成・デバッグを支援するツールです。
# MAGIC
# MAGIC | 機能 | 説明 |
# MAGIC |---|---|
# MAGIC | **自然言語からSQLを生成** | 「売上トップ10の商品を表示」→ SQL に変換 |
# MAGIC | **クエリのデバッグ** | エラーの原因を分析し修正案を提示 |
# MAGIC | **クエリの最適化** | より効率的なクエリへの書き換えを提案 |
# MAGIC | **コードの説明** | 既存のクエリの意味をわかりやすく説明 |
# MAGIC
# MAGIC > **使い方**: SQL エディタやノートブック内で、Assistantアイコンをクリックするか
# MAGIC > `Cmd/Ctrl + I` で起動できます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Databricks Marketplace
# MAGIC
# MAGIC **Databricks Marketplace** は、外部のデータセットやソリューションを発見・取得できるマーケットプレイスです。
# MAGIC
# MAGIC | 項目 | 説明 |
# MAGIC |---|---|
# MAGIC | **データセット** | 公開データ、商用データを検索・取得 |
# MAGIC | **ソリューション** | ノートブック、ダッシュボードのテンプレート |
# MAGIC | **モデル** | 学習済みAIモデル |
# MAGIC
# MAGIC ### Marketplace の利用手順
# MAGIC
# MAGIC 1. 左サイドバーの **「マーケットプレイス」** をクリック
# MAGIC 2. カテゴリやキーワードで検索
# MAGIC 3. リスティングの詳細を確認し、**「Get」** または **「Request」** をクリック
# MAGIC 4. 取得したデータはUnity Catalog内に自動的に登録
# MAGIC
# MAGIC > **認定試験のポイント**: Marketplace でのデータ発見と取得のワークフロー、
# MAGIC > Consumer（利用者）としての操作方法が出題範囲です。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **Databricks Data Intelligence Platform の主要コンポーネント** — Delta Lake, Unity Catalog, Databricks SQL など
# MAGIC 2. **Delta Lake** — ACID トランザクション、タイムトラベル、スキーマ管理
# MAGIC 3. **Unity Catalog の三層名前空間** — カタログ / スキーマ / テーブルの階層構造
# MAGIC 4. **マネージドテーブル vs 外部テーブル** — DROP TABLE 時の挙動の違い
# MAGIC 5. **ビューの種類** — 標準ビュー、一時ビュー、マテリアライズドビュー
# MAGIC 6. **Catalog Explorer** — データ資産の探索、リネージ追跡、タグ管理
# MAGIC 7. **Databricks SQL と SQL ウェアハウス** — Serverless / Pro / Classic の違い
# MAGIC 8. **Databricks Marketplace** — 外部データの発見と取得
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - Catalog Explorer で作成したテーブルの詳細を確認してみましょう
# MAGIC - **次のノートブック `02_managing_data.py`** でデータの管理・クリーニングを学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Analyst Associate):
# MAGIC > - **Understanding of Databricks Data Intelligence Platform (11%)**: プラットフォームコンポーネント、Unity Catalog、Catalog Explorer

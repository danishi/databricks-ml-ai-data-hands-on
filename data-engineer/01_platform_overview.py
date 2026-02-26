# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Data Intelligence Platform の概要
# MAGIC
# MAGIC このノートブックでは、Databricks Data Intelligence Platform の基本的な構成要素と機能を学びます。
# MAGIC **データエンジニアリングが初めての方でも安心して取り組める**よう、各要素を丁寧に解説しています。
# MAGIC
# MAGIC **Databricks Data Intelligence Platform とは？**
# MAGIC > データの取り込み・加工・分析・機械学習を **一つのプラットフォーム上で** 実現する統合環境です。
# MAGIC > 従来は別々のツールで行っていた作業を、Databricks 上でシームレスに行えます。
# MAGIC
# MAGIC ### プラットフォームの全体像
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │              Databricks Data Intelligence Platform       │
# MAGIC │                                                         │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
# MAGIC │  │ Notebook  │  │ Cluster  │  │Git folders│  ← 開発環境 │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘             │
# MAGIC │                                                         │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
# MAGIC │  │Delta Lake│  │Unity     │  │ Lakeflow │  ← 基盤技術  │
# MAGIC │  │          │  │Catalog   │  │  Jobs    │             │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘             │
# MAGIC │                                                         │
# MAGIC │  ┌──────────────────────────────────────────┐          │
# MAGIC │  │         クラウドストレージ (S3/ADLS/GCS)    │ ← データ │
# MAGIC │  └──────────────────────────────────────────┘          │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - ノートブックの基本操作とマジックコマンド
# MAGIC - クラスターの種類と使い分け
# MAGIC - Git フォルダーによる Git 連携
# MAGIC - クエリの最適化とコンピュートの選択
# MAGIC - Databricks の価値提案（レイクハウスアーキテクチャ）
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime（例: 16.x）のクラスターを使用してください
# MAGIC - クラスターサイズは **シングルノード（Single Node）** で十分です
# MAGIC
# MAGIC > **初心者の方へ**: 各セルのコードは**上から順番に実行**してください。
# MAGIC > セルの左上にある ▶ ボタンをクリックするか、`Shift + Enter` で実行できます。
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ノートブックの基本操作
# MAGIC
# MAGIC Databricks ノートブックは、コードと説明文を交互に記述できる対話型の開発環境です。
# MAGIC
# MAGIC ### セルの種類
# MAGIC
# MAGIC | セルの種類 | 説明 | 使い方 |
# MAGIC |---|---|---|
# MAGIC | Python セル | Python コードを実行 | デフォルト |
# MAGIC | SQL セル | SQL クエリを実行 | `%sql` マジックコマンド |
# MAGIC | Markdown セル | 説明文を記述 | `%md` マジックコマンド |
# MAGIC | Shell セル | シェルコマンドを実行 | `%sh` マジックコマンド |
# MAGIC | R セル | R コードを実行 | `%r` マジックコマンド |
# MAGIC
# MAGIC > **用語メモ: マジックコマンドとは？**
# MAGIC >
# MAGIC > セルの先頭に `%` を付けることで、セルの言語を切り替えられます。
# MAGIC > たとえば Python ノートブックの中でも `%sql` と書けば SQL を実行できます。
# MAGIC > 「魔法のように言語が切り替わる」ので「マジックコマンド」と呼ばれます。

# COMMAND ----------

# Python セル（デフォルト）
print("これは Python セルです")
print(f"現在の Spark バージョン: {spark.version}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- これは SQL セルです（%sql マジックコマンド）
# MAGIC SELECT 'Hello from SQL!' AS message, current_date() AS today

# COMMAND ----------

# MAGIC %md
# MAGIC ### `%run` マジックコマンド
# MAGIC
# MAGIC `%run` を使うと、他のノートブックを実行してその変数や関数を取り込めます。
# MAGIC
# MAGIC ```python
# MAGIC %run ./shared_config  # 同じフォルダ内の shared_config ノートブックを実行
# MAGIC ```
# MAGIC
# MAGIC > **試験ポイント**: `%run` は**別のノートブックの変数を現在のノートブックに取り込む**ために使います。
# MAGIC > 実行されたノートブック内で定義された変数や関数がそのまま使えるようになります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ウィジェット（パラメータ化）
# MAGIC
# MAGIC ウィジェットを使うと、ノートブックにパラメータを渡せます。
# MAGIC Lakeflow Jobs（旧 Workflows）でジョブとして実行する際に特に便利です。
# MAGIC
# MAGIC | ウィジェットの種類 | 説明 |
# MAGIC |---|---|
# MAGIC | `text` | テキスト入力 |
# MAGIC | `dropdown` | ドロップダウン選択 |
# MAGIC | `combobox` | コンボボックス |
# MAGIC | `multiselect` | 複数選択 |

# COMMAND ----------

# テキストウィジェットの作成
dbutils.widgets.text("greeting", "Hello", "挨拶メッセージ")
dbutils.widgets.dropdown("language", "ja", ["ja", "en", "fr"], "言語")

# ウィジェットの値を取得
greeting = dbutils.widgets.get("greeting")
language = dbutils.widgets.get("language")

print(f"挨拶: {greeting}")
print(f"言語: {language}")

# COMMAND ----------

# ウィジェットの削除（後片付け）
dbutils.widgets.removeAll()
print("ウィジェットを削除しました")

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: ウィジェットは `dbutils.widgets` で作成・取得・削除できます。
# MAGIC > Lakeflow Jobs のジョブパラメータとして渡すこともできます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. クラスターの種類と選択
# MAGIC
# MAGIC Databricks では用途に応じて異なるクラスター構成を選択します。
# MAGIC
# MAGIC ### クラスタータイプの比較
# MAGIC
# MAGIC | クラスタータイプ | 説明 | 適した用途 |
# MAGIC |---|---|---|
# MAGIC | **All-Purpose（汎用）** | 対話型の開発・分析用 | ノートブック開発、アドホック分析 |
# MAGIC | **Job（ジョブ）** | 自動化されたジョブ実行用 | 本番 ETL パイプライン |
# MAGIC | **SQL Warehouse** | SQL クエリ実行に特化 | BI ダッシュボード、SQL 分析 |
# MAGIC
# MAGIC ### コンピュートの選択基準
# MAGIC
# MAGIC | シナリオ | 推奨コンピュート |
# MAGIC |---|---|
# MAGIC | ノートブックで探索的にデータを確認 | All-Purpose クラスター |
# MAGIC | 毎日実行する ETL ジョブ | Job クラスター（コスト最適） |
# MAGIC | BI ツールから SQL を実行 | SQL Warehouse |
# MAGIC | 大量データの並列処理 | マルチノードクラスター |
# MAGIC | 小規模な開発・テスト | シングルノードクラスター |
# MAGIC
# MAGIC > **用語メモ: All-Purpose vs Job クラスター**
# MAGIC >
# MAGIC > All-Purpose クラスターは**手動で起動/停止**します。複数のノートブックを切り替えながら使えます。
# MAGIC > Job クラスターは**ジョブ開始時に自動起動、終了時に自動停止**します。そのためコスト効率が高いです。

# COMMAND ----------

# 現在のクラスター情報を確認
print("=== 現在のクラスター情報 ===")
print(f"Spark バージョン: {spark.version}")
print("Spark コンフィグ例:")

# よく使うコンフィグの確認
configs = [
    ("spark.databricks.clusterUsageTags.clusterName", "クラスター名"),
    ("spark.databricks.clusterUsageTags.sparkVersion", "Runtime バージョン"),
]

for key, label in configs:
    try:
        value = spark.conf.get(key)
        print(f"  {label}: {value}")
    except Exception:
        print(f"  {label}: (取得不可)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### サーバーレスコンピュート
# MAGIC
# MAGIC Databricks では**サーバーレスコンピュート**も利用可能です。
# MAGIC
# MAGIC | 特徴 | 従来のクラスター | サーバーレス |
# MAGIC |---|---|---|
# MAGIC | 起動時間 | 数分 | 数秒 |
# MAGIC | 管理 | ユーザーが設定 | Databricks が自動管理 |
# MAGIC | スケーリング | 手動/自動スケール設定 | 完全自動 |
# MAGIC | コスト | 稼働時間課金 | 使用時間のみ課金 |
# MAGIC
# MAGIC > **試験ポイント**: サーバーレスコンピュートは起動が速く管理不要ですが、
# MAGIC > カスタムライブラリのインストールなど一部の機能に制約があります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Git フォルダー（Git 連携）
# MAGIC
# MAGIC **Git フォルダー**（旧称: Databricks Repos）を使うと、
# MAGIC Git リポジトリと Databricks ワークスペースを連携できます。
# MAGIC
# MAGIC ### Git フォルダーでできること
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐    clone/pull/push    ┌──────────────┐
# MAGIC │   GitHub     │ ◄──────────────────► │  Databricks  │
# MAGIC │   GitLab     │                      │  Git folders │
# MAGIC │   Azure DevOps│                      │              │
# MAGIC └─────────────┘                       └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC | 操作 | 説明 |
# MAGIC |---|---|
# MAGIC | Clone | リモートリポジトリをワークスペースにコピー |
# MAGIC | Pull | リモートの変更をローカルに取り込む |
# MAGIC | Push | ローカルの変更をリモートに反映 |
# MAGIC | Branch | ブランチの作成・切り替え |
# MAGIC | Commit | 変更をコミット |
# MAGIC
# MAGIC ### 旧 Repos からの変更点
# MAGIC
# MAGIC | 項目 | 旧 Repos | Git フォルダー |
# MAGIC |---|---|---|
# MAGIC | 配置場所 | `/Repos` 配下のみ | ワークスペースの任意の階層 |
# MAGIC | 対応アセット | ノートブック、ファイル | + SQL アセット、MLflow 実験等 |
# MAGIC | リモート URL | 任意 | 必須 |
# MAGIC | UI 操作 | 新規 > Repo | 新規 > Git フォルダー |
# MAGIC
# MAGIC > **試験ポイント**: Git フォルダー（旧 Repos）は Git との統合を提供し、
# MAGIC > ノートブックや Python ファイルのバージョン管理を可能にします。
# MAGIC > CI/CD パイプラインとの統合にも活用されます。
# MAGIC > 既存の `/Repos` パスは引き続き動作します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dbutils ユーティリティ
# MAGIC
# MAGIC `dbutils` は Databricks が提供するユーティリティツールです。
# MAGIC ファイル操作やシークレット管理など、さまざまな機能を提供します。
# MAGIC
# MAGIC | カテゴリ | コマンド例 | 説明 |
# MAGIC |---|---|---|
# MAGIC | ファイル操作 | `dbutils.fs.ls("/")` | ファイル一覧表示 |
# MAGIC | | `dbutils.fs.head("/path")` | ファイル先頭を表示 |
# MAGIC | ノートブック | `dbutils.notebook.run("path", timeout)` | 別ノートブックを実行 |
# MAGIC | ウィジェット | `dbutils.widgets.text("name", "default")` | ウィジェット作成 |
# MAGIC | シークレット | `dbutils.secrets.get("scope", "key")` | シークレット取得 |
# MAGIC | ライブラリ | `dbutils.library.restartPython()` | Python 環境の再起動 |

# COMMAND ----------

# dbutils.fs でファイルシステムを確認
print("=== DBFS ルートディレクトリの内容 ===")
try:
    files = dbutils.fs.ls("/")
    for f in files[:10]:  # 先頭10件を表示
        file_type = "DIR" if f.isDir() else "FILE"
        print(f"  [{file_type}] {f.name}")
except Exception as e:
    print(f"  DBFS へのアクセスが制限されています（Unity Catalog 環境では正常な動作です）")
    print(f"  理由: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **用語メモ: DBFS（Databricks File System）とは？**
# MAGIC >
# MAGIC > DBFS は Databricks が提供する分散ファイルシステムです。
# MAGIC > クラウドストレージ（S3, ADLS, GCS）を抽象化して、ローカルファイルのように扱えます。
# MAGIC > `dbutils.fs` や `/dbfs/` パスでアクセスできます。
# MAGIC >
# MAGIC > **注意**: Unity Catalog が有効な環境では、DBFS へのアクセスが制限される場合があります。
# MAGIC > 上のセルでエラーが出た場合は正常な動作です。現在は Unity Catalog の
# MAGIC > **ボリューム（Volumes）** を使ったファイルアクセスが推奨されています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. クエリの最適化
# MAGIC
# MAGIC Databricks には、クエリ性能を自動的に最適化する機能が組み込まれています。
# MAGIC
# MAGIC ### 主な最適化機能
# MAGIC
# MAGIC | 機能 | 説明 |
# MAGIC |---|---|
# MAGIC | **Photon エンジン** | ネイティブ実行エンジンで Spark SQL を高速化 |
# MAGIC | **Adaptive Query Execution (AQE)** | 実行時にクエリプランを動的に最適化 |
# MAGIC | **Predictive I/O** | データアクセスパターンを予測して I/O を最適化 |
# MAGIC | **Liquid Clustering** | データの物理配置を自動的に最適化（後のノートブックで詳述） |
# MAGIC
# MAGIC > **試験ポイント**: Photon は Delta Lake と Spark SQL のクエリを高速化するエンジンです。
# MAGIC > Photon 対応の Runtime を選択することで自動的に有効になります。

# COMMAND ----------

# AQE（Adaptive Query Execution）の設定を確認
try:
    aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")
except Exception:
    aqe_enabled = "デフォルトで有効（設定値の直接参照は不可）"
print(f"Adaptive Query Execution: {aqe_enabled}")

# AQE の主な最適化
print("\n=== AQE の主な最適化 ===")
optimizations = [
    ("Coalescing Shuffle Partitions", "小さすぎるパーティションを結合して減らす"),
    ("Converting Sort Merge Join to Broadcast Join", "小さいテーブルをブロードキャストして結合を高速化"),
    ("Skew Join Optimization", "データの偏り（スキュー）を自動的に検出して対処"),
]
for name, desc in optimizations:
    print(f"  • {name}")
    print(f"    → {desc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. レイクハウスアーキテクチャ
# MAGIC
# MAGIC Databricks の**レイクハウスアーキテクチャ**は、データレイクとデータウェアハウスの長所を組み合わせた設計です。
# MAGIC
# MAGIC ### 従来のアーキテクチャとの比較
# MAGIC
# MAGIC ```
# MAGIC 従来: データレイク + データウェアハウス（2つのシステム）
# MAGIC ┌──────────────┐    ETL     ┌──────────────────┐
# MAGIC │  データレイク   │ ────────→ │ データウェアハウス   │
# MAGIC │  (生データ保存) │           │ (構造化データ分析)  │
# MAGIC └──────────────┘           └──────────────────┘
# MAGIC
# MAGIC レイクハウス: 一つの統合プラットフォーム
# MAGIC ┌──────────────────────────────────────┐
# MAGIC │         レイクハウス（Delta Lake）       │
# MAGIC │  ┌──────────┐  ┌──────────────────┐ │
# MAGIC │  │生データ保存│ + │ 構造化データ分析   │ │
# MAGIC │  └──────────┘  └──────────────────┘ │
# MAGIC │  → ACID トランザクション                │
# MAGIC │  → スキーマ管理                        │
# MAGIC │  → タイムトラベル                       │
# MAGIC └──────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC | 特徴 | データレイク | データウェアハウス | レイクハウス |
# MAGIC |---|---|---|---|
# MAGIC | データ形式 | 非構造化OK | 構造化のみ | 両方対応 |
# MAGIC | トランザクション | なし | あり | あり（Delta Lake） |
# MAGIC | コスト | 低い | 高い | 中程度 |
# MAGIC | データ品質 | 低い | 高い | 高い |
# MAGIC | 機械学習 | 対応 | 困難 | 対応 |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta Lake のデモ用テーブルを作成
# MAGIC CREATE OR REPLACE TEMP VIEW platform_demo AS
# MAGIC SELECT
# MAGIC   'Databricks Data Intelligence Platform' AS platform,
# MAGIC   'Delta Lake' AS storage_format,
# MAGIC   'Unity Catalog' AS governance,
# MAGIC   'Photon' AS query_engine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM platform_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. 最新の用語変更
# MAGIC
# MAGIC Databricks は 2024〜2025年にかけてプラットフォームの名称を大幅に刷新しました。
# MAGIC 試験では最新の用語が使われるため、以下の対応表を把握しておきましょう。
# MAGIC
# MAGIC | 旧名称 | 新名称 | 変更時期 |
# MAGIC |---|---|---|
# MAGIC | Lakehouse Platform | **Data Intelligence Platform** | 2024 |
# MAGIC | Databricks Repos | **Git フォルダー（Git folders）** | 2024 |
# MAGIC | Delta Live Tables (DLT) | **Lakeflow Declarative Pipelines** | 2024-2025 |
# MAGIC | Databricks Workflows | **Lakeflow Jobs** | 2024-2025 |
# MAGIC | Lakeview Dashboards | **AI/BI Dashboards** | 2024 |
# MAGIC
# MAGIC > **Lakeflow について**: Databricks は「**Lakeflow**」ブランドの下に
# MAGIC > データエンジニアリング関連機能を統合しています。
# MAGIC > - **Lakeflow Connect** — データの取り込み
# MAGIC > - **Lakeflow Declarative Pipelines** — 宣言型 ETL（旧 DLT）
# MAGIC > - **Lakeflow Jobs** — ワークフローのオーケストレーション（旧 Workflows）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Databricks のヘルプと学習リソース
# MAGIC
# MAGIC Databricks には充実したドキュメントと学習リソースがあります。
# MAGIC
# MAGIC | リソース | 内容 |
# MAGIC |---|---|
# MAGIC | **Databricks ドキュメント** | 公式リファレンス |
# MAGIC | **Databricks Academy** | 無料のオンライン学習コース |
# MAGIC | **Community Edition** | 無料のクラウド環境（一部機能制限あり） |
# MAGIC | **ノートブック内ヘルプ** | `help()` 関数や `?` でドキュメント参照 |

# COMMAND ----------

# ヘルプの確認方法
print("=== ヘルプの確認方法 ===")
print("1. help(関数名)  → 関数のドキュメントを表示")
print("2. 関数名?       → Jupyter スタイルのヘルプ")
print("3. dbutils.fs.help() → dbutils のヘルプ")
print("4. dbutils.help()    → dbutils 全体のヘルプ")
print()

# dbutils のヘルプを表示
try:
    dbutils.fs.help()
except Exception:
    print("dbutils.fs.help() は現在の環境では利用できません。")
    print("dbutils の主なモジュール: fs, notebook, widgets, secrets, library")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **ノートブックの基本** — マジックコマンド（%sql, %md, %sh）、%run によるノートブック連携
# MAGIC 2. **ウィジェット** — パラメータ化されたノートブックの作成
# MAGIC 3. **クラスターの種類** — All-Purpose、Job、SQL Warehouse の使い分け
# MAGIC 4. **Git フォルダー** — Git 連携によるバージョン管理（旧 Repos）
# MAGIC 5. **dbutils** — ファイル操作やシークレット管理
# MAGIC 6. **クエリ最適化** — Photon、AQE による自動最適化
# MAGIC 7. **レイクハウスアーキテクチャ** — データレイク + DWH の統合
# MAGIC 8. **用語変更** — Repos → Git フォルダー、Workflows → Lakeflow Jobs 等
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - **次のノートブック `02_data_ingestion.py`** でデータの取り込み方法を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Engineer Associate):
# MAGIC > - **Databricks Intelligence Platform (10%)**: ワークスペースの構成要素、マジックコマンド、クラスタータイプ、Git フォルダー、クエリ最適化

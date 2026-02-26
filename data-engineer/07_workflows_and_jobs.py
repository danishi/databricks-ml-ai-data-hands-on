# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Jobs（旧 Workflows）、ジョブ管理、CI/CD
# MAGIC
# MAGIC このノートブックでは、**Lakeflow Jobs**（旧 Databricks Workflows）によるジョブの管理・スケジューリング、
# MAGIC 障害復旧、**Databricks Asset Bundles（DABs）** による CI/CD を学びます。
# MAGIC
# MAGIC **Lakeflow Jobs とは？**
# MAGIC > データパイプラインやMLワークロードを **スケジューリング・オーケストレーション** するサービスです。
# MAGIC > 複数のタスクを依存関係に基づいて実行し、監視・アラートを自動化します。
# MAGIC > 以前は **Databricks Workflows** と呼ばれていましたが、Lakeflow ブランドに統合されました。
# MAGIC
# MAGIC ### Lakeflow Jobs の全体像
# MAGIC
# MAGIC ```
# MAGIC ┌───────────────────────────────────────────────────────┐
# MAGIC │                      Lakeflow Jobs                     │
# MAGIC │                                                       │
# MAGIC │  ┌─────────┐    ┌─────────┐    ┌─────────┐          │
# MAGIC │  │ Task A  │───→│ Task B  │───→│ Task C  │          │
# MAGIC │  │(Bronze) │    │(Silver) │    │(Gold)   │          │
# MAGIC │  └─────────┘    └─────────┘    └─────────┘          │
# MAGIC │       │                              │               │
# MAGIC │       │         ┌─────────┐          │               │
# MAGIC │       └────────→│ Task D  │──────────┘               │
# MAGIC │                 │(テスト)  │                          │
# MAGIC │                 └─────────┘                          │
# MAGIC │                                                       │
# MAGIC │  スケジュール: CRON  │  アラート: メール/Slack          │
# MAGIC └───────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - Lakeflow Jobs のジョブ作成と管理
# MAGIC - CRON によるスケジューリング
# MAGIC - タスク間の依存関係
# MAGIC - 障害復旧とリトライ設定
# MAGIC - Databricks Asset Bundles（DABs）による CI/CD
# MAGIC - サーバーレスコンピュート
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime（例: 16.x）のクラスター
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. ジョブの基本概念
# MAGIC
# MAGIC ### ジョブの構成要素
# MAGIC
# MAGIC | 要素 | 説明 |
# MAGIC |---|---|
# MAGIC | **ジョブ** | 1つ以上のタスクをまとめた実行単位 |
# MAGIC | **タスク** | 実行する処理の最小単位（ノートブック、Python スクリプト、SQL など） |
# MAGIC | **実行（Run）** | ジョブの1回の実行インスタンス |
# MAGIC | **トリガー** | ジョブを起動する条件（スケジュール、手動、ファイル到着など） |
# MAGIC | **クラスター** | タスクを実行するコンピュートリソース |
# MAGIC
# MAGIC ### タスクの種類
# MAGIC
# MAGIC | タスクタイプ | 説明 | 用途 |
# MAGIC |---|---|---|
# MAGIC | Notebook | ノートブックを実行 | ETL 処理、データ変換 |
# MAGIC | Python Script | Python ファイルを実行 | 汎用スクリプト |
# MAGIC | SQL | SQL クエリを実行 | データ分析、テーブル作成 |
# MAGIC | Pipeline | Lakeflow Declarative Pipeline を実行 | 宣言型 ETL |
# MAGIC | JAR | Java/Scala JAR を実行 | 高性能処理 |
# MAGIC | dbt | dbt プロジェクトを実行 | データモデリング |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ジョブの作成（API を使った例）
# MAGIC
# MAGIC ジョブは UI からも API からも作成できます。以下は Python SDK を使った例です。

# COMMAND ----------

# Databricks SDK を使ったジョブの概念説明
# （実際のジョブ作成は Lakeflow Jobs の UI から行うのが一般的です）

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# 現在のジョブ一覧を確認
print("=== 既存のジョブ一覧（先頭5件） ===")
jobs = list(w.jobs.list(limit=5))
if jobs:
    for job in jobs:
        print(f"  ジョブ ID: {job.job_id}, 名前: {job.settings.name}")
else:
    print("  （ジョブが見つかりません）")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ジョブ定義の JSON 構造
# MAGIC
# MAGIC ジョブは以下のような JSON 構造で定義されます。
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "daily_etl_pipeline",
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "bronze_ingestion",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Workspace/Users/user/project/data-engineer/02_data_ingestion"
# MAGIC       },
# MAGIC       "job_cluster_key": "etl_cluster"
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "silver_transformation",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Workspace/Users/user/project/data-engineer/05_medallion_architecture"
# MAGIC       },
# MAGIC       "depends_on": [{"task_key": "bronze_ingestion"}],
# MAGIC       "job_cluster_key": "etl_cluster"
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "gold_aggregation",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/Workspace/Users/user/project/data-engineer/05_medallion_architecture"
# MAGIC       },
# MAGIC       "depends_on": [{"task_key": "silver_transformation"}],
# MAGIC       "job_cluster_key": "etl_cluster"
# MAGIC     }
# MAGIC   ],
# MAGIC   "job_clusters": [
# MAGIC     {
# MAGIC       "job_cluster_key": "etl_cluster",
# MAGIC       "new_cluster": {
# MAGIC         "spark_version": "16.0.x-scala2.12",
# MAGIC         "num_workers": 2,
# MAGIC         "node_type_id": "i3.xlarge"
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC > **試験ポイント**: Job クラスターはジョブ開始時に自動作成・終了時に自動削除されるため、
# MAGIC > All-Purpose クラスターより**コスト効率が高い**です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. CRON によるスケジューリング
# MAGIC
# MAGIC Lakeflow Jobs では **CRON 式** を使ってジョブの実行スケジュールを定義します。
# MAGIC
# MAGIC ### CRON 式の構文
# MAGIC
# MAGIC ```
# MAGIC ┌───────────── 秒 (0-59)          ※ Quartz CRONの場合
# MAGIC │ ┌─────────── 分 (0-59)
# MAGIC │ │ ┌───────── 時 (0-23)
# MAGIC │ │ │ ┌─────── 日 (1-31)
# MAGIC │ │ │ │ ┌───── 月 (1-12)
# MAGIC │ │ │ │ │ ┌─── 曜日 (0-7, 0=日曜)
# MAGIC │ │ │ │ │ │
# MAGIC * * * * * *
# MAGIC ```
# MAGIC
# MAGIC ### よく使われる CRON 式の例
# MAGIC
# MAGIC | CRON 式 | 説明 |
# MAGIC |---|---|
# MAGIC | `0 0 * * *` | 毎日 0:00 に実行 |
# MAGIC | `0 6 * * 1-5` | 平日の 6:00 に実行 |
# MAGIC | `0 */2 * * *` | 2時間ごとに実行 |
# MAGIC | `0 0 1 * *` | 毎月1日の 0:00 に実行 |
# MAGIC | `0 9 * * 1` | 毎週月曜の 9:00 に実行 |
# MAGIC | `*/15 * * * *` | 15分ごとに実行 |

# COMMAND ----------

# CRON 式の解説
cron_examples = [
    ("0 0 * * *",     "毎日 0:00（深夜）"),
    ("0 6 * * 1-5",   "平日の 6:00（朝）"),
    ("0 */2 * * *",   "2時間ごと"),
    ("0 0 1 * *",     "毎月1日の 0:00"),
    ("0 9 * * 1",     "毎週月曜の 9:00"),
    ("*/15 * * * *",  "15分ごと"),
]

print("=== CRON 式の例 ===")
print(f"{'CRON 式':<20} {'説明'}")
print("-" * 50)
for cron, desc in cron_examples:
    print(f"{cron:<20} {desc}")

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: CRON 式を読み解く問題が出題されます。
# MAGIC > 特に「毎日」「平日のみ」「N時間ごと」のパターンを理解しておきましょう。
# MAGIC > Databricks では Quartz CRON（秒を含む6フィールド）が使われる場合もあります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. タスクの依存関係
# MAGIC
# MAGIC Lakeflow Jobs では、タスク間の依存関係を定義して実行順序を制御できます。
# MAGIC
# MAGIC ### 依存関係のパターン
# MAGIC
# MAGIC ```
# MAGIC 直列実行:          並列実行:           分岐と合流:
# MAGIC A → B → C          A ─→ C              A ─→ B ─┐
# MAGIC                    B ─→ C                      ├→ D
# MAGIC                                        A ─→ C ─┘
# MAGIC ```
# MAGIC
# MAGIC | パターン | 説明 | 用途 |
# MAGIC |---|---|---|
# MAGIC | 直列 | 前のタスクが完了してから次を実行 | ETL の Bronze → Silver → Gold |
# MAGIC | 並列 | 独立したタスクを同時実行 | 異なるソースからの取り込み |
# MAGIC | 分岐→合流 | 並列処理後に結果を統合 | 複数テーブルの変換後に集計 |

# COMMAND ----------

# タスク依存関係の可視化（テキストベース）
print("=== ETL パイプラインのタスク依存関係 ===")
print()
print("  [Bronze取込み]──→[Silver変換]──→[Gold集計]──→[品質チェック]")
print("       │                                          │")
print("       │           [ログ取込み]──→[ログ変換]──→[ダッシュボード更新]")
print("       │                                          │")
print("       └──────────────────────────────────────────→[通知送信]")
print()
print("  依存関係:")
print("  • Silver変換 は Bronze取込み の完了後に開始")
print("  • Gold集計 は Silver変換 の完了後に開始")
print("  • ログ取込み は Bronze取込み と並列実行可能")
print("  • 通知送信 は全タスクの完了後に実行")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 障害復旧とリトライ設定
# MAGIC
# MAGIC ジョブが失敗した場合の復旧方法を理解しましょう。
# MAGIC
# MAGIC ### リトライ設定
# MAGIC
# MAGIC | 設定 | 説明 |
# MAGIC |---|---|
# MAGIC | `max_retries` | 最大リトライ回数 |
# MAGIC | `min_retry_interval_millis` | リトライ間隔（最小） |
# MAGIC | `retry_on_timeout` | タイムアウト時にリトライするか |
# MAGIC
# MAGIC ### 障害発生時の動作
# MAGIC
# MAGIC | シナリオ | デフォルト動作 | 推奨設定 |
# MAGIC |---|---|---|
# MAGIC | タスク失敗 | 依存タスクもスキップ | リトライ設定を追加 |
# MAGIC | タイムアウト | ジョブを終了 | 適切なタイムアウト値を設定 |
# MAGIC | クラスター起動失敗 | ジョブを終了 | クラスタープールを使用 |
# MAGIC
# MAGIC ### 失敗したタスクの再実行
# MAGIC
# MAGIC ```
# MAGIC ジョブ実行結果:
# MAGIC  [Task A: 成功] → [Task B: 失敗] → [Task C: スキップ]
# MAGIC
# MAGIC 「失敗したタスクから再実行」を選択すると:
# MAGIC  [Task A: スキップ] → [Task B: 再実行] → [Task C: 実行]
# MAGIC ```
# MAGIC
# MAGIC > **試験ポイント**: Lakeflow Jobs では失敗したタスクからの再実行が可能です。
# MAGIC > 成功済みのタスクはスキップされるため、効率的に復旧できます。

# COMMAND ----------

# リトライ設定の JSON 例
import json

retry_config = {
    "task_key": "bronze_ingestion",
    "notebook_task": {
        "notebook_path": "/Workspace/Users/user/project/02_data_ingestion"
    },
    "max_retries": 3,
    "min_retry_interval_millis": 60000,
    "retry_on_timeout": True,
    "timeout_seconds": 3600,
}

print("=== タスクのリトライ設定例 ===")
print(json.dumps(retry_config, indent=2, ensure_ascii=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Databricks Asset Bundles（DABs）
# MAGIC
# MAGIC **Databricks Asset Bundles (DABs)** は、Databricks リソースを
# MAGIC **コードとして管理（Infrastructure as Code）** するツールです。
# MAGIC
# MAGIC ### DABs の仕組み
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────┐     deploy     ┌────────────────────┐
# MAGIC │  ローカル開発環境     │  ──────────→  │  Databricks        │
# MAGIC │                    │               │  ワークスペース      │
# MAGIC │  databricks.yml    │               │  • ジョブ           │
# MAGIC │  src/              │               │  • パイプライン      │
# MAGIC │  tests/            │               │  • ノートブック      │
# MAGIC └────────────────────┘               └────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### DABs のプロジェクト構造
# MAGIC
# MAGIC ```
# MAGIC my_project/
# MAGIC ├── databricks.yml          # メインの設定ファイル
# MAGIC ├── resources/
# MAGIC │   ├── my_job.yml          # ジョブの定義
# MAGIC │   └── my_pipeline.yml     # パイプラインの定義
# MAGIC ├── src/
# MAGIC │   ├── notebook1.py        # ソースコード
# MAGIC │   └── notebook2.sql
# MAGIC └── tests/
# MAGIC     └── test_transforms.py  # テストコード
# MAGIC ```
# MAGIC
# MAGIC ### databricks.yml の例
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: my_etl_pipeline
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     daily_etl:
# MAGIC       name: "Daily ETL Pipeline"
# MAGIC       schedule:
# MAGIC         quartz_cron_expression: "0 0 6 * * ?"
# MAGIC         timezone_id: "Asia/Tokyo"
# MAGIC       tasks:
# MAGIC         - task_key: bronze
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./src/bronze.py
# MAGIC         - task_key: silver
# MAGIC           depends_on:
# MAGIC             - task_key: bronze
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./src/silver.py
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     workspace:
# MAGIC       host: https://adb-xxxx.azuredatabricks.net
# MAGIC   prod:
# MAGIC     workspace:
# MAGIC       host: https://adb-yyyy.azuredatabricks.net
# MAGIC ```
# MAGIC
# MAGIC ### DABs の CLI コマンド
# MAGIC
# MAGIC | コマンド | 説明 |
# MAGIC |---|---|
# MAGIC | `databricks bundle init` | プロジェクトテンプレートの作成 |
# MAGIC | `databricks bundle validate` | 設定ファイルのバリデーション |
# MAGIC | `databricks bundle deploy` | リソースをワークスペースにデプロイ |
# MAGIC | `databricks bundle run` | ジョブやパイプラインを実行 |
# MAGIC | `databricks bundle destroy` | デプロイしたリソースを削除 |

# COMMAND ----------

# MAGIC %md
# MAGIC > **試験ポイント**: Databricks Asset Bundles（DABs）はリソースをコードとして管理し、
# MAGIC > CI/CD パイプラインに統合できます。
# MAGIC > `databricks.yml` がプロジェクトのメイン設定ファイルです。
# MAGIC > `targets` で開発/本番などの環境を切り替えてデプロイできます。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. サーバーレスコンピュート for Lakeflow Jobs
# MAGIC
# MAGIC Lakeflow Jobs のジョブをサーバーレスコンピュートで実行すると、
# MAGIC クラスターの管理が不要になり、起動時間も大幅に短縮されます。
# MAGIC
# MAGIC | 特徴 | Job クラスター | サーバーレス |
# MAGIC |---|---|---|
# MAGIC | 起動時間 | 数分 | 数秒〜数十秒 |
# MAGIC | クラスター管理 | 手動設定が必要 | 不要 |
# MAGIC | スケーリング | 設定が必要 | 自動 |
# MAGIC | コスト | インスタンス時間課金 | 実行時間のみ課金 |
# MAGIC | カスタマイズ | 自由 | 一部制限あり |
# MAGIC
# MAGIC > **試験ポイント**: サーバーレスコンピュートはクラスター管理の手間を省き、
# MAGIC > ジョブの起動時間を短縮します。ただし、カスタムライブラリの使用など一部制限があります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. ジョブの監視とアラート
# MAGIC
# MAGIC ### 監視方法
# MAGIC
# MAGIC | 方法 | 説明 |
# MAGIC |---|---|
# MAGIC | Lakeflow Jobs UI | ジョブの実行履歴、成功/失敗率を確認 |
# MAGIC | メールアラート | 成功/失敗/開始時にメール通知 |
# MAGIC | Webhook | 外部サービスに通知（Slack 等） |
# MAGIC | API | プログラムからジョブの状態を確認 |
# MAGIC
# MAGIC ### アラート設定の例
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "email_notifications": {
# MAGIC     "on_start": ["team@example.com"],
# MAGIC     "on_success": ["team@example.com"],
# MAGIC     "on_failure": ["oncall@example.com", "manager@example.com"]
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# ジョブの実行履歴を確認する例
print("=== ジョブの監視ポイント ===")
print()
monitoring_points = [
    ("実行時間の推移", "処理時間が徐々に増加していないか"),
    ("成功率", "失敗が頻繁に発生していないか"),
    ("データ量の推移", "処理するデータ量が想定通りか"),
    ("リソース使用率", "CPU やメモリが不足していないか"),
    ("SLA 遵守", "処理が期限内に完了しているか"),
]

for point, desc in monitoring_points:
    print(f"  • {point}")
    print(f"    → {desc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **Lakeflow Jobs の基本** — ジョブ、タスク、トリガーの概念（旧 Workflows）
# MAGIC 2. **CRON スケジューリング** — CRON 式の構文と代表的なパターン
# MAGIC 3. **タスク依存関係** — 直列・並列・分岐の実行パターン
# MAGIC 4. **障害復旧** — リトライ設定、失敗タスクからの再実行
# MAGIC 5. **Databricks Asset Bundles** — IaC によるリソース管理と CI/CD
# MAGIC 6. **サーバーレスコンピュート** — 管理不要な実行環境
# MAGIC 7. **監視とアラート** — メール通知、Webhook、監視ポイント
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - **次のノートブック `08_unity_catalog_governance.py`** で Unity Catalog とデータガバナンスを学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Engineer Associate):
# MAGIC > - **Productionizing Data Pipelines (18%)**: Lakeflow Jobs のジョブ管理、CRON スケジューリング、タスク依存関係、障害復旧、DABs、サーバーレスコンピュート

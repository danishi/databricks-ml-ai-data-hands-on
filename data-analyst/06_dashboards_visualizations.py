# Databricks notebook source
# MAGIC %md
# MAGIC # ダッシュボードと可視化の作成
# MAGIC
# MAGIC このノートブックでは、**AI/BI ダッシュボード**と**データの可視化**を学びます。
# MAGIC ダッシュボードは分析結果をステークホルダーに共有する重要なツールです。
# MAGIC
# MAGIC **ダッシュボードとは？**
# MAGIC > データをグラフや表で可視化し、一目でビジネスの状況を把握できる画面です。
# MAGIC > 「車のダッシュボード（計器盤）」のように、重要な指標を集約して表示します。
# MAGIC
# MAGIC ### このノートブックで体験する流れ
# MAGIC
# MAGIC ```
# MAGIC ① ノートブックでの可視化 → ② ダッシュボードの作成（UI操作）
# MAGIC                                         ↓
# MAGIC ④ アラートの設定 ← ③ パラメータ・共有・スケジュール
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - ノートブック上でのデータ可視化
# MAGIC - AI/BI ダッシュボードの構成と作成方法
# MAGIC - 効果的な可視化タイプの選び方
# MAGIC - パラメータの設定と使い方
# MAGIC - ダッシュボードの共有・埋め込み・スケジュール
# MAGIC - アラートの設定
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
# MAGIC ## 1. ノートブック上での可視化
# MAGIC
# MAGIC Databricks のノートブックでは、クエリ結果を直接グラフに変換できます。
# MAGIC
# MAGIC ### 可視化の作成方法（ノートブック）
# MAGIC
# MAGIC 1. SQL セルまたは `display()` でデータを表示
# MAGIC 2. 結果テーブルの上にある **「+」** → **「可視化」** をクリック
# MAGIC 3. グラフの種類、X軸、Y軸などを設定
# MAGIC 4. **「保存」** をクリック

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 商品カテゴリ別の売上データ（棒グラフに適したデータ）
# MAGIC SELECT
# MAGIC   p.category,
# MAGIC   COUNT(o.order_id) AS order_count,
# MAGIC   SUM(o.total_amount) AS total_revenue,
# MAGIC   AVG(o.total_amount) AS avg_order_value
# MAGIC FROM da_orders o
# MAGIC JOIN da_sample_products p ON o.product_name = p.product_name
# MAGIC WHERE o.status = 'completed'
# MAGIC GROUP BY p.category
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC > **試してみよう**: 上のクエリ結果のテーブル上部にある **「+」** → **「可視化」** をクリックして、
# MAGIC > **棒グラフ**を作成してみましょう。
# MAGIC > - X軸: `category`
# MAGIC > - Y軸: `total_revenue`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 月次売上トレンド（折れ線グラフに適したデータ）
# MAGIC SELECT
# MAGIC   DATE_TRUNC('month', order_date) AS order_month,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(total_amount) AS total_revenue
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY DATE_TRUNC('month', order_date)
# MAGIC ORDER BY order_month;

# COMMAND ----------

# MAGIC %md
# MAGIC > **試してみよう**: 上のクエリ結果から**折れ線グラフ**を作成してみましょう。
# MAGIC > - X軸: `order_month`
# MAGIC > - Y軸: `total_revenue`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 顧客セグメント別の構成比（円グラフに適したデータ）
# MAGIC SELECT
# MAGIC   COALESCE(segment, 'Unknown') AS segment,
# MAGIC   COUNT(*) AS customer_count
# MAGIC FROM da_customers
# MAGIC GROUP BY segment;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 顧客別の注文金額分布（散布図やヒストグラムに適したデータ）
# MAGIC SELECT
# MAGIC   c.name,
# MAGIC   c.segment,
# MAGIC   o.total_amount,
# MAGIC   o.quantity
# MAGIC FROM da_orders o
# MAGIC JOIN da_customers c ON o.customer_id = c.customer_id
# MAGIC WHERE o.status = 'completed';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 効果的な可視化タイプの選び方
# MAGIC
# MAGIC データの特性と伝えたいメッセージに応じて、適切なグラフタイプを選びましょう。
# MAGIC
# MAGIC | 目的 | 推奨グラフ | 例 |
# MAGIC |---|---|---|
# MAGIC | **カテゴリの比較** | 棒グラフ（Bar Chart） | カテゴリ別売上比較 |
# MAGIC | **時系列トレンド** | 折れ線グラフ（Line Chart） | 月次売上推移 |
# MAGIC | **構成比** | 円グラフ / ドーナツ（Pie Chart） | 売上シェア |
# MAGIC | **2変数の関係** | 散布図（Scatter Plot） | 価格と販売数の関係 |
# MAGIC | **分布** | ヒストグラム / 箱ひげ図 | 注文金額の分布 |
# MAGIC | **地理データ** | マップ（Map） | 地域別売上 |
# MAGIC | **KPI表示** | カウンター（Counter） | 売上合計、顧客数 |
# MAGIC | **詳細データ** | テーブル（Table） | 明細データ |
# MAGIC
# MAGIC > **認定試験のポイント**: データの種類や分析目的に応じた適切な可視化タイプの選択は
# MAGIC > 試験の重要な出題範囲です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. AI/BI ダッシュボード
# MAGIC
# MAGIC **AI/BI ダッシュボード**（旧 Lakeview ダッシュボード）は、
# MAGIC Databricks の新しいダッシュボード機能です。
# MAGIC
# MAGIC ### 特徴
# MAGIC
# MAGIC | 機能 | 説明 |
# MAGIC |---|---|
# MAGIC | **マルチタブレイアウト** | 複数のページ/タブでダッシュボードを整理 |
# MAGIC | **ウィジェット** | グラフ、テキスト、画像を自由に配置 |
# MAGIC | **AI支援** | 自然言語でグラフを作成・修正 |
# MAGIC | **パラメータ** | インタラクティブなフィルタ機能 |
# MAGIC | **自動レイアウト** | 画面サイズに応じたレスポンシブデザイン |
# MAGIC
# MAGIC ### ダッシュボードの作成手順
# MAGIC
# MAGIC 1. 左サイドバーの **「ダッシュボード」** をクリック
# MAGIC 2. **「ダッシュボードの作成」** をクリック
# MAGIC 3. **「データ」** タブでクエリを追加
# MAGIC 4. **「キャンバス」** タブでウィジェット（グラフ・テキスト）を配置
# MAGIC 5. レイアウトを調整して **「公開」**
# MAGIC
# MAGIC ### ウィジェットの種類
# MAGIC
# MAGIC | ウィジェット | 説明 |
# MAGIC |---|---|
# MAGIC | **可視化ウィジェット** | グラフ（棒、折れ線、円、散布図等） |
# MAGIC | **テキストウィジェット** | 説明文やタイトル |
# MAGIC | **画像ウィジェット** | ロゴや画像 |
# MAGIC | **フィルタウィジェット** | パラメータベースのフィルタ |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 実践: ダッシュボード用のデータを準備
# MAGIC
# MAGIC ダッシュボードに使えるデータセットを準備しましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ダッシュボード用: KPI サマリー
# MAGIC SELECT
# MAGIC   SUM(total_amount) AS total_revenue,
# MAGIC   COUNT(DISTINCT order_id) AS total_orders,
# MAGIC   COUNT(DISTINCT customer_id) AS active_customers,
# MAGIC   AVG(total_amount) AS avg_order_value
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ダッシュボード用: 日別売上トレンド
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(total_amount) AS daily_revenue
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY order_date
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ダッシュボード用: 商品別売上ランキング
# MAGIC SELECT
# MAGIC   product_name,
# MAGIC   SUM(total_amount) AS total_revenue,
# MAGIC   SUM(quantity) AS total_quantity,
# MAGIC   COUNT(*) AS order_count
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY product_name
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. パラメータの設定
# MAGIC
# MAGIC **パラメータ**を使うと、ダッシュボードやクエリにインタラクティブなフィルタを追加できます。
# MAGIC
# MAGIC ### パラメータの種類
# MAGIC
# MAGIC | 種類 | 説明 | 例 |
# MAGIC |---|---|---|
# MAGIC | **テキスト** | 自由入力 | 商品名の検索 |
# MAGIC | **数値** | 数値入力 | 金額の閾値 |
# MAGIC | **ドロップダウン** | 選択肢から選択 | カテゴリの選択 |
# MAGIC | **日付** | 日付選択 | 期間の指定 |
# MAGIC | **日付範囲** | 開始日〜終了日 | レポート期間 |
# MAGIC
# MAGIC ### ダッシュボードでのパラメータ設定手順
# MAGIC
# MAGIC 1. ダッシュボードの編集画面で **「データ」** タブを選択
# MAGIC 2. クエリ内で `{{parameter_name}}` 構文でパラメータを使用
# MAGIC 3. パラメータの型、デフォルト値を設定
# MAGIC 4. キャンバスに **フィルタウィジェット** を追加
# MAGIC
# MAGIC > **認定試験のポイント**: パラメータの定義方法、構成手順、テスト方法が出題されます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- パラメータ化されたクエリの例
# MAGIC -- ダッシュボードやSQLエディタで {{start_date}} がフィルタウィジェットになる
# MAGIC
# MAGIC -- ノートブック内ではウィジェットで代用
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   product_name,
# MAGIC   total_amount,
# MAGIC   status
# MAGIC FROM da_orders
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC   AND order_date <= '2024-12-31'
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. ダッシュボードの共有と権限
# MAGIC
# MAGIC ### 共有方法
# MAGIC
# MAGIC | 方法 | 説明 | 対象 |
# MAGIC |---|---|---|
# MAGIC | **ワークスペース内共有** | ユーザー/グループに権限を付与 | 社内ユーザー |
# MAGIC | **共有リンク** | URLリンクで共有 | リンクを知っているユーザー |
# MAGIC | **埋め込み** | iframe でアプリケーションに埋め込み | 外部アプリケーション |
# MAGIC | **PDF/画像エクスポート** | 静的なレポートとして出力 | メール添付等 |
# MAGIC
# MAGIC ### 権限レベル
# MAGIC
# MAGIC | 権限 | 説明 |
# MAGIC |---|---|
# MAGIC | **閲覧者（Viewer）** | ダッシュボードの閲覧のみ |
# MAGIC | **実行者（Runner）** | クエリの再実行が可能 |
# MAGIC | **編集者（Editor）** | ダッシュボードの編集が可能 |
# MAGIC | **オーナー（Owner）** | 権限管理を含む全操作が可能 |
# MAGIC
# MAGIC ### 共有リンクの設定手順
# MAGIC
# MAGIC 1. ダッシュボードの右上の **「共有」** ボタンをクリック
# MAGIC 2. **「共有リンクを取得」** を選択
# MAGIC 3. リンクのアクセスレベルを設定
# MAGIC 4. 必要に応じて **クレデンシャル** の埋め込みを設定
# MAGIC
# MAGIC ### ダッシュボードの埋め込み
# MAGIC
# MAGIC ダッシュボードを外部のWebアプリケーションに iframe として埋め込むことができます。
# MAGIC
# MAGIC ```html
# MAGIC <!-- 埋め込みの例 -->
# MAGIC <iframe src="https://workspace.databricks.com/embed/dashboards/..." width="100%" height="600"></iframe>
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ダッシュボードのスケジュール実行
# MAGIC
# MAGIC ダッシュボードのデータを定期的に自動更新するスケジュールを設定できます。
# MAGIC
# MAGIC ### スケジュールの設定手順
# MAGIC
# MAGIC 1. ダッシュボードの右上の **「スケジュール」** ボタンをクリック
# MAGIC 2. 更新頻度を設定（1時間ごと、毎日、毎週など）
# MAGIC 3. SQL ウェアハウスを選択
# MAGIC 4. **「サブスクライバー」** を追加（メール通知先）
# MAGIC
# MAGIC ### サブスクリプション
# MAGIC
# MAGIC | 設定 | 説明 |
# MAGIC |---|---|
# MAGIC | **更新頻度** | データの更新間隔（1時間〜1ヶ月） |
# MAGIC | **SQL ウェアハウス** | スケジュール実行に使うウェアハウス |
# MAGIC | **サブスクライバー** | ダッシュボード更新時にメール通知を受け取るユーザー |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. アラートの設定
# MAGIC
# MAGIC **アラート**は、クエリ結果が特定の条件を満たしたときに通知を送る機能です。
# MAGIC
# MAGIC ### アラートの設定手順
# MAGIC
# MAGIC 1. SQL エディタでクエリを作成・保存
# MAGIC 2. 左サイドバーの **「アラート」** → **「アラートの作成」**
# MAGIC 3. 監視するクエリを選択
# MAGIC 4. **条件を設定**（例: 売上が閾値を下回った場合）
# MAGIC 5. **通知先を設定**（メール、Slack、Webhook等）
# MAGIC 6. **評価スケジュール**を設定
# MAGIC
# MAGIC ### アラート条件の例
# MAGIC
# MAGIC | 条件 | 説明 | ユースケース |
# MAGIC |---|---|---|
# MAGIC | **値が閾値を超える** | `value > threshold` | 異常値の検出 |
# MAGIC | **値が閾値を下回る** | `value < threshold` | 在庫切れ警告 |
# MAGIC | **値が変化する** | 前回と値が異なる | データ更新の検知 |
# MAGIC
# MAGIC ### 通知先（Notification Destinations）
# MAGIC
# MAGIC | 通知先 | 説明 |
# MAGIC |---|---|
# MAGIC | **メール** | 指定アドレスにメール送信 |
# MAGIC | **Slack** | Slackチャンネルに通知 |
# MAGIC | **Webhook** | カスタムURLにHTTPリクエスト |
# MAGIC | **PagerDuty** | インシデント管理ツール連携 |
# MAGIC
# MAGIC > **認定試験のポイント**: アラートの設定手順、条件の種類、通知先の種類が出題されます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- アラート用クエリの例: 在庫が一定数以下の商品を検出
# MAGIC SELECT
# MAGIC   product_name,
# MAGIC   stock_quantity,
# MAGIC   CASE
# MAGIC     WHEN stock_quantity < 30 THEN 'CRITICAL'
# MAGIC     WHEN stock_quantity < 50 THEN 'WARNING'
# MAGIC     ELSE 'OK'
# MAGIC   END AS stock_status
# MAGIC FROM da_sample_products
# MAGIC WHERE stock_quantity < 50
# MAGIC ORDER BY stock_quantity;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. 実践: ダッシュボードの作成
# MAGIC
# MAGIC 以下の手順で、ハンズオンのデータを使ったダッシュボードを作成してみましょう。
# MAGIC
# MAGIC ### Step 1: ダッシュボードの作成
# MAGIC 1. 左サイドバーの **「ダッシュボード」** をクリック
# MAGIC 2. **「ダッシュボードの作成」** をクリック
# MAGIC 3. タイトルを入力（例: 「EC サイト売上ダッシュボード」）
# MAGIC
# MAGIC ### Step 2: データの追加
# MAGIC 1. **「データ」** タブをクリック
# MAGIC 2. 以下のクエリを追加:
# MAGIC    - KPIサマリー（売上合計、注文数）
# MAGIC    - 月次売上トレンド
# MAGIC    - 商品別売上
# MAGIC    - 顧客セグメント別分析
# MAGIC
# MAGIC ### Step 3: ウィジェットの配置
# MAGIC 1. **「キャンバス」** タブに切り替え
# MAGIC 2. カウンターウィジェットでKPI表示
# MAGIC 3. 折れ線グラフで売上トレンド
# MAGIC 4. 棒グラフで商品別売上
# MAGIC 5. 円グラフでセグメント別構成
# MAGIC
# MAGIC ### Step 4: 公開
# MAGIC 1. レイアウトを調整
# MAGIC 2. **「公開」** ボタンをクリック
# MAGIC 3. 共有設定を行う

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **ノートブック上での可視化** — display() からグラフを作成
# MAGIC 2. **効果的な可視化タイプの選び方** — 棒グラフ、折れ線、円グラフ、散布図等
# MAGIC 3. **AI/BI ダッシュボード** — マルチタブ、ウィジェット、AI支援
# MAGIC 4. **パラメータ** — インタラクティブフィルタの設定
# MAGIC 5. **共有と権限** — ワークスペース共有、リンク共有、埋め込み
# MAGIC 6. **スケジュール実行** — 定期的なデータ更新、サブスクリプション
# MAGIC 7. **アラート** — 条件ベースの自動通知
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - 実際にダッシュボードを作成して公開してみましょう
# MAGIC - **次のノートブック `07_genie_spaces.py`** で AI/BI Genie Spaces を学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Analyst Associate):
# MAGIC > - **Creating Dashboards and Visualizations (16%)**: ダッシュボード作成、可視化、パラメータ、共有、アラート

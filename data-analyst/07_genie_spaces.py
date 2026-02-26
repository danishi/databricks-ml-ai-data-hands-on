# Databricks notebook source
# MAGIC %md
# MAGIC # AI/BI Genie Spaces の開発
# MAGIC
# MAGIC このノートブックでは、**AI/BI Genie Spaces** の概念と活用方法を学びます。
# MAGIC Genie は Databricks の会話型AIアナリティクスツールで、
# MAGIC 自然言語でデータに質問できるインターフェースを提供します。
# MAGIC
# MAGIC **Genie Space とは？**
# MAGIC > 「データに対して日本語や英語で質問すると、SQLを自動生成して回答してくれる」ツールです。
# MAGIC > Excel でピボットテーブルを作る代わりに、「先月の売上トップ5を教えて」と聞くだけで
# MAGIC > 結果が得られるイメージです。
# MAGIC
# MAGIC ### このノートブックで体験する流れ
# MAGIC
# MAGIC ```
# MAGIC ① Genie の目的と機能 → ② Genie Space の作成 → ③ 権限と配布
# MAGIC                                                       ↓
# MAGIC                     ⑤ ベンチマーク検証 ← ④ 最適化とメンテナンス
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - Genie Space の目的、主要機能、コンポーネント
# MAGIC - Genie Space の作成手順（サンプル質問、ドメイン指示、Trusted Assets）
# MAGIC - 権限設定と配布方法
# MAGIC - Genie Space の最適化とメンテナンス
# MAGIC
# MAGIC ## 前提条件
# MAGIC - `02_managing_data.py` を実行済みであること（サンプルデータが必要）
# MAGIC - Databricks Runtime（例: 16.x）のクラスターを使用してください
# MAGIC - Genie Space の作成には SQL ウェアハウスが必要です
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
# MAGIC ## 1. AI/BI Genie とは？
# MAGIC
# MAGIC **AI/BI Genie** は、自然言語でデータに質問し、回答を得るための会話型インターフェースです。
# MAGIC
# MAGIC ### Genie の仕組み
# MAGIC
# MAGIC ```
# MAGIC ユーザーの質問（自然言語）
# MAGIC      ↓
# MAGIC [AI/BI Genie エンジン]
# MAGIC   1. 質問を理解
# MAGIC   2. 関連テーブルを特定
# MAGIC   3. SQL を自動生成
# MAGIC   4. SQL ウェアハウスで実行
# MAGIC   5. 結果を自然言語 + 表/グラフで返す
# MAGIC      ↓
# MAGIC 回答（テーブル、グラフ、説明文）
# MAGIC ```
# MAGIC
# MAGIC ### 主要コンポーネント
# MAGIC
# MAGIC | コンポーネント | 説明 |
# MAGIC |---|---|
# MAGIC | **Genie Space** | 特定のドメイン/データセット向けの質問応答環境 |
# MAGIC | **サンプル質問** | ユーザーの参考となる質問例 |
# MAGIC | **ドメイン指示** | データの文脈や用語を定義するガイド |
# MAGIC | **Trusted Assets** | 検証済みのSQL クエリ（信頼できる回答パターン） |
# MAGIC | **データセット** | Unity Catalog のテーブル / ビュー |
# MAGIC | **SQL ウェアハウス** | クエリ実行用のコンピューティング |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Genie Space の作成
# MAGIC
# MAGIC ### 作成手順
# MAGIC
# MAGIC 1. 左サイドバーの **「Genie」** をクリック
# MAGIC 2. **「Genie Space を新規作成」** をクリック
# MAGIC 3. 以下を設定:
# MAGIC    - **タイトル**: Genie Space の名前（例: 「EC サイト売上分析」）
# MAGIC    - **SQL ウェアハウス**: 使用するウェアハウスを選択
# MAGIC    - **テーブル**: Unity Catalog からテーブルを選択
# MAGIC
# MAGIC ### 実践: Genie Space 用のテーブルを準備
# MAGIC
# MAGIC Genie Space で使用するテーブルには、**カラムのコメント**と**テーブルのコメント**を
# MAGIC 充実させることが重要です。Genie はこれらの情報を使ってデータを理解します。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Genie Space 用にカラムコメントを充実させる
# MAGIC ALTER TABLE da_orders ALTER COLUMN order_id COMMENT '注文を一意に識別するID';
# MAGIC ALTER TABLE da_orders ALTER COLUMN customer_id COMMENT '注文した顧客のID（da_customersテーブルと結合可能）';
# MAGIC ALTER TABLE da_orders ALTER COLUMN order_date COMMENT '注文日（YYYY-MM-DD形式）';
# MAGIC ALTER TABLE da_orders ALTER COLUMN product_name COMMENT '注文された商品の名前';
# MAGIC ALTER TABLE da_orders ALTER COLUMN quantity COMMENT '注文数量';
# MAGIC ALTER TABLE da_orders ALTER COLUMN unit_price COMMENT '商品の単価（日本円）';
# MAGIC ALTER TABLE da_orders ALTER COLUMN total_amount COMMENT '注文の合計金額（日本円）= quantity × unit_price';
# MAGIC ALTER TABLE da_orders ALTER COLUMN status COMMENT '注文ステータス（completed: 完了, pending: 処理中, cancelled: キャンセル）';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- カラムコメントが設定されたことを確認
# MAGIC DESCRIBE TABLE da_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC > **認定試験のポイント**: Genie Space を作成する際に Unity Catalog のテーブル/ビューの
# MAGIC > コメントやタグを充実させることで、Genie の回答精度が向上します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. サンプル質問の設定
# MAGIC
# MAGIC **サンプル質問**は、ユーザーが Genie Space を使い始める際の参考となる質問例です。
# MAGIC 良いサンプル質問を設定することで、ユーザーが適切な質問の仕方を学べます。
# MAGIC
# MAGIC ### サンプル質問の例（EC サイト分析）
# MAGIC
# MAGIC | カテゴリ | サンプル質問 |
# MAGIC |---|---|
# MAGIC | **売上分析** | 「先月の売上合計はいくらですか？」 |
# MAGIC | **商品分析** | 「最も売れている商品トップ5を教えてください」 |
# MAGIC | **顧客分析** | 「Premium セグメントの顧客は何人いますか？」 |
# MAGIC | **トレンド** | 「月別の売上推移をグラフで表示してください」 |
# MAGIC | **比較** | 「東京と大阪の顧客数を比較してください」 |
# MAGIC
# MAGIC ### サンプル質問設定のベストプラクティス
# MAGIC
# MAGIC - ユーザーがよく聞きそうな質問を5〜10個設定
# MAGIC - 異なる分析パターン（集計、比較、トレンド）をカバー
# MAGIC - 具体的な用語やカラム名を含めて、Genie が理解しやすくする

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ドメイン固有の指示（Domain-Specific Instructions）
# MAGIC
# MAGIC **ドメイン指示**は、データの文脈やビジネスルールを Genie に教えるための設定です。
# MAGIC
# MAGIC ### ドメイン指示の例
# MAGIC
# MAGIC ```
# MAGIC このデータセットはECサイトの売上データです。
# MAGIC
# MAGIC ビジネスルール:
# MAGIC - 「売上」は status が 'completed' の注文の total_amount の合計を指します
# MAGIC - 「アクティブ顧客」は過去90日以内に注文した顧客です
# MAGIC - 金額は日本円（JPY）で表示してください
# MAGIC - 日付は YYYY-MM-DD 形式を使用してください
# MAGIC
# MAGIC 用語の定義:
# MAGIC - Premium顧客: segment が 'Premium' の顧客
# MAGIC - Standard顧客: segment が 'Standard' の顧客
# MAGIC - AOV（Average Order Value）: 注文あたりの平均金額
# MAGIC ```
# MAGIC
# MAGIC > **認定試験のポイント**: ドメイン指示で何を定義すべきか（ビジネスルール、用語定義、
# MAGIC > フォーマット規則等）を理解しておきましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Trusted Assets（信頼済みアセット）
# MAGIC
# MAGIC **Trusted Assets** は、正確性が検証されたSQL クエリです。
# MAGIC Genie はユーザーの質問に回答する際、まず Trusted Assets の中から
# MAGIC 適切なクエリを探し、あればそれを使って回答します。
# MAGIC
# MAGIC ### Trusted Assets の設定手順
# MAGIC
# MAGIC 1. Genie Space の設定画面で **「Trusted Assets」** タブを選択
# MAGIC 2. **「アセットの追加」** をクリック
# MAGIC 3. SQL クエリを入力
# MAGIC 4. クエリの説明（どんな質問に対する回答か）を記述
# MAGIC 5. テスト実行して結果を確認
# MAGIC 6. **「信頼済みとして保存」**
# MAGIC
# MAGIC ### Trusted Assets の例

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trusted Asset 例1: 月次売上サマリー
# MAGIC -- 質問: 「月別の売上を教えて」「月次売上はどうなっていますか？」
# MAGIC SELECT
# MAGIC   DATE_TRUNC('month', order_date) AS month,
# MAGIC   COUNT(DISTINCT order_id) AS order_count,
# MAGIC   SUM(total_amount) AS total_revenue,
# MAGIC   COUNT(DISTINCT customer_id) AS active_customers,
# MAGIC   ROUND(AVG(total_amount), 0) AS avg_order_value
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY DATE_TRUNC('month', order_date)
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Trusted Asset 例2: 顧客ランキング
# MAGIC -- 質問: 「優良顧客は誰ですか？」「最も注文金額が多い顧客は？」
# MAGIC SELECT
# MAGIC   c.name AS customer_name,
# MAGIC   c.segment,
# MAGIC   c.city,
# MAGIC   COUNT(o.order_id) AS order_count,
# MAGIC   SUM(o.total_amount) AS total_spent,
# MAGIC   ROUND(AVG(o.total_amount), 0) AS avg_order_value
# MAGIC FROM da_customers c
# MAGIC JOIN da_orders o ON c.customer_id = o.customer_id
# MAGIC WHERE o.status = 'completed'
# MAGIC GROUP BY c.name, c.segment, c.city
# MAGIC ORDER BY total_spent DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC > **試してみよう**: 他にどんな Trusted Asset が必要か考えてみましょう。
# MAGIC > 例: 「商品別在庫状況」「地域別顧客分布」「キャンセル率の推移」

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 権限の設定と配布
# MAGIC
# MAGIC ### Genie Space の権限
# MAGIC
# MAGIC | 権限 | 説明 |
# MAGIC |---|---|
# MAGIC | **閲覧者（Viewer）** | Genie Space で質問できる |
# MAGIC | **編集者（Editor）** | 設定の変更、Trusted Assets の管理 |
# MAGIC | **オーナー（Owner）** | 権限管理を含む全操作 |
# MAGIC
# MAGIC ### 配布方法
# MAGIC
# MAGIC | 方法 | 説明 |
# MAGIC |---|---|
# MAGIC | **ワークスペース内共有** | ユーザー/グループに直接権限を付与 |
# MAGIC | **埋め込みリンク** | iframe でアプリケーションに埋め込み |
# MAGIC | **外部連携** | Slack 等の外部ツールとの統合 |
# MAGIC
# MAGIC ### 権限設定のベストプラクティス
# MAGIC
# MAGIC - ビジネスユーザーには **閲覧者** 権限を付与
# MAGIC - データチームのメンバーには **編集者** 権限を付与
# MAGIC - Genie Space の管理者には **オーナー** 権限を付与
# MAGIC - **最小権限の原則**を適用（必要最小限の権限のみ付与）

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Genie Space の最適化とメンテナンス
# MAGIC
# MAGIC Genie Space は継続的な改善が必要です。
# MAGIC
# MAGIC ### 最適化の方法
# MAGIC
# MAGIC | 方法 | 説明 |
# MAGIC |---|---|
# MAGIC | **ユーザー質問の追跡** | よくある質問をモニタリング |
# MAGIC | **回答精度の確認** | 正確に回答できているか検証 |
# MAGIC | **フィードバック反映** | ユーザーからのフィードバックを指示に反映 |
# MAGIC | **指示の更新** | ビジネスルールの変更に合わせて更新 |
# MAGIC | **Trusted Assets の追加** | よくある質問に対するアセットを追加 |
# MAGIC | **メタデータの更新** | Unity Catalog のコメント・タグを最新に保つ |
# MAGIC
# MAGIC ### ベンチマーク検証
# MAGIC
# MAGIC Genie Space の回答精度を定量的に評価するために、
# MAGIC ベンチマーク質問とその正解を用意して定期的にテストします。
# MAGIC
# MAGIC | テスト項目 | 説明 |
# MAGIC |---|---|
# MAGIC | **正確性** | 回答の数値が正しいか |
# MAGIC | **完全性** | 質問に対して十分な情報を返しているか |
# MAGIC | **応答速度** | 許容可能な時間内に回答しているか |
# MAGIC | **適切性** | 質問の意図を正しく理解しているか |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ベンチマーク用: Genie の回答を検証するための「正解データ」
# MAGIC -- 質問: 「2024年1月の完了注文数と売上合計は？」
# MAGIC -- 期待する回答:
# MAGIC SELECT
# MAGIC   '2024年1月の実績' AS benchmark_question,
# MAGIC   COUNT(*) AS expected_order_count,
# MAGIC   SUM(total_amount) AS expected_revenue
# MAGIC FROM da_orders
# MAGIC WHERE status = 'completed'
# MAGIC   AND order_date >= '2024-01-01'
# MAGIC   AND order_date < '2024-02-01';

# COMMAND ----------

# MAGIC %md
# MAGIC > **認定試験のポイント**: Genie Space の作成・設定・最適化のライフサイクル全体が出題範囲です。
# MAGIC > 特に以下が重要:
# MAGIC > - サンプル質問とドメイン指示の設定方法
# MAGIC > - Trusted Assets の目的と設定
# MAGIC > - ユーザーフィードバックに基づく最適化
# MAGIC > - ベンチマークによる精度検証

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **AI/BI Genie の概要** — 自然言語によるデータ質問応答
# MAGIC 2. **Genie Space の作成** — テーブル選択、ウェアハウス設定
# MAGIC 3. **サンプル質問** — ユーザー向けの質問テンプレート
# MAGIC 4. **ドメイン指示** — ビジネスルールと用語の定義
# MAGIC 5. **Trusted Assets** — 検証済みSQLクエリの登録
# MAGIC 6. **権限と配布** — アクセス制御と共有方法
# MAGIC 7. **最適化とメンテナンス** — フィードバック反映とベンチマーク検証
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - 実際に Genie Space を作成して、自然言語で質問してみましょう
# MAGIC - **次のノートブック `08_data_modeling.py`** でデータモデリングを学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (Data Analyst Associate):
# MAGIC > - **Developing AI/BI Genie Spaces (12%)**: Genie Space の作成、サンプル質問、Trusted Assets、最適化

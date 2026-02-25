# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Vector Search による本格的なRAG
# MAGIC
# MAGIC このノートブックでは **Databricks Vector Search** を使った本格的なRAGシステムを構築します。
# MAGIC
# MAGIC 前回（`02_rag_chat.py`）ではメモリ上でベクトル検索を行いましたが、
# MAGIC 本番環境では **Vector Search Index** を使うことで、大量のドキュメントに対してスケーラブルに検索できます。
# MAGIC
# MAGIC > **前回との違い**
# MAGIC > ```
# MAGIC > 02（前回）:  ドキュメント → numpy配列（メモリ上） → コサイン類似度で検索
# MAGIC >              → 数百件が限界、アプリ再起動でデータ消失
# MAGIC >
# MAGIC > 03（今回）:  ドキュメント → Delta Table → Vector Search Index
# MAGIC >              → 数百万件もOK、永続化、自動同期
# MAGIC > ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - ドキュメントのチャンク分割戦略
# MAGIC - Delta Table へのドキュメント保存
# MAGIC - Databricks Vector Search Index の作成
# MAGIC - Vector Search を使ったRAGパイプライン
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）
# MAGIC - Unity Catalog が有効なワークスペース
# MAGIC - Vector Search エンドポイントが利用可能であること
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %pip install -q openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. セットアップ

# COMMAND ----------

import os
from openai import OpenAI
from databricks.sdk import WorkspaceClient

token = os.environ.get("DATABRICKS_TOKEN", dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
host = os.environ.get("DATABRICKS_HOST", f"https://{spark.conf.get('spark.databricks.workspaceUrl')}")

client = OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")
w = WorkspaceClient()

LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"
EMBEDDING_MODEL = "databricks-gte-large-en"

print("セットアップ完了")

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "カタログ名")
dbutils.widgets.text("schema", "default", "スキーマ名")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

source_table = f"{catalog}.{schema}.rag_documents"
index_name = f"{catalog}.{schema}.rag_documents_index"
vs_endpoint_name = "rag_hands_on_endpoint"

print(f"ソーステーブル: {source_table}")
print(f"インデックス名: {index_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ドキュメントのチャンク分割
# MAGIC
# MAGIC 長いドキュメントをそのままEmbeddingすると、検索精度が下がります。
# MAGIC **チャンク分割**により、ドキュメントを適切なサイズに分割します。
# MAGIC
# MAGIC | 戦略 | 説明 | 適した場面 |
# MAGIC |---|---|---|
# MAGIC | 固定長分割 | 一定の文字数で分割 | シンプル、汎用的 |
# MAGIC | 段落・文分割 | 段落や文の境界で分割 | 文脈を保ちやすい |
# MAGIC | オーバーラップ分割 | 隣接チャンクを一部重複させる | 境界の情報損失を防ぐ |
# MAGIC
# MAGIC > **チャンクサイズの目安**: 200〜500トークン程度が一般的です。
# MAGIC > 小さすぎると文脈が失われ、大きすぎると検索精度が下がります。
# MAGIC
# MAGIC > **初心者の方へ: なぜチャンク分割するの？**
# MAGIC >
# MAGIC > 100ページの本から「パスワードのルール」を探す場面を想像してください。
# MAGIC > - 本全体を1つのベクトルにすると、「この本はITマニュアルっぽい」くらいの大雑把な検索
# MAGIC > - 1段落ごとにベクトルにすると、「この段落がまさにパスワードの話」とピンポイントで発見
# MAGIC >
# MAGIC > **オーバーラップ**は、チャンクの境界で情報が切れるのを防ぐ工夫です。

# COMMAND ----------

def chunk_text(text: str, chunk_size: int = 300, overlap: int = 50) -> list[str]:
    """テキストをオーバーラップ付きで分割する"""
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        if chunk.strip():
            chunks.append(chunk.strip())
        start = end - overlap
    return chunks

# サンプルドキュメント
documents = [
    {
        "doc_id": "policy_001",
        "title": "リモートワーク制度について",
        "content": (
            "テックコーポレーションでは週3日までリモートワークが可能です。"
            "リモートワークを利用する場合は、前日までにチームリーダーへ申請してください。"
            "コアタイムは10:00〜15:00です。リモートワーク用のVPN接続手順は社内ポータルの「IT設定ガイド」を参照してください。"
            "リモートワーク中の業務報告は日報システムで行います。チーム会議はオンラインで参加可能です。"
            "自宅以外（カフェ等）での作業は情報セキュリティ上、原則禁止です。"
        ),
    },
    {
        "doc_id": "policy_002",
        "title": "福利厚生について",
        "content": (
            "テックコーポレーションの福利厚生には以下が含まれます。"
            "健康診断は年1回（35歳以上は人間ドック）が会社負担で受けられます。"
            "スポーツジム利用補助として月額3,000円が支給されます。"
            "社員食堂は昼食1食300円で利用可能です。"
            "育児休業は最大2年間取得可能で、復帰後は時短勤務制度も利用できます。"
            "結婚祝金30,000円、出産祝金50,000円が支給されます。"
            "介護休業は通算93日まで取得可能です。介護のための時短勤務も可能です。"
        ),
    },
    {
        "doc_id": "policy_003",
        "title": "セキュリティポリシー",
        "content": (
            "社外へのデータ持ち出しは原則禁止です。必要な場合はISMS管理者の承認を得てください。"
            "パスワードは12文字以上で、英数字記号を含む必要があります。90日ごとに変更してください。"
            "不審なメールを受信した場合はセキュリティチーム（security@techcorp.example.com）に転送してください。"
            "個人所有デバイスでの業務利用（BYOD）は申請制です。MDMアプリのインストールが必須です。"
            "社内Wi-Fiは WPA3-Enterprise で暗号化されています。ゲスト用Wi-Fiは社内ネットワークとは分離されています。"
        ),
    },
]

# チャンク分割を実行
chunks = []
for doc in documents:
    doc_chunks = chunk_text(doc["content"], chunk_size=200, overlap=30)
    for i, chunk in enumerate(doc_chunks):
        chunks.append({
            "chunk_id": f"{doc['doc_id']}_chunk_{i}",
            "doc_id": doc["doc_id"],
            "title": doc["title"],
            "content": chunk,
        })

print(f"元ドキュメント数: {len(documents)}")
print(f"チャンク数: {len(chunks)}")
for c in chunks:
    print(f"  [{c['chunk_id']}] {c['content'][:60]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delta Table にチャンクを保存
# MAGIC
# MAGIC チャンク分割したドキュメントを **Delta Table** に保存します。
# MAGIC Vector Search は Delta Table をソースとしてインデックスを構築します。

# COMMAND ----------

import pandas as pd
from pyspark.sql import functions as F

chunks_sdf = spark.createDataFrame(pd.DataFrame(chunks))

# Delta Tableとして保存
chunks_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(source_table)

# Change Data Feed を有効化（Vector Searchの同期に必要）
spark.sql(f"ALTER TABLE {source_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

print(f"Delta Table '{source_table}' に {chunks_sdf.count()} チャンクを保存しました")
display(spark.table(source_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Vector Search エンドポイントとインデックスの作成
# MAGIC
# MAGIC **Vector Search エンドポイント**: ベクトル検索を実行するサーバー
# MAGIC **Vector Search インデックス**: ドキュメントのベクトルを格納するインデックス
# MAGIC
# MAGIC > Delta Sync Index を使うと、Delta Table の変更が自動的にインデックスに反映されます。

# COMMAND ----------

from databricks.sdk.service.catalog import VectorSearchIndex

# Vector Searchエンドポイントを作成（既存なら再利用）
try:
    w.vector_search_endpoints.create_and_wait(name=vs_endpoint_name, endpoint_type="STANDARD")
    print(f"Vector Search エンドポイント '{vs_endpoint_name}' を作成しました")
except Exception as e:
    print(f"エンドポイント '{vs_endpoint_name}' は既に存在します（再利用します）")

# COMMAND ----------

# Delta Sync Index を作成
try:
    w.vector_search_indexes.create_index(
        name=index_name,
        endpoint_name=vs_endpoint_name,
        primary_key="chunk_id",
        index_type="DELTA_SYNC",
        delta_sync_index_spec={
            "source_table": source_table,
            "pipeline_type": "TRIGGERED",
            "embedding_source_columns": [
                {"name": "content", "embedding_model_endpoint_name": EMBEDDING_MODEL},
            ],
        },
    )
    print(f"Vector Search Index '{index_name}' を作成しました")
    print("インデックスの構築には数分かかる場合があります...")
except Exception as e:
    print(f"インデックスの作成をスキップしました: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Vector Search を使った検索

# COMMAND ----------

import time

# インデックスの準備完了を待つ
print("インデックスの準備を待っています...")
for i in range(30):
    try:
        idx = w.vector_search_indexes.get_index(index_name)
        if idx.status and idx.status.ready:
            print("インデックスが準備完了しました！")
            break
    except Exception:
        pass
    time.sleep(10)
    print(f"  待機中... ({(i+1)*10}秒)")

# COMMAND ----------

# Vector Searchで検索
def vector_search(query: str, num_results: int = 3) -> list[dict]:
    """Vector Search Indexを使ってドキュメントを検索"""
    results = w.vector_search_indexes.query_index(
        index_name=index_name,
        columns=["chunk_id", "title", "content"],
        query_text=query,
        num_results=num_results,
    )
    return [
        {
            "chunk_id": row["chunk_id"],
            "title": row["title"],
            "content": row["content"],
            "score": row.get("score", 0),
        }
        for row in results.result.data_array_to_json()
    ]

# 検索テスト
query = "在宅勤務の申請方法は？"
search_results = vector_search(query)
print(f"質問: {query}\n")
for r in search_results:
    print(f"  [{r['title']}] (score: {r.get('score', 'N/A')})")
    print(f"  {r['content'][:80]}...\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Vector Search RAG パイプライン
# MAGIC
# MAGIC Vector Search による検索結果をコンテキストとして LLM に渡します。

# COMMAND ----------

def rag_with_vector_search(query: str) -> dict:
    """Vector Search を使ったRAGパイプライン"""
    # ① Vector Searchで検索
    results = vector_search(query, num_results=3)

    # ② コンテキストを構築
    context = "\n\n".join([
        f"【{r['title']}】\n{r['content']}" for r in results
    ])

    # ③ LLMで回答を生成
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "あなたは社内FAQアシスタントです。参考情報のみに基づいて回答してください。"
                    "参考情報にない場合は「情報が見つかりませんでした」と回答してください。"
                ),
            },
            {"role": "user", "content": f"## 参考情報\n\n{context}\n\n## 質問\n\n{query}"},
        ],
        max_tokens=512,
        temperature=0.0,
    )

    return {
        "answer": response.choices[0].message.content,
        "sources": [{"title": r["title"], "chunk_id": r["chunk_id"]} for r in results],
    }

# テスト
result = rag_with_vector_search("パスワードのルールを教えてください")
print(f"回答: {result['answer']}\n")
print("参照元:")
for s in result["sources"]:
    print(f"  - {s['title']} ({s['chunk_id']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC | 項目 | 02（前回） | 03（今回） |
# MAGIC |---|---|---|
# MAGIC | ベクトル保存 | メモリ上 | **Delta Table + Vector Search Index** |
# MAGIC | 検索方法 | numpy コサイン類似度 | **Databricks Vector Search** |
# MAGIC | チャンク分割 | なし | **オーバーラップ分割** |
# MAGIC | スケーラビリティ | 小規模のみ | **大規模データ対応** |
# MAGIC
# MAGIC 1. **チャンク分割** — ドキュメントを適切なサイズに分割
# MAGIC 2. **Delta Table** — ドキュメントとメタデータの永続的な保存
# MAGIC 3. **Vector Search** — スケーラブルなベクトル検索
# MAGIC 4. **RAGパイプライン** — 検索→コンテキスト→生成の本格的な構成
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - `04_agents_tool_use.py` でマルチステージ推論とエージェントを学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (GenAI Engineer Associate):
# MAGIC > - **Data Preparation for RAG**: チャンク分割戦略（固定長、オーバーラップ、セマンティック）
# MAGIC > - **Data Preparation for RAG**: Delta Table、Vector Search Index（DELTA_SYNC）、Embedding モデル
# MAGIC > - **Databricks Tools**: Vector Search エンドポイントの作成と管理

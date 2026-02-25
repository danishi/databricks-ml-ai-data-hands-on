# Databricks notebook source
# MAGIC %md
# MAGIC # RAG（検索拡張生成）で社内ドキュメントQAを作る
# MAGIC
# MAGIC このノートブックでは、**RAG（Retrieval-Augmented Generation: 検索拡張生成）** の仕組みを
# MAGIC ステップバイステップで学びます。
# MAGIC
# MAGIC ## RAG とは？
# MAGIC
# MAGIC > **RAG** は、LLM（大規模言語モデル）が**知らない情報**を答えられるようにする技術です。
# MAGIC >
# MAGIC > LLMは学習データに含まれない情報（社内ドキュメント、最新ニュースなど）を知りません。
# MAGIC > RAGでは、質問に関連するドキュメントを**まず検索**し、その内容をLLMに渡して回答を生成します。
# MAGIC
# MAGIC > **初心者の方へ: RAG を身近な例で理解する**
# MAGIC >
# MAGIC > 想像してください: あなたが「会社の有給休暇のルール」を友人に聞かれたとします。
# MAGIC > - **LLMだけ**: 記憶をたどって一般的なことを答える（間違うかも）
# MAGIC > - **RAG**: まず社内規則集を開いて該当ページを読み、それを元に正確に答える
# MAGIC >
# MAGIC > RAG = **カンペを見てから回答する賢いアシスタント** です。
# MAGIC
# MAGIC ### RAG の流れ
# MAGIC
# MAGIC ```
# MAGIC ユーザーの質問
# MAGIC     ↓
# MAGIC ① 質問をベクトルに変換（Embedding）
# MAGIC     ↓
# MAGIC ② ベクトル類似度でドキュメントを検索（Retrieval）
# MAGIC     ↓
# MAGIC ③ 検索結果 + 質問 を LLM に渡す（Generation）
# MAGIC     ↓
# MAGIC 回答を生成
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - RAGの基本概念と仕組み
# MAGIC - ドキュメントのベクトル化（Embedding）
# MAGIC - ベクトル類似度による検索（Retrieval）
# MAGIC - 検索結果を使ったLLMの回答生成（Generation）
# MAGIC - RAGありとRAGなしの比較
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）のクラスターを使用してください
# MAGIC - ワークスペースで Foundation Model APIs が有効になっていること
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. セットアップ
# MAGIC
# MAGIC 前回のノートブック（`01_foundation_model_apis.py`）と同じく、
# MAGIC OpenAI互換クライアントでDatabricks Foundation Model APIs にアクセスします。

# COMMAND ----------

# MAGIC %pip install -q openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import numpy as np
from openai import OpenAI

# Databricks のトークンとエンドポイントを自動取得
token = os.environ.get("DATABRICKS_TOKEN", dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
host = os.environ.get("DATABRICKS_HOST", f"https://{spark.conf.get('spark.databricks.workspaceUrl')}")

client = OpenAI(
    api_key=token,
    base_url=f"{host}/serving-endpoints",
)

LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"
EMBEDDING_MODEL = "databricks-gte-large-en"

print("セットアップ完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. サンプルドキュメントの準備
# MAGIC
# MAGIC RAGの「検索元」となるドキュメントを用意します。
# MAGIC
# MAGIC 今回は架空の会社「テックコーポレーション」の**社内FAQ**を使います。
# MAGIC 実際の業務では、社内Wiki、マニュアル、規程集などを使うことを想像してください。
# MAGIC
# MAGIC > **ポイント**: LLMはこれらの社内情報を知りません。
# MAGIC > RAGを使うことで、LLMがこれらの情報に基づいた回答を生成できるようになります。

# COMMAND ----------

# 社内FAQドキュメント（架空のデータ）
documents = [
    {
        "id": 1,
        "title": "リモートワーク制度について",
        "content": (
            "テックコーポレーションでは週3日までリモートワークが可能です。"
            "リモートワークを利用する場合は、前日までにチームリーダーへ申請してください。"
            "コアタイムは10:00〜15:00です。"
            "リモートワーク用のVPN接続手順は社内ポータルの「IT設定ガイド」を参照してください。"
        ),
    },
    {
        "id": 2,
        "title": "有給休暇の取得について",
        "content": (
            "有給休暇は入社6ヶ月後に10日付与されます。"
            "申請は休暇管理システム（HRポータル）から行ってください。"
            "3日以上の連続休暇は2週間前までに申請が必要です。"
            "半日単位での取得も可能です。"
            "年末年始（12/29〜1/3）は会社指定休日のため有給消化は不要です。"
        ),
    },
    {
        "id": 3,
        "title": "経費精算の方法",
        "content": (
            "経費精算は経費精算システム（EXシステム）から申請してください。"
            "領収書の原本またはスキャンデータの添付が必須です。"
            "交通費は月末締め翌月15日払いです。"
            "タクシー利用は原則として事前承認が必要です。ただし終電後の帰宅は事後申請可能です。"
            "出張時の宿泊費上限は1泊12,000円です。"
        ),
    },
    {
        "id": 4,
        "title": "社内勉強会・研修制度",
        "content": (
            "テックコーポレーションでは毎週金曜日16:00〜17:00に社内Tech勉強会を開催しています。"
            "外部カンファレンスへの参加費は年間10万円まで会社が負担します。"
            "Udemy、Courseraなどのオンライン学習プラットフォームの法人アカウントが利用可能です。"
            "書籍購入費は月額5,000円まで経費として申請できます。"
            "資格取得時の受験料は合格した場合に全額会社負担となります。"
        ),
    },
    {
        "id": 5,
        "title": "セキュリティポリシー",
        "content": (
            "社外へのデータ持ち出しは原則禁止です。必要な場合はISMS管理者の承認を得てください。"
            "パスワードは12文字以上で、英数字記号を含む必要があります。90日ごとに変更してください。"
            "不審なメールを受信した場合はセキュリティチーム（security@techcorp.example.com）に転送してください。"
            "個人所有デバイスでの業務利用（BYOD）は申請制です。MDMアプリのインストールが必須です。"
        ),
    },
    {
        "id": 6,
        "title": "福利厚生について",
        "content": (
            "テックコーポレーションの福利厚生には以下が含まれます。"
            "健康診断は年1回（35歳以上は人間ドック）が会社負担で受けられます。"
            "スポーツジム利用補助として月額3,000円が支給されます。"
            "社員食堂は昼食1食300円で利用可能です。"
            "育児休業は最大2年間取得可能で、復帰後は時短勤務制度も利用できます。"
            "結婚祝金30,000円、出産祝金50,000円が支給されます。"
        ),
    },
    {
        "id": 7,
        "title": "開発環境のセットアップ",
        "content": (
            "新入社員の開発環境セットアップ手順です。"
            "1. IT部門からMacBookまたはWindows PCを受け取ります。"
            "2. 社内GitLabアカウントの作成を IT ヘルプデスクに依頼してください。"
            "3. VPN クライアント（GlobalProtect）をインストールしてください。"
            "4. Slack の #general チャンネルに参加してください。"
            "5. 開発用のAWSアカウントが必要な場合はクラウドチームに申請してください。"
        ),
    },
    {
        "id": 8,
        "title": "評価制度と昇給について",
        "content": (
            "人事評価は年2回（4月と10月）実施されます。"
            "評価は「目標達成度」「行動評価」「360度フィードバック」の3軸で行われます。"
            "昇給は4月の評価結果に基づき7月に反映されます。"
            "賞与は6月と12月に支給され、業績連動部分と個人評価部分で構成されます。"
            "マネージャー昇格には社内研修の修了と部門長の推薦が必要です。"
        ),
    },
]

print(f"ドキュメント数: {len(documents)}")
for doc in documents:
    print(f"  [{doc['id']}] {doc['title']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ドキュメントのベクトル化（Embedding）
# MAGIC
# MAGIC 各ドキュメントをEmbeddingモデルで**数値ベクトル**に変換します。
# MAGIC
# MAGIC 前回のノートブックで学んだ通り、Embeddingは「テキストの意味」を数値で表現する技術です。
# MAGIC **意味が近いテキストはベクトルも近くなる**ため、質問と関連するドキュメントを探せます。
# MAGIC
# MAGIC ```
# MAGIC "リモートワークの申請方法" → [0.12, -0.34, 0.56, ...]  （1024次元のベクトル）
# MAGIC "在宅勤務について教えて"   → [0.11, -0.33, 0.55, ...]  ← 意味が近いのでベクトルも近い！
# MAGIC "経費の精算方法"          → [-0.45, 0.22, 0.01, ...]  ← 意味が違うのでベクトルも遠い
# MAGIC ```

# COMMAND ----------

def get_embeddings(texts: list[str]) -> list[list[float]]:
    """テキストのリストをEmbeddingベクトルに変換する"""
    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts,
    )
    return [item.embedding for item in response.data]

# 全ドキュメントの content をベクトル化
doc_texts = [doc["content"] for doc in documents]
doc_embeddings = get_embeddings(doc_texts)

print(f"各ドキュメントを {len(doc_embeddings[0])} 次元のベクトルに変換しました")
print(f"ベクトル化したドキュメント数: {len(doc_embeddings)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ベクトル類似度による検索（Retrieval）
# MAGIC
# MAGIC ユーザーの質問もベクトルに変換し、**コサイン類似度**でドキュメントとの類似度を計算します。
# MAGIC 類似度が高いドキュメントが、質問に関連する情報を含んでいる可能性が高いです。
# MAGIC
# MAGIC > **コサイン類似度**: 2つのベクトルの方向がどれだけ似ているかを 0〜1 で表します。
# MAGIC > 1に近いほど「意味が似ている」ことを示します。

# COMMAND ----------

def cosine_similarity(a, b):
    """2つのベクトルのコサイン類似度を計算"""
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def search_documents(query: str, top_k: int = 3) -> list[dict]:
    """
    質問に関連するドキュメントを検索する

    手順:
    1. 質問をベクトルに変換
    2. 全ドキュメントとのコサイン類似度を計算
    3. 類似度が高い順に top_k 件を返す
    """
    # 質問をベクトルに変換
    query_embedding = get_embeddings([query])[0]

    # 全ドキュメントとの類似度を計算
    similarities = []
    for i, doc_emb in enumerate(doc_embeddings):
        sim = cosine_similarity(query_embedding, doc_emb)
        similarities.append((i, sim))

    # 類似度の高い順にソート
    similarities.sort(key=lambda x: x[1], reverse=True)

    # 上位 top_k 件を返す
    results = []
    for idx, sim in similarities[:top_k]:
        results.append({
            "document": documents[idx],
            "similarity": sim,
        })
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ### 検索を試してみよう
# MAGIC
# MAGIC 実際に質問を投げて、どのドキュメントが検索されるか確認してみましょう。

# COMMAND ----------

query = "在宅勤務は週に何日できますか？"
results = search_documents(query, top_k=3)

print(f"質問: {query}\n")
print("=== 検索結果（類似度の高い順）===")
for i, r in enumerate(results):
    doc = r["document"]
    sim = r["similarity"]
    print(f"\n--- {i+1}位 (類似度: {sim:.4f}) ---")
    print(f"タイトル: {doc['title']}")
    print(f"内容: {doc['content'][:100]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC > **確認ポイント**: 「在宅勤務」という質問に対して、「リモートワーク制度について」のドキュメントが
# MAGIC > 最も高い類似度で検索されたはずです。
# MAGIC > 「在宅勤務」と「リモートワーク」は同じ意味ですが、Embeddingモデルがこの意味的な近さを捉えています。

# COMMAND ----------

# もう1つ試してみましょう
query2 = "本の購入費用は会社で出してもらえますか？"
results2 = search_documents(query2, top_k=3)

print(f"質問: {query2}\n")
print("=== 検索結果（類似度の高い順）===")
for i, r in enumerate(results2):
    doc = r["document"]
    sim = r["similarity"]
    print(f"\n--- {i+1}位 (類似度: {sim:.4f}) ---")
    print(f"タイトル: {doc['title']}")
    print(f"内容: {doc['content'][:100]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. RAG による回答生成（Generation）
# MAGIC
# MAGIC 検索で見つけたドキュメントを**コンテキスト（文脈情報）** としてLLMに渡し、
# MAGIC それに基づいた回答を生成します。
# MAGIC
# MAGIC これがRAGの核心部分です:
# MAGIC ```
# MAGIC LLMへの入力 = 「以下の情報を参考にして質問に答えて」+ 検索結果 + ユーザーの質問
# MAGIC ```

# COMMAND ----------

def rag_answer(query: str, top_k: int = 3) -> dict:
    """
    RAGパイプライン: 検索 → コンテキスト構築 → LLMで回答生成

    返り値:
        answer: LLMの回答
        sources: 参照したドキュメント一覧
    """
    # ① 関連ドキュメントを検索
    search_results = search_documents(query, top_k=top_k)

    # ② 検索結果をコンテキスト文字列に整形
    context_parts = []
    for r in search_results:
        doc = r["document"]
        context_parts.append(f"【{doc['title']}】\n{doc['content']}")
    context = "\n\n".join(context_parts)

    # ③ LLMに質問とコンテキストを渡して回答を生成
    system_prompt = (
        "あなたは社内FAQアシスタントです。\n"
        "以下の「参考情報」のみに基づいて質問に回答してください。\n"
        "参考情報に記載がない内容については「この情報は社内FAQに見つかりませんでした」と回答してください。\n"
        "回答は簡潔かつ正確にお願いします。"
    )

    user_prompt = f"## 参考情報\n\n{context}\n\n## 質問\n\n{query}"

    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_tokens=512,
        temperature=0.0,
    )

    return {
        "answer": response.choices[0].message.content,
        "sources": [
            {"title": r["document"]["title"], "similarity": r["similarity"]}
            for r in search_results
        ],
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### RAG で質問に回答してみよう

# COMMAND ----------

query = "リモートワークは週何日まで可能ですか？申請方法も教えてください。"

result = rag_answer(query)

print(f"質問: {query}\n")
print("=== RAG の回答 ===")
print(result["answer"])
print("\n=== 参照元ドキュメント ===")
for src in result["sources"]:
    print(f"  - {src['title']} (類似度: {src['similarity']:.4f})")

# COMMAND ----------

query = "書籍の購入補助はありますか？上限はいくらですか？"

result = rag_answer(query)

print(f"質問: {query}\n")
print("=== RAG の回答 ===")
print(result["answer"])
print("\n=== 参照元ドキュメント ===")
for src in result["sources"]:
    print(f"  - {src['title']} (類似度: {src['similarity']:.4f})")

# COMMAND ----------

query = "パスワードのルールを教えてください。"

result = rag_answer(query)

print(f"質問: {query}\n")
print("=== RAG の回答 ===")
print(result["answer"])
print("\n=== 参照元ドキュメント ===")
for src in result["sources"]:
    print(f"  - {src['title']} (類似度: {src['similarity']:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. RAG あり vs なし の比較
# MAGIC
# MAGIC RAGの効果を実感するために、**RAGなし（LLMだけ）** と **RAGあり** の回答を比較してみましょう。
# MAGIC
# MAGIC RAGなしの場合、LLMは「テックコーポレーション」の社内情報を知らないため、
# MAGIC 一般的な回答しかできません。

# COMMAND ----------

comparison_query = "テックコーポレーションの育児休業は何年取れますか？復帰後の制度はありますか？"

# --- RAGなし（LLMのみ）---
response_no_rag = client.chat.completions.create(
    model=LLM_MODEL,
    messages=[
        {"role": "system", "content": "あなたは親切な日本語アシスタントです。"},
        {"role": "user", "content": comparison_query},
    ],
    max_tokens=512,
    temperature=0.0,
)

print(f"質問: {comparison_query}\n")
print("=" * 50)
print("【RAGなし】LLMだけの回答:")
print("=" * 50)
print(response_no_rag.choices[0].message.content)

# COMMAND ----------

# --- RAGあり ---
result_with_rag = rag_answer(comparison_query)

print("=" * 50)
print("【RAGあり】検索結果を使った回答:")
print("=" * 50)
print(result_with_rag["answer"])
print("\n参照元:")
for src in result_with_rag["sources"]:
    print(f"  - {src['title']} (類似度: {src['similarity']:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC > **比較ポイント**:
# MAGIC > - **RAGなし**: 一般的な法律上の育休制度について回答（テックコーポレーション固有の情報なし）
# MAGIC > - **RAGあり**: 「最大2年間」「時短勤務制度あり」といった**テックコーポレーション固有の情報**に基づいた回答
# MAGIC >
# MAGIC > これがRAGの威力です！

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. RAGが答えられない質問
# MAGIC
# MAGIC RAGは万能ではありません。検索元のドキュメントに情報がなければ、正しく答えられません。
# MAGIC その場合は「情報がない」と正直に回答するよう、プロンプトで指示しています。

# COMMAND ----------

query_unknown = "社内のプロジェクト管理ツールは何を使っていますか？"

result_unknown = rag_answer(query_unknown)

print(f"質問: {query_unknown}\n")
print("=== RAG の回答 ===")
print(result_unknown["answer"])
print("\n=== 参照元ドキュメント ===")
for src in result_unknown["sources"]:
    print(f"  - {src['title']} (類似度: {src['similarity']:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC > **確認ポイント**: プロジェクト管理ツールに関する情報はFAQに含まれていないため、
# MAGIC > 「情報が見つかりませんでした」のような回答が返るはずです。
# MAGIC > これは**ハルシネーション（嘘の情報を生成すること）を防ぐ**ための重要な工夫です。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **RAGの概念** — LLMが知らない情報を「検索＋生成」で補う仕組み
# MAGIC 2. **Embedding** — ドキュメントと質問をベクトルに変換
# MAGIC 3. **ベクトル検索** — コサイン類似度で関連ドキュメントを発見
# MAGIC 4. **コンテキスト付き生成** — 検索結果をLLMに渡して正確な回答を生成
# MAGIC 5. **RAGの限界** — 検索元にない情報は答えられない（ハルシネーション防止）
# MAGIC
# MAGIC ### 本番環境での改善ポイント
# MAGIC
# MAGIC 今回はシンプルな実装でしたが、本番環境では以下の改善が考えられます:
# MAGIC
# MAGIC | 項目 | 今回の実装 | 本番向け |
# MAGIC |---|---|---|
# MAGIC | ドキュメント保存 | メモリ上 | Delta Table + Vector Search Index |
# MAGIC | ベクトル検索 | numpy コサイン類似度 | Databricks Vector Search |
# MAGIC | チャンク分割 | なし（文書全体） | 適切なサイズに分割 |
# MAGIC | 会話履歴 | なし | チャット履歴の管理 |
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - **Streamlit アプリ**（`app_rag/` ディレクトリ）で、チャットUIからRAGを体験してみましょう
# MAGIC - [Databricks Vector Search](https://docs.databricks.com/ja/generative-ai/vector-search.html) でスケーラブルなベクトル検索を試してみましょう
# MAGIC
# MAGIC > **認定試験との関連** (GenAI Engineer Associate):
# MAGIC > - **Data Preparation for RAG**: Embedding によるドキュメントのベクトル化、コサイン類似度
# MAGIC > - **Data Preparation for RAG**: RAG パイプラインの設計（検索→コンテキスト構築→生成）
# MAGIC > - **Evaluate and Optimize**: RAG あり/なしの比較、ハルシネーション防止

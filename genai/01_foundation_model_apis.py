# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Foundation Model APIs で生成AIを試す
# MAGIC
# MAGIC このノートブックでは、Databricksに組み込まれた **Foundation Model APIs** を使って、大規模言語モデル（LLM）を手軽に試す方法を学びます。
# MAGIC
# MAGIC **LLM（大規模言語モデル）とは？**
# MAGIC > ChatGPT などでおなじみの、テキストを理解・生成できるAIモデルです。
# MAGIC > 質問に答えたり、文章を要約したり、翻訳したりすることができます。
# MAGIC
# MAGIC **Foundation Model APIs とは？**
# MAGIC > Databricksが提供する、LLMを簡単に呼び出せるAPIです。
# MAGIC > 自分でモデルをデプロイ（設置）する必要がなく、**従量課金（pay-per-token）** で手軽に利用できます。
# MAGIC
# MAGIC > **初心者の方へ**: 普段 ChatGPT を使っているなら、イメージは同じです。
# MAGIC > 違いは、**プログラムから**LLMを呼び出せること。これにより、
# MAGIC > 大量のデータに自動でLLMを適用したり、自分のアプリにAI機能を組み込んだりできます。
# MAGIC
# MAGIC ```
# MAGIC OpenAI の ChatGPT API:  openai.com にリクエスト → データが外部に出る
# MAGIC Databricks Foundation Model APIs:  自社の Databricks ワークスペース内で完結 → データが外に出ない
# MAGIC ```
# MAGIC
# MAGIC ## 学べること
# MAGIC - Foundation Model APIs（pay-per-token）によるLLMの呼び出し
# MAGIC - OpenAI互換クライアントを使ったチャット補完
# MAGIC - プロンプトエンジニアリングの基本（Few-shotなど）
# MAGIC - バッチ処理によるデータへのLLM適用
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）のクラスターを使用してください
# MAGIC - ワークスペースで Foundation Model APIs が有効になっていること
# MAGIC - クラスターサイズはシングルノード（Single Node）で十分です
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. セットアップ
# MAGIC
# MAGIC Databricks Foundation Model APIs は **OpenAI互換のインターフェース** を提供しています。
# MAGIC つまり、OpenAI社のライブラリ（`openai`）をそのまま使ってDatabricks上のLLMにアクセスできます。
# MAGIC
# MAGIC > **ポイント**: APIキーやURLはDatabricksノートブック上では自動的に取得されるため、
# MAGIC > 手動で設定する必要はありません。

# COMMAND ----------

# MAGIC %pip install -q openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
from openai import OpenAI

# Databricks のトークンとエンドポイントを自動取得
# ノートブック上では環境変数に自動設定されています
token = os.environ.get("DATABRICKS_TOKEN", dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
host = os.environ.get("DATABRICKS_HOST", f"https://{spark.conf.get('spark.databricks.workspaceUrl')}")

client = OpenAI(
    api_key=token,
    base_url=f"{host}/serving-endpoints",
)

print(f"エンドポイント: {host}/serving-endpoints")
print("セットアップ完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 基本的なチャット補完
# MAGIC
# MAGIC まずはシンプルな質問をLLMに投げてみましょう。
# MAGIC
# MAGIC `databricks-meta-llama-3-3-70b-instruct` は Databricks が提供する pay-per-token モデルの一つです。
# MAGIC
# MAGIC **メッセージの構造**:
# MAGIC - `system`: LLMの役割や振る舞いを指定します（例: 「日本語で答えてください」）
# MAGIC - `user`: ユーザーからの質問や指示です
# MAGIC - `assistant`: LLMの応答です（Few-shotの例で使います）

# COMMAND ----------

MODEL_NAME = "databricks-meta-llama-3-3-70b-instruct"

response = client.chat.completions.create(
    model=MODEL_NAME,
    messages=[
        {"role": "system", "content": "あなたは親切な日本語アシスタントです。"},
        {"role": "user", "content": "機械学習とは何ですか？初心者にもわかるように3文で説明してください。"},
    ],
    max_tokens=256,
)

print(response.choices[0].message.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. パラメータの調整
# MAGIC
# MAGIC `temperature` パラメータを変えると、出力のランダムさを制御できます。
# MAGIC
# MAGIC | temperature | 特徴 | 適した用途 |
# MAGIC |---|---|---|
# MAGIC | `0.0` | 毎回ほぼ同じ回答（決定的） | 分類、情報抽出など正確性が必要なタスク |
# MAGIC | `0.5` | 適度なバランス | 一般的な質問応答 |
# MAGIC | `1.0` | ランダムで創造的 | ブレインストーミング、創作 |
# MAGIC
# MAGIC 以下で同じ質問を `temperature=0.0` と `temperature=1.0` で試して、違いを観察してみましょう。

# COMMAND ----------

# temperature = 0.0（決定的: 毎回同じ結果になりやすい）
response_low = client.chat.completions.create(
    model=MODEL_NAME,
    messages=[
        {"role": "user", "content": "「データ」を使った俳句を1つ作ってください。"},
    ],
    max_tokens=128,
    temperature=0.0,
)

print("=== temperature=0.0 ===")
print(response_low.choices[0].message.content)

# COMMAND ----------

# temperature = 1.0（ランダム: 毎回異なる結果になりやすい）
response_high = client.chat.completions.create(
    model=MODEL_NAME,
    messages=[
        {"role": "user", "content": "「データ」を使った俳句を1つ作ってください。"},
    ],
    max_tokens=128,
    temperature=1.0,
)

print("=== temperature=1.0 ===")
print(response_high.choices[0].message.content)

# COMMAND ----------

# MAGIC %md
# MAGIC > **試してみよう**: 上のセルを何度か実行してみてください。
# MAGIC > `temperature=0.0` はほぼ同じ結果になりますが、`temperature=1.0` は毎回異なる結果になるはずです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Few-shot プロンプティング
# MAGIC
# MAGIC **Few-shot プロンプティング**とは、LLMに「こういう入力にはこう答えてほしい」という**例を数個**見せるテクニックです。
# MAGIC
# MAGIC プログラムを書かなくても、例を見せるだけでLLMが「パターン」を理解して同じ形式で回答してくれます。
# MAGIC
# MAGIC | プロンプト手法 | 例の数 | 説明 |
# MAGIC |---|---|---|
# MAGIC | **Zero-shot** | 0個 | 例なしで直接質問（上のセクションで実践済み） |
# MAGIC | **One-shot** | 1個 | 例を1つ見せる |
# MAGIC | **Few-shot** | 2〜数個 | 例を複数見せる（最もよく使われる） |
# MAGIC
# MAGIC 以下の例では、レビューの感情分析（ポジティブ/ネガティブ/ニュートラル）を行います:
# MAGIC - 例1〜3: LLMにパターンを教える「お手本」
# MAGIC - 最後の入力: 実際に分析したいテキスト

# COMMAND ----------

response = client.chat.completions.create(
    model=MODEL_NAME,
    messages=[
        {
            "role": "system",
            "content": "あなたはテキストの感情を分析するアシスタントです。",
        },
        # 例1: ポジティブの例
        {"role": "user", "content": "テキスト: この映画は最高でした！\n感情:"},
        {"role": "assistant", "content": "ポジティブ"},
        # 例2: ニュートラルの例
        {"role": "user", "content": "テキスト: 今日の天気は普通ですね。\n感情:"},
        {"role": "assistant", "content": "ニュートラル"},
        # 例3: ネガティブの例
        {"role": "user", "content": "テキスト: 電車が遅延して最悪だった。\n感情:"},
        {"role": "assistant", "content": "ネガティブ"},
        # ↓ ここからが実際の分析対象
        {"role": "user", "content": "テキスト: この製品のコスパがとても良くて気に入っています。\n感情:"},
    ],
    max_tokens=16,
    temperature=0.0,
)

print(f"感情分析の結果: {response.choices[0].message.content}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 構造化出力（JSON形式）
# MAGIC
# MAGIC LLMの出力をプログラムで処理したい場合、**JSON形式**で回答するよう指示すると便利です。
# MAGIC
# MAGIC **JSON（ジェイソン）** とは、プログラムが扱いやすいデータ形式です:
# MAGIC ```json
# MAGIC {"company": "ソニー", "product": "ヘッドホン", "sentiment": "positive"}
# MAGIC ```
# MAGIC
# MAGIC systemメッセージで出力形式を具体的に指定するのがコツです。

# COMMAND ----------

import json

response = client.chat.completions.create(
    model=MODEL_NAME,
    messages=[
        {
            "role": "system",
            "content": (
                "あなたはテキストから情報を抽出するアシスタントです。"
                "必ず以下のJSON形式で回答してください:\n"
                '{"company": "会社名", "product": "製品名", "sentiment": "positive/negative/neutral"}'
            ),
        },
        {
            "role": "user",
            "content": "ソニーの新しいヘッドホンWH-1000XM6はノイキャン性能が素晴らしい。",
        },
    ],
    max_tokens=128,
    temperature=0.0,
)

result_text = response.choices[0].message.content
print("=== LLMの出力 ===")
print(result_text)

# JSONとしてパース（プログラムで扱えるデータに変換）
try:
    result = json.loads(result_text)
    print("\n=== パース結果 ===")
    for key, value in result.items():
        print(f"  {key}: {value}")
except json.JSONDecodeError:
    print("\n（JSONとしてパースできませんでした。プロンプトの調整が必要です）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. バッチ処理: 複数テキストへのLLM適用
# MAGIC
# MAGIC 実務では、1件だけでなく**大量のテキストデータ**に対してLLMを適用したい場合があります。
# MAGIC
# MAGIC ここでは、pandas DataFrame に格納された複数のレビューに対して、
# MAGIC 感情分析を**一括で適用**する方法を示します。
# MAGIC
# MAGIC > **仕組み**: `apply()` メソッドを使って、DataFrameの各行に対して関数を適用しています。

# COMMAND ----------

import pandas as pd

# サンプルデータ: 5件のレビュー
reviews = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "review": [
        "価格が安くて品質も良い、大満足です",
        "届くまでに2週間かかった、遅すぎる",
        "普通に使えるけど特に感動はない",
        "デザインが美しく機能性も抜群",
        "初期不良があり交換対応もイマイチだった",
    ],
})

display(reviews)

# COMMAND ----------

def analyze_sentiment(text: str) -> str:
    """テキストの感情をLLMで分析する関数"""
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {
                "role": "system",
                "content": "テキストの感情を「ポジティブ」「ネガティブ」「ニュートラル」の一語で回答してください。",
            },
            {"role": "user", "content": text},
        ],
        max_tokens=8,
        temperature=0.0,
    )
    return response.choices[0].message.content.strip()

# 各レビューに感情分析を適用
# apply() で各行のレビューテキストに対して analyze_sentiment 関数を実行
reviews["sentiment"] = reviews["review"].apply(analyze_sentiment)

display(reviews)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Embedding（テキスト埋め込み）
# MAGIC
# MAGIC **Embedding（埋め込み）** とは、テキストを**数値のベクトル（数値の配列）** に変換する技術です。
# MAGIC
# MAGIC **何の役に立つの？**
# MAGIC - テキスト同士の**類似度**を数値で計算できます
# MAGIC - 意味が近い文章は、ベクトルも近くなります
# MAGIC - 検索、レコメンデーション、クラスタリングなどに活用されます
# MAGIC
# MAGIC 以下の例では3つのテキストをベクトルに変換し、**コサイン類似度**で類似性を比較します。

# COMMAND ----------

EMBEDDING_MODEL = "databricks-gte-large-en"

texts = [
    "機械学習はデータからパターンを学ぶ技術です",
    "深層学習はニューラルネットワークを使った手法です",
    "今日のランチはカレーライスでした",
]

response = client.embeddings.create(
    model=EMBEDDING_MODEL,
    input=texts,
)

for i, emb in enumerate(response.data):
    vector = emb.embedding
    print(f"テキスト{i+1}: 次元数={len(vector)}, 先頭5要素={vector[:5]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### コサイン類似度で類似性を確認
# MAGIC
# MAGIC **コサイン類似度**は、2つのベクトルがどれくらい同じ方向を向いているかを測る指標です。
# MAGIC - **1に近い**: 意味が似ている
# MAGIC - **0に近い**: 意味が異なる

# COMMAND ----------

# コサイン類似度で類似性を確認
from numpy import dot
from numpy.linalg import norm

vectors = [emb.embedding for emb in response.data]

def cosine_similarity(a, b):
    return dot(a, b) / (norm(a) * norm(b))

print("=== テキスト間のコサイン類似度 ===")
for i in range(len(texts)):
    for j in range(i + 1, len(texts)):
        sim = cosine_similarity(vectors[i], vectors[j])
        print(f"  テキスト{i+1} vs テキスト{j+1}: {sim:.4f}")

print(f"\nテキスト1: {texts[0]}")
print(f"テキスト2: {texts[1]}")
print(f"テキスト3: {texts[2]}")
print("\n→ テキスト1とテキスト2（どちらもML関連）の類似度が高いことを確認してみましょう")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## まとめ
# MAGIC
# MAGIC このノートブックでは以下を学びました:
# MAGIC
# MAGIC 1. **Foundation Model APIs** — Databricksに組み込まれたLLMの呼び出し方
# MAGIC 2. **パラメータ調整** — temperatureによる出力制御
# MAGIC 3. **Few-shot** — 例を与えることでタスクを定義
# MAGIC 4. **構造化出力** — JSON形式での情報抽出
# MAGIC 5. **バッチ処理** — DataFrameへのLLM一括適用
# MAGIC 6. **Embedding** — テキストのベクトル化と類似度計算
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - 異なるモデル（`databricks-dbrx-instruct` など）を試してみましょう
# MAGIC - より複雑なプロンプトで要約・翻訳などのタスクに挑戦してみましょう
# MAGIC - [Databricks AI Playground](https://docs.databricks.com/ja/large-language-models/ai-playground.html) でインタラクティブにモデルを試すこともできます
# MAGIC
# MAGIC > **認定試験との関連** (GenAI Engineer Associate):
# MAGIC > - **Design Applications with Foundation Models**: Foundation Model APIs、pay-per-token、temperature パラメータ
# MAGIC > - **Design Applications with Foundation Models**: Few-shot プロンプティング、構造化出力
# MAGIC > - **Databricks Tools**: Foundation Model APIs の OpenAI 互換インターフェース

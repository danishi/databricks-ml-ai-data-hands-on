# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Foundation Model APIs で生成AIを試す
# MAGIC
# MAGIC このノートブックでは、Databricksに組み込まれた Foundation Model APIs を使って、大規模言語モデル（LLM）を手軽に試す方法を学びます。
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
# MAGIC Databricks Foundation Model APIs は OpenAI互換のインターフェースを提供しています。
# MAGIC `openai` ライブラリを使ってアクセスできます。

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
# MAGIC `temperature` を変えると、出力のランダムさを制御できます。
# MAGIC - `0.0`: 最も決定的（毎回同じ回答）
# MAGIC - `1.0`: よりランダム（創造的）

# COMMAND ----------

# temperature = 0.0（決定的）
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

# temperature = 1.0（ランダム）
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
# MAGIC ## 4. Few-shot プロンプティング
# MAGIC
# MAGIC 数個の例を示すことで、LLMの出力形式をコントロールできます。

# COMMAND ----------

response = client.chat.completions.create(
    model=MODEL_NAME,
    messages=[
        {
            "role": "system",
            "content": "あなたはテキストの感情を分析するアシスタントです。",
        },
        # 例1
        {"role": "user", "content": "テキスト: この映画は最高でした！\n感情:"},
        {"role": "assistant", "content": "ポジティブ"},
        # 例2
        {"role": "user", "content": "テキスト: 今日の天気は普通ですね。\n感情:"},
        {"role": "assistant", "content": "ニュートラル"},
        # 例3
        {"role": "user", "content": "テキスト: 電車が遅延して最悪だった。\n感情:"},
        {"role": "assistant", "content": "ネガティブ"},
        # 実際の分析対象
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
# MAGIC プロンプトで出力形式を指定すると、プログラムで扱いやすい構造化データを得られます。

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

# JSONとしてパース
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
# MAGIC pandas DataFrameのテキストデータに対して、LLMを一括適用する例です。

# COMMAND ----------

import pandas as pd

# サンプルデータ
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
    """テキストの感情をLLMで分析する"""
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
reviews["sentiment"] = reviews["review"].apply(analyze_sentiment)

display(reviews)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Embedding（テキスト埋め込み）
# MAGIC
# MAGIC テキストをベクトルに変換する Embedding モデルも利用できます。
# MAGIC 類似度検索やクラスタリングに活用できます。

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

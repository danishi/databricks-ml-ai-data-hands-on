# Databricks notebook source
# MAGIC %md
# MAGIC # マルチステージ推論とエージェント
# MAGIC
# MAGIC このノートブックでは、LLMを使った**マルチステージ推論**と**エージェント**の基本を学びます。
# MAGIC
# MAGIC **マルチステージ推論とは？**
# MAGIC > 複雑な質問を**複数のステップ**に分解して段階的に処理する手法です。
# MAGIC > 一度のLLM呼び出しでは難しい問題を、分割して解決します。
# MAGIC
# MAGIC **エージェントとは？**
# MAGIC > LLMが**ツール（関数）を自律的に呼び出して**タスクを遂行するシステムです。
# MAGIC > 検索、計算、API呼び出しなどのツールを使い分けて問題を解決します。
# MAGIC
# MAGIC > **初心者の方へ: エージェントを身近な例で理解する**
# MAGIC >
# MAGIC > エージェント = **秘書さん** のイメージです。
# MAGIC > - あなた: 「明日の会議室を予約して」
# MAGIC > - 秘書: (1) スケジュールを確認 → (2) 空き会議室を検索 → (3) 予約を実行 → (4) 結果を報告
# MAGIC >
# MAGIC > LLMエージェントも同様に、質問を理解し、必要なツールを選び、実行し、結果をまとめます。
# MAGIC
# MAGIC ## 学べること
# MAGIC - プロンプトチェイニング（複数のLLM呼び出しの連結）
# MAGIC - ツール使用（Function Calling）の概念
# MAGIC - シンプルなRAGエージェントの実装
# MAGIC
# MAGIC ## 前提条件
# MAGIC - Databricks Runtime ML（例: 16.x ML）
# MAGIC - Foundation Model APIs が有効であること
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %pip install -q openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
import json
from openai import OpenAI

token = os.environ.get("DATABRICKS_TOKEN", dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get())
host = os.environ.get("DATABRICKS_HOST", f"https://{spark.conf.get('spark.databricks.workspaceUrl')}")

client = OpenAI(api_key=token, base_url=f"{host}/serving-endpoints")
LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"

print("セットアップ完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. プロンプトチェイニング
# MAGIC
# MAGIC **プロンプトチェイニング**は、あるLLMの出力を次のLLMの入力に使う手法です。
# MAGIC
# MAGIC ```
# MAGIC ステップ1: 質問を分析 → 「3つのサブ質問に分割」
# MAGIC ステップ2: 各サブ質問に回答
# MAGIC ステップ3: 回答を統合して最終回答を生成
# MAGIC ```
# MAGIC
# MAGIC > **メリット**: 複雑な質問を段階的に処理することで、回答の品質が向上します。

# COMMAND ----------

def call_llm(system_prompt: str, user_prompt: str) -> str:
    """LLMを呼び出すヘルパー関数"""
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_tokens=512,
        temperature=0.0,
    )
    return response.choices[0].message.content


# ステップ1: 質問を分析して計画を立てる
complex_question = "Databricksの主要機能を3つ挙げ、それぞれの利点と使用場面を説明してください。"

plan = call_llm(
    "あなたは質問を分析して回答計画を立てるアシスタントです。回答計画をJSON配列で出力してください。",
    f"以下の質問に答えるための回答計画を3ステップで立ててください。各ステップをJSON配列で出力してください。\n\n質問: {complex_question}",
)

print("=== ステップ1: 回答計画 ===")
print(plan)

# COMMAND ----------

# ステップ2: 計画に基づいて詳細な回答を生成
detailed_answer = call_llm(
    "あなたは技術文書を書く専門家です。簡潔かつ正確に回答してください。",
    f"以下の回答計画に基づいて、元の質問に対する包括的な回答を生成してください。\n\n質問: {complex_question}\n\n回答計画:\n{plan}",
)

print("=== ステップ2: 詳細な回答 ===")
print(detailed_answer)

# COMMAND ----------

# ステップ3: 回答を要約
summary = call_llm(
    "あなたは文章を簡潔に要約するアシスタントです。",
    f"以下の回答を3行以内で要約してください。\n\n{detailed_answer}",
)

print("=== ステップ3: 要約 ===")
print(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. ツール使用（Function Calling）の概念
# MAGIC
# MAGIC **ツール使用**は、LLMに「利用可能なツール（関数）の一覧」を渡し、
# MAGIC LLMが必要に応じて適切なツールを選んで呼び出す仕組みです。
# MAGIC
# MAGIC ```
# MAGIC ユーザー: 「東京の今の気温は？」
# MAGIC     ↓
# MAGIC LLM: 「weather_api(city="東京") を呼び出す必要がある」
# MAGIC     ↓
# MAGIC システム: weather_api を実行 → 結果: 15℃
# MAGIC     ↓
# MAGIC LLM: 「東京の現在の気温は15℃です」
# MAGIC ```
# MAGIC
# MAGIC 以下では、シンプルなツールを定義してLLMに使わせる例を実装します。

# COMMAND ----------

import numpy as np

# ツール（関数）の定義
def search_faq(query: str) -> str:
    """社内FAQを検索するツール"""
    faq_data = {
        "リモートワーク": "週3日まで可能。前日までにチームリーダーへ申請。コアタイム10:00〜15:00。",
        "有給休暇": "入社6ヶ月後に10日付与。3日以上の連続休暇は2週間前までに申請。",
        "経費精算": "EXシステムから申請。領収書添付必須。交通費は月末締め翌月15日払い。",
        "セキュリティ": "パスワード12文字以上。90日ごとに変更。データ持ち出し原則禁止。",
    }
    # 簡易的なキーワードマッチング
    for key, value in faq_data.items():
        if key in query:
            return value
    return "該当する情報が見つかりませんでした。"


def calculate(expression: str) -> str:
    """数式を計算するツール"""
    try:
        result = eval(expression, {"__builtins__": {}}, {"np": np})
        return str(result)
    except Exception as e:
        return f"計算エラー: {e}"


def get_current_date() -> str:
    """現在の日付を返すツール"""
    from datetime import datetime
    return datetime.now().strftime("%Y年%m月%d日 %H:%M")


# ツールの一覧
TOOLS = {
    "search_faq": {
        "function": search_faq,
        "description": "社内FAQデータベースからキーワードで情報を検索する",
    },
    "calculate": {
        "function": calculate,
        "description": "数式を計算する（四則演算、平方根など）",
    },
    "get_current_date": {
        "function": get_current_date,
        "description": "現在の日付と時刻を取得する",
    },
}

print("利用可能なツール:")
for name, info in TOOLS.items():
    print(f"  - {name}: {info['description']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. シンプルなエージェントの実装
# MAGIC
# MAGIC LLMに質問とツール一覧を渡し、LLMが「どのツールを使うか」を判断するエージェントを作ります。

# COMMAND ----------

def run_agent(question: str) -> str:
    """
    シンプルなエージェント: LLMがツール使用を判断して回答を生成

    流れ:
    1. LLMに質問とツール一覧を渡す
    2. LLMがどのツールを使うか判断
    3. ツールを実行
    4. ツールの結果を使ってLLMが最終回答を生成
    """
    tool_descriptions = "\n".join([
        f"- {name}: {info['description']}" for name, info in TOOLS.items()
    ])

    # ステップ1: LLMにツール選択を依頼
    tool_selection = call_llm(
        system_prompt=(
            "あなたはツールを使って質問に答えるアシスタントです。\n"
            "以下のツールが利用可能です:\n"
            f"{tool_descriptions}\n\n"
            "質問に答えるために必要なツールとその引数をJSON形式で出力してください。\n"
            "ツールが不要な場合は {\"tool\": \"none\"} と出力してください。\n"
            '出力例: {"tool": "search_faq", "args": "リモートワーク"}'
        ),
        user_prompt=question,
    )

    print(f"  [エージェント] ツール選択: {tool_selection}")

    # ステップ2: ツールの実行
    tool_result = None
    try:
        # JSONをパース（LLMの出力からJSON部分を抽出）
        json_str = tool_selection
        if "```" in json_str:
            json_str = json_str.split("```")[1].strip()
            if json_str.startswith("json"):
                json_str = json_str[4:].strip()
        tool_call = json.loads(json_str)

        tool_name = tool_call.get("tool", "none")
        if tool_name != "none" and tool_name in TOOLS:
            args = tool_call.get("args", "")
            tool_func = TOOLS[tool_name]["function"]
            tool_result = tool_func(args) if args else tool_func()
            print(f"  [エージェント] ツール実行: {tool_name}({args}) → {tool_result}")
    except (json.JSONDecodeError, KeyError):
        print("  [エージェント] ツール選択をスキップ（直接回答します）")

    # ステップ3: ツール結果を使って最終回答を生成
    if tool_result:
        final_answer = call_llm(
            "あなたは親切な日本語アシスタントです。ツールの結果に基づいて質問に回答してください。",
            f"質問: {question}\n\nツールの結果: {tool_result}\n\nこの結果に基づいて質問に簡潔に回答してください。",
        )
    else:
        final_answer = call_llm(
            "あなたは親切な日本語アシスタントです。",
            question,
        )

    return final_answer

# COMMAND ----------

# MAGIC %md
# MAGIC ### エージェントを試してみよう

# COMMAND ----------

questions = [
    "リモートワークのルールを教えてください",
    "100の平方根を計算してください",
    "今日の日付と時刻は？",
    "Pythonの特徴を教えてください",  # ツール不要なケース
]

for q in questions:
    print(f"\n{'='*60}")
    print(f"質問: {q}")
    answer = run_agent(q)
    print(f"回答: {answer}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. マルチターンエージェント
# MAGIC
# MAGIC 実際のチャットボットでは、**会話履歴**を保持して文脈を理解する必要があります。

# COMMAND ----------

class ConversationAgent:
    """会話履歴を保持するマルチターンエージェント"""

    def __init__(self):
        self.history = [
            {"role": "system", "content": "あなたは社内FAQアシスタントです。会話の文脈を理解して回答してください。"},
        ]

    def chat(self, user_message: str) -> str:
        self.history.append({"role": "user", "content": user_message})

        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=self.history,
            max_tokens=512,
            temperature=0.0,
        )
        assistant_message = response.choices[0].message.content
        self.history.append({"role": "assistant", "content": assistant_message})

        return assistant_message


agent = ConversationAgent()

# マルチターン会話のデモ
conversations = [
    "Databricksとは何ですか？",
    "それはどんな場面で使われますか？",
    "機械学習以外にも使えますか？",
]

for msg in conversations:
    print(f"\nユーザー: {msg}")
    reply = agent.chat(msg)
    print(f"アシスタント: {reply}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC 1. **プロンプトチェイニング** — 複数のLLM呼び出しを連結して複雑な質問に対応
# MAGIC 2. **ツール使用** — LLMが関数を呼び出して外部情報を取得
# MAGIC 3. **エージェント** — ツール選択→実行→回答生成を自律的に行うシステム
# MAGIC 4. **マルチターン** — 会話履歴を保持した文脈理解
# MAGIC
# MAGIC ### 次のステップ
# MAGIC - `05_evaluation_governance.py` で生成AIの評価とガバナンスを学びましょう
# MAGIC
# MAGIC > **認定試験との関連** (GenAI Engineer Associate):
# MAGIC > - **Build Multi-stage Reasoning Applications**: プロンプトチェイニング、ツール使用パターン
# MAGIC > - **Build Multi-stage Reasoning Applications**: エージェントの設計（ツール選択→実行→回答）
# MAGIC > - **Build Multi-stage Reasoning Applications**: マルチターン会話の文脈管理

"""
Streamlit Databricks App: RAG チャットボット

社内FAQドキュメントを Vector Search で検索し、LLM が回答を生成する RAG チャットアプリです。
Databricks Apps としてデプロイして使用します。

=== このアプリの仕組み（RAG パイプライン）===

  ユーザーが質問を入力
       ↓
  ① Databricks Vector Search で関連ドキュメントを検索
       ↓
  ② 検索結果 + 質問 を LLM に渡して回答を生成（Generation）
       ↓
  回答 + 参照元ドキュメントを表示

=== 前提条件 ===
- Foundation Model APIs が有効なワークスペースで Databricks Apps としてデプロイ
- genai/03_vector_search_rag.py を実行済み（Vector Search Index が作成済みであること）

=== デプロイ方法 ===
  1. サイドバーの「+ 新規」→「アプリ」を選択
  2. 「カスタムアプリを作成」をクリック
  3. アプリ名を入力（例: rag-chat-app）
  4. 「次: 設定」で「サービングエンドポイント」リソースを追加
  5. 「アプリの作成」をクリック
  6. アプリ詳細画面で「デプロイ」→ app_rag/ フォルダを選択

=== ファイル構成 ===
  app_rag/
  ├── app.py              ← このファイル（メインアプリ）
  ├── app.yaml            ← アプリの実行設定
  └── requirements.txt    ← 必要なライブラリ一覧
"""

import streamlit as st
from openai import OpenAI
from databricks.sdk import WorkspaceClient

# --- ページ設定 ---
st.set_page_config(
    page_title="社内FAQ RAGチャット",
    page_icon="💬",
    layout="centered",
)

# --- 定数 ---
LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"
VS_INDEX_NAME = "main.default.rag_documents_index"


@st.cache_resource
def get_clients():
    """Databricks クライアントを取得（Databricks Apps では自動認証）"""
    w = WorkspaceClient()
    openai_client = OpenAI(
        api_key=w.config.token,
        base_url=f"{w.config.host}/serving-endpoints",
    )
    return w, openai_client


def search_documents(query: str, top_k: int = 3) -> list[dict]:
    """Vector Search Index を使ってドキュメントを検索"""
    w, _ = get_clients()
    results = w.vector_search_indexes.query_index(
        index_name=VS_INDEX_NAME,
        columns=["chunk_id", "title", "content"],
        query_text=query,
        num_results=top_k,
    )
    return [
        {
            "chunk_id": row[0],
            "title": row[1],
            "content": row[2],
            "score": row[3] if len(row) > 3 else 0,
        }
        for row in results.result.data_array
    ]


def generate_rag_response(query: str, search_results: list) -> str:
    """検索結果をコンテキストとしてLLMに回答を生成させる"""
    _, openai_client = get_clients()

    context_parts = []
    for r in search_results:
        context_parts.append(f"【{r['title']}】\n{r['content']}")
    context = "\n\n".join(context_parts)

    system_prompt = (
        "あなたは社内FAQアシスタントです。\n"
        "以下の「参考情報」のみに基づいて質問に回答してください。\n"
        "参考情報に記載がない内容については「この情報は社内FAQに見つかりませんでした」と回答してください。\n"
        "回答は簡潔かつ正確にお願いします。"
    )

    user_prompt = f"## 参考情報\n\n{context}\n\n## 質問\n\n{query}"

    response = openai_client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        max_tokens=512,
        temperature=0.0,
    )
    return response.choices[0].message.content


def main():
    st.title("社内FAQ RAGチャット")
    st.caption("Databricks Vector Search + LLM で社内ドキュメントに基づいて回答します（RAG: 検索拡張生成）")

    # サイドバー: 設定情報を表示
    with st.sidebar:
        st.subheader("RAG 設定")
        st.text(f"Vector Search Index:\n{VS_INDEX_NAME}")
        st.text(f"LLM: {LLM_MODEL}")
        st.divider()
        st.caption("genai/03_vector_search_rag.py で作成した Vector Search Index を使用しています")

    # チャット履歴の初期化
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # チャット履歴を表示
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "sources" in msg:
                with st.expander("参照元ドキュメント"):
                    for src in msg["sources"]:
                        st.write(f"- {src['title']} (スコア: {src['score']:.4f})")

    # ユーザー入力
    if prompt := st.chat_input("社内制度について質問してください（例: リモートワークは何日まで？）"):
        # ユーザーメッセージを表示・保存
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # RAGで回答を生成
        with st.chat_message("assistant"):
            with st.spinner("Vector Search で検索中..."):
                search_results = search_documents(prompt, top_k=3)

            with st.spinner("回答を生成中..."):
                answer = generate_rag_response(prompt, search_results)

            st.markdown(answer)

            # 参照元を表示
            sources = [
                {"title": r["title"], "score": r["score"]}
                for r in search_results
            ]
            with st.expander("参照元ドキュメント"):
                for src in sources:
                    st.write(f"- {src['title']} (スコア: {src['score']:.4f})")

        # アシスタントメッセージを保存
        st.session_state.messages.append({
            "role": "assistant",
            "content": answer,
            "sources": sources,
        })


if __name__ == "__main__":
    main()

"""
Streamlit Databricks App: RAG チャットボット

社内FAQドキュメントを検索し、LLMが回答を生成するRAGチャットアプリです。
Databricks Apps としてデプロイして使用します。

=== このアプリの仕組み（RAG パイプライン）===

  ユーザーが質問を入力
       ↓
  ① 質問をベクトル（数値配列）に変換（Embedding）
       ↓
  ② FAQドキュメントとのコサイン類似度を計算し、関連するFAQを検索
       ↓
  ③ 検索結果 + 質問 を LLM に渡して回答を生成（Generation）
       ↓
  回答 + 参照元ドキュメントを表示

=== 前提条件 ===
- Foundation Model APIs が有効なワークスペースで Databricks Apps としてデプロイ
- genai/02_rag_chat.py の内容を理解していると、コードの意味がよりわかります

=== デプロイ方法 ===
  1. Databricks ワークスペースの「コンピューティング」→「アプリ」を選択
  2. 「アプリの作成」をクリック
  3. アプリ名を入力（例: rag-chat-app）し作成
  4. このリポジトリの app_rag/ フォルダをソースコードパスに指定
  5. デプロイを実行

=== ファイル構成 ===
  app_rag/
  ├── app.py              ← このファイル（メインアプリ）
  └── requirements.txt    ← 必要なライブラリ一覧
"""

import streamlit as st
import numpy as np
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
EMBEDDING_MODEL = "databricks-gte-large-en"

# --- 社内FAQドキュメント ---
DOCUMENTS = [
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


@st.cache_resource
def get_openai_client():
    """Databricks Foundation Model APIs 用のクライアントを取得"""
    w = WorkspaceClient()
    host = w.config.host
    token = w.config.token
    return OpenAI(
        api_key=token,
        base_url=f"{host}/serving-endpoints",
    )


@st.cache_resource
def compute_document_embeddings():
    """全ドキュメントのEmbeddingを事前計算（アプリ起動時に1回だけ実行）"""
    openai_client = get_openai_client()
    texts = [doc["content"] for doc in DOCUMENTS]
    response = openai_client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts,
    )
    return [item.embedding for item in response.data]


def cosine_similarity(a, b):
    """コサイン類似度を計算"""
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def search_documents(query: str, doc_embeddings: list, top_k: int = 3) -> list[dict]:
    """質問に関連するドキュメントをベクトル検索"""
    openai_client = get_openai_client()
    query_response = openai_client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=[query],
    )
    query_embedding = query_response.data[0].embedding

    similarities = []
    for i, doc_emb in enumerate(doc_embeddings):
        sim = cosine_similarity(query_embedding, doc_emb)
        similarities.append((i, sim))

    similarities.sort(key=lambda x: x[1], reverse=True)

    results = []
    for idx, sim in similarities[:top_k]:
        results.append({
            "document": DOCUMENTS[idx],
            "similarity": sim,
        })
    return results


def generate_rag_response(query: str, search_results: list) -> str:
    """検索結果をコンテキストとしてLLMに回答を生成させる"""
    openai_client = get_openai_client()

    context_parts = []
    for r in search_results:
        doc = r["document"]
        context_parts.append(f"【{doc['title']}】\n{doc['content']}")
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
    st.caption("社内ドキュメントをもとに、AIが質問に回答します（RAG: 検索拡張生成）")

    # サイドバー: ドキュメント一覧を表示
    with st.sidebar:
        st.subheader("検索対象のドキュメント")
        for doc in DOCUMENTS:
            with st.expander(f"[{doc['id']}] {doc['title']}"):
                st.write(doc["content"])
        st.divider()
        st.caption("これらのドキュメントからRAGで検索・回答します")

    # Embedding の事前計算
    doc_embeddings = compute_document_embeddings()

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
                        st.write(f"- {src['title']} (類似度: {src['similarity']:.4f})")

    # ユーザー入力
    if prompt := st.chat_input("社内制度について質問してください（例: リモートワークは何日まで？）"):
        # ユーザーメッセージを表示・保存
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # RAGで回答を生成
        with st.chat_message("assistant"):
            with st.spinner("検索中..."):
                search_results = search_documents(prompt, doc_embeddings, top_k=3)

            with st.spinner("回答を生成中..."):
                answer = generate_rag_response(prompt, search_results)

            st.markdown(answer)

            # 参照元を表示
            sources = [
                {"title": r["document"]["title"], "similarity": r["similarity"]}
                for r in search_results
            ]
            with st.expander("参照元ドキュメント"):
                for src in sources:
                    st.write(f"- {src['title']} (類似度: {src['similarity']:.4f})")

        # アシスタントメッセージを保存
        st.session_state.messages.append({
            "role": "assistant",
            "content": answer,
            "sources": sources,
        })


if __name__ == "__main__":
    main()

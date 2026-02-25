"""
Streamlit Databricks App: ワイン分類モデルの予測UI

=== このアプリの仕組み ===

  ブラウザ（このアプリ）
       ↓ スライダーで特徴量を入力
  Databricks Apps（Streamlit サーバー）
       ↓ WorkspaceClient.serving_endpoints.query()
  Model Serving エンドポイント（wine-classifier-endpoint）
       ↓ 学習済みモデルで予測
  予測結果（ワインの品種: class_0 / class_1 / class_2）
       ↓
  ブラウザに結果を表示

=== 前提条件 ===
- ml/08_model_serving.py を先に実行して、エンドポイントを作成済みであること
- Databricks Apps としてデプロイされていること（ローカルPCでは動きません）

=== デプロイ方法 ===
  1. Databricks ワークスペースの「コンピューティング」→「アプリ」を選択
  2. 「アプリの作成」をクリック
  3. アプリ名を入力（例: wine-classifier-app）し作成
  4. このリポジトリの app/ フォルダをソースコードパスに指定
  5. デプロイを実行

=== ファイル構成 ===
  app/
  ├── app.py              ← このファイル（メインアプリ）
  └── requirements.txt    ← 必要なライブラリ一覧
"""

import streamlit as st
import pandas as pd
from databricks.sdk import WorkspaceClient

# --- ページ設定 ---
st.set_page_config(
    page_title="ワイン分類予測",
    page_icon="🍷",
    layout="centered",
)

# --- 定数 ---
ENDPOINT_NAME = "wine-classifier-endpoint"
CLASS_NAMES = {0: "class_0", 1: "class_1", 2: "class_2"}

# ワインの特徴量の情報（名前、説明、範囲）
FEATURES = [
    {"name": "alcohol", "label": "アルコール度数", "min": 11.0, "max": 15.0, "default": 13.0, "step": 0.1},
    {"name": "malic_acid", "label": "リンゴ酸", "min": 0.7, "max": 6.0, "default": 2.3, "step": 0.1},
    {"name": "ash", "label": "灰分", "min": 1.3, "max": 3.3, "default": 2.4, "step": 0.1},
    {"name": "alcalinity_of_ash", "label": "灰分のアルカリ度", "min": 10.0, "max": 30.0, "default": 19.5, "step": 0.5},
    {"name": "magnesium", "label": "マグネシウム", "min": 70.0, "max": 165.0, "default": 100.0, "step": 1.0},
    {"name": "total_phenols", "label": "総フェノール", "min": 0.9, "max": 4.0, "default": 2.3, "step": 0.1},
    {"name": "flavanoids", "label": "フラバノイド", "min": 0.3, "max": 5.1, "default": 2.0, "step": 0.1},
    {"name": "nonflavanoid_phenols", "label": "非フラバノイドフェノール", "min": 0.1, "max": 0.7, "default": 0.4, "step": 0.05},
    {"name": "proanthocyanins", "label": "プロアントシアニン", "min": 0.4, "max": 3.6, "default": 1.6, "step": 0.1},
    {"name": "color_intensity", "label": "色の濃さ", "min": 1.2, "max": 13.0, "default": 5.1, "step": 0.1},
    {"name": "hue", "label": "色相", "min": 0.5, "max": 1.7, "default": 1.0, "step": 0.05},
    {"name": "od280/od315_of_diluted_wines", "label": "OD280/OD315（希釈ワイン）", "min": 1.2, "max": 4.0, "default": 2.6, "step": 0.1},
    {"name": "proline", "label": "プロリン", "min": 278.0, "max": 1680.0, "default": 750.0, "step": 10.0},
]


@st.cache_resource  # この関数の結果をキャッシュ（アプリ再読み込み時に再実行しない）
def get_workspace_client():
    """Databricks WorkspaceClient を取得（Databricks Apps では自動認証）

    Databricks Apps 上で動かす場合、認証情報（トークン等）は自動的に設定されるため、
    引数なしで WorkspaceClient() を呼ぶだけでOKです。
    """
    return WorkspaceClient()


def predict_wine(features: dict) -> int:
    """Model Serving エンドポイントに予測をリクエスト"""
    w = get_workspace_client()
    response = w.serving_endpoints.query(
        name=ENDPOINT_NAME,
        dataframe_records=[features],
    )
    return int(response.predictions[0])


def main():
    st.title("ワイン分類予測アプリ")
    st.markdown(
        "ワインの化学成分を入力すると、**Databricks Model Serving** を使って品種を予測します。"
    )

    st.divider()

    # --- 入力フォーム ---
    st.subheader("化学成分の入力")
    st.caption("スライダーを動かして各成分の値を設定してください。")

    feature_values = {}

    # 2列レイアウトで入力を配置
    col1, col2 = st.columns(2)
    for i, feat in enumerate(FEATURES):
        target_col = col1 if i % 2 == 0 else col2
        with target_col:
            feature_values[feat["name"]] = st.slider(
                feat["label"],
                min_value=feat["min"],
                max_value=feat["max"],
                value=feat["default"],
                step=feat["step"],
                key=feat["name"],
            )

    st.divider()

    # --- 予測実行 ---
    if st.button("予測を実行", type="primary", use_container_width=True):
        with st.spinner("Model Serving エンドポイントに問い合わせ中..."):
            try:
                prediction = predict_wine(feature_values)
                class_name = CLASS_NAMES.get(prediction, f"unknown({prediction})")

                st.success(f"予測結果: **{class_name}**（クラス {prediction}）")

                # 入力値のサマリーを表示
                with st.expander("入力した特徴量の詳細"):
                    input_df = pd.DataFrame(
                        [feature_values],
                        columns=[f["name"] for f in FEATURES],
                    )
                    st.dataframe(input_df.T.rename(columns={0: "値"}))

            except Exception as e:
                st.error(f"予測に失敗しました: {e}")
                st.info(
                    "エンドポイント 'wine-classifier-endpoint' が起動していることを確認してください。\n\n"
                    "ノートブック `ml/02_model_serving.py` を先に実行してエンドポイントを作成してください。"
                )


if __name__ == "__main__":
    main()

# Databricks ML / 生成AI ハンズオン

Databricks 上で機械学習（ML）と生成AI を低コストで試せる初心者向けハンズオン教材です。

## 対象者

- Databricks をこれから使い始める方
- 機械学習や生成AI に興味があるエンジニア・データサイエンティスト
- Python の基本的な知識がある方

## 必要な環境

- Databricks ワークスペース（Community Edition でも可）
- **Databricks Runtime ML**（例: 16.x ML）のクラスター
- クラスターサイズは **シングルノード（Single Node）** で十分です

## 使い方

### 1. リポジトリを Databricks にクローン

1. Databricks ワークスペースの左サイドバーで **「ワークスペース」** を選択
2. 右上の **「追加」** → **「Git フォルダー」** をクリック
3. このリポジトリの URL を入力してクローン

### 2. クラスターの作成

1. 左サイドバーの **「コンピューティング」** を選択
2. **「クラスターを作成」** をクリック
3. 以下の設定を推奨:
   - クラスターモード: **シングルノード**
   - Databricks Runtime: **Runtime ML** を選択（例: 16.x ML）
   - ノードタイプ: 最小構成でOK

### 3. ノートブックの実行

クローンしたフォルダー内のノートブック（`.py` ファイル）を開き、上から順にセルを実行してください。

## コンテンツ

### ML（機械学習）

| ノートブック | 内容 |
|---|---|
| `ml/01_scikit-learn_classification.py` | scikit-learn でワインの分類モデルを構築。MLflow で実験管理。 |

### GenAI（生成AI）

| ノートブック | 内容 |
|---|---|
| `genai/01_foundation_model_apis.py` | Foundation Model APIs で LLM を呼び出し。感情分析・構造化出力・Embedding など。 |

## ディレクトリ構成

```
databricks-ml-hands-on/
├── README.md
├── ml/                          # 機械学習ノートブック
│   └── 01_scikit-learn_classification.py
└── genai/                       # 生成AIノートブック
    └── 01_foundation_model_apis.py
```

## コスト目安

- **MLノートブック**: シングルノードクラスターで数分で完了します
- **生成AIノートブック**: Foundation Model APIs は pay-per-token のため、このハンズオンの範囲では少額で収まります

## ライセンス

MIT License

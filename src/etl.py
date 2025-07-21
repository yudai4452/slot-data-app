import pandas as pd
import numpy as np

# --- 店舗ごとの列名マッピング（前回の定義を利用） ---
COLUMN_MAP = {...}           # 省略（前回記載の dict）

CANONICAL_COLUMNS = [
    "台番号","累計スタート","スタート回数","BB回数","RB回数","ART回数",
    "最大持玉","最大差玉","BB確率","RB確率","ART確率","合成確率","前日最終スタート"
]

def normalize(df_raw: pd.DataFrame, store: str) -> pd.DataFrame:
    # 列名を共通化
    df = df_raw.rename(columns=COLUMN_MAP[store])

    # 足りない列を NaN で追加
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    # 文字列 "1/300" → 実数 1/300
    prob_cols = ["BB確率","RB確率","ART確率","合成確率"]
    for c in prob_cols:
        df[c] = (df[c]
                 .astype(str)
                 .str.extract(r"(\d+\.?\d*)")      # 数字だけ抜く
                 .astype(float)
                 .rdiv(1)                          # 1 / 値
                 .replace({np.inf: np.nan}))       # 0 で割った例外
    # 型をそろえる
    int_cols = ["台番号","累計スタート","スタート回数","BB回数","RB回数",
                "ART回数","最大持玉","最大差玉","前日最終スタート"]
    df[int_cols] = df[int_cols].apply(pd.to_numeric, errors="coerce").astype("Int64")
    return df[CANONICAL_COLUMNS]                # 列順も固定

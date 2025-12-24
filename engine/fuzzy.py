import pandas as pd
from rapidfuzz import fuzz


FUZZY_THRESHOLD = 92


def mark_fuzzy(df):
    df = df.copy()

    # 🔐 Backwards compatibility
    if "amount_cent" in df.columns and "amount_pence" not in df.columns:
        df["amount_pence"] = df["amount_cent"]

    df["fuzzy_group"] = None

    refs = df["ref_norm"].fillna("").tolist()

    for i in range(len(df)):
        if df.at[i, "fuzzy_group"] is not None:
            continue

        df.at[i, "fuzzy_group"] = i

        for j in range(i + 1, len(df)):
            if df.at[j, "fuzzy_group"] is not None:
                continue

            score = fuzz.ratio(refs[i], refs[j])

            if score >= FUZZY_THRESHOLD:
                df.at[j, "fuzzy_group"] = i

    # merge fuzzy groups into match_key when needed
    df["match_key"] = df.apply(
        lambda r: f"{r['fuzzy_group']}_{r['amount_pence']}"
        if pd.isna(r.get("match_key"))
        else r["match_key"],
        axis=1,
    )

    return df

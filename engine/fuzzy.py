from difflib import SequenceMatcher
import logging
from engine.utils import load_config
cfg = load_config()

def score(a, b):
    return SequenceMatcher(None, a, b).ratio()

def mark_fuzzy(df):
    thresh = float(cfg["matching"].get("fuzzy_threshold", 0.75))
    df["fuzzy_status"] = "NotFuzzy"
    grouped = df.groupby("amount_cent")
    for amount, group in grouped:
        refs = group["ref_norm"].tolist()
        idxs = group.index.tolist()
        for i in range(len(refs)):
            for j in range(i+1, len(refs)):
                s = score(refs[i], refs[j])
                if s >= thresh:
                    df.loc[idxs[i], "fuzzy_status"] = "FuzzyMatched"
                    df.loc[idxs[j], "fuzzy_status"] = "FuzzyMatched"
    logging.info("Fuzzy marking complete.")
    return df

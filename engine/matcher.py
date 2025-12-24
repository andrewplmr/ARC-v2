# matcher.py
import pandas as pd
from engine.fuzzy import mark_fuzzy

FX_RATES = {"GBP": 1.0, "USD": 0.79, "EUR": 0.86}

KNOWN_GATEWAYS = ["stripe", "paypal", "square"]
SMALL_FEE_THRESHOLD_PENCE = 500
DATE_TOLERANCE_DAYS = 2


def normalise_reference(x):
    if pd.isna(x):
        return ""
    return "".join(ch for ch in str(x).lower() if ch.isalnum())


def prepare_dataframe(df):
    df = df.copy()

    df["date"] = pd.to_datetime(df.get("date"), errors="coerce").dt.date
    df["amount"] = pd.to_numeric(df.get("amount"), errors="coerce").fillna(0.0)

    df["polarity"] = "debit" if df["amount"].mean() < 0 else "credit"
    df["amount"] = df["amount"].abs()

    df["currency"] = df.get("currency", "GBP")
    df["amount_gbp"] = df.apply(
        lambda r: r["amount"] * FX_RATES.get(r["currency"], 1.0), axis=1
    )

    df["amount_cent"] = (df["amount_gbp"] * 100).round().astype(int)
    df["reference"] = df.get("reference", "")
    df["ref_norm"] = df["reference"].apply(normalise_reference)

    return df


def make_match_key(df):
    df["match_key"] = (
        df["amount_cent"].astype(str) + "_" +
        df["ref_norm"] + "_" +
        df["date"].astype(str)
    )
    return df


def generate_match_reason(row):
    if row["final_status"] == "Matched":
        return "Exact reference match – cleared automatically"

    if row["final_status"] == "FuzzyMatched":
        return "Similar reference detected – cleared automatically"

    if row["variance_amount"]:
        return f"Likely processing fee – £{row['variance_amount']:.2f} detected"

    return "Unmatched transaction – manual review required"


def apply_matching(bank_df, ledger_df, gateway_df):
    bank_df["source"] = "bank"
    ledger_df["source"] = "ledger"
    gateway_df["source"] = "gateway"

    master = pd.concat([bank_df, ledger_df, gateway_df], ignore_index=True)
    master = prepare_dataframe(master)
    master = make_match_key(master)

    grouped = master.groupby("match_key")["source"].apply(set)
    reconciled_keys = grouped[grouped == {"bank", "ledger", "gateway"}].index

    master["final_status"] = "Unmatched"
    master.loc[master["match_key"].isin(reconciled_keys), "final_status"] = "Matched"

    master = mark_fuzzy(master)
    master.loc[
        (master["final_status"] == "Unmatched") &
        (master["fuzzy_status"] == "FuzzyMatched"),
        "final_status"
    ] = "FuzzyMatched"

    # ---------- Intelligent reasoning ----------
    master["variance_amount"] = master.apply(
        lambda r: r["amount_gbp"]
        if r["final_status"] == "Unmatched"
        and r["amount_cent"] <= SMALL_FEE_THRESHOLD_PENCE
        and any(g in r["ref_norm"] for g in KNOWN_GATEWAYS)
        else None,
        axis=1
    )

    master["match_reason"] = master.apply(generate_match_reason, axis=1)

    return (
        master,
        master[master["final_status"] == "Matched"],
        master[master["final_status"] == "FuzzyMatched"],
        master[master["final_status"] == "Unmatched"],
    )

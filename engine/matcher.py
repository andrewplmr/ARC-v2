import pandas as pd
import logging
from engine.fuzzy import mark_fuzzy
from engine.utils import load_config

cfg = load_config()

FX_RATES = {"GBP": 1.0, "USD": 0.79, "EUR": 0.86}

KNOWN_GATEWAYS = ["stripe", "paypal", "square"]
SMALL_FEE_THRESHOLD_PENCE = 500  # £5.00
DATE_TOLERANCE_DAYS = 2

def normalise_reference(x):
    if pd.isna(x):
        return ""
    s = str(x).lower()
    return "".join(ch for ch in s if ch.isalnum())

def prepare_dataframe(df):
    df = df.copy()

    # Dates
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    # Amounts
    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    else:
        df["amount"] = 0.0

    # Debit / Credit detection (NEW)
    if df["amount"].mean() < 0:
        df["polarity"] = "debit"
        df["amount"] = df["amount"].abs()
    else:
        df["polarity"] = "credit"

    # Currency conversion (NEW)
    if "currency" not in df.columns:
        df["currency"] = "GBP"
    df["amount_gbp"] = df.apply(
        lambda r: r["amount"] * FX_RATES.get(r["currency"], 1.0), axis=1
    )

    df["amount_cent"] = (df["amount_gbp"] * 100).round().astype(int)

    if "reference" not in df.columns:
        df["reference"] = ""

    df["ref_norm"] = df["reference"].apply(normalise_reference)

    return df

def make_match_key(df):
    df["match_key"] = (
        df["amount_cent"].astype(str) + "_" +
        df["ref_norm"] + "_" +
        df["date"].astype(str)
    )
    return df

def classify_match_reason(row, master_df):
    # Defaults
    row["match_type"] = None
    row["resolution_status"] = None
    row["variance_amount"] = None
    row["reason_code"] = None

    if row["final_status"] == "Matched":
        row["match_type"] = "exact_reference"
        row["resolution_status"] = "cleared"

    elif row["final_status"] == "FuzzyMatched":
        row["match_type"] = "fuzzy_reference"
        row["resolution_status"] = "cleared"

    else:
        # UNMATCHED LOGIC
        same_amount = master_df[
            (master_df["amount_cent"] == row["amount_cent"]) &
            (master_df["source"] != row["source"])
        ]

        # Timing difference
        if not same_amount.empty:
            date_diffs = same_amount["date"].apply(
                lambda d: abs((d - row["date"]).days) if d and row["date"] else None
            ).dropna()

            if any(date_diffs > DATE_TOLERANCE_DAYS):
                row["resolution_status"] = "exception_timing"
                row["reason_code"] = "timing_difference"
                return row

        # Likely gateway fee
        if row["amount_cent"] <= SMALL_FEE_THRESHOLD_PENCE:
            ref = row["ref_norm"]
            if any(g in ref for g in KNOWN_GATEWAYS):
                row["resolution_status"] = "cleared_with_difference"
                row["reason_code"] = "likely_processing_fee"
                row["variance_amount"] = row["amount_cent"] / 100
                return row

        # Unknown
        row["resolution_status"] = "exception_unknown"
        row["reason_code"] = "unknown"

    return row

def generate_match_reason(row):
    if row["resolution_status"] == "cleared":
        if row["match_type"] == "exact_reference":
            return "Exact reference match – cleared automatically"
        if row["match_type"] == "fuzzy_reference":
            return "Similar reference detected – cleared automatically"
        return "Matched by amount and date – cleared automatically"

    if row["resolution_status"] == "cleared_with_difference":
        if row["reason_code"] == "likely_processing_fee":
            return f"Likely processing fee – £{row['variance_amount']:.2f} with no corresponding bank entry"
        return f"Matched across 2 sources; £{row['variance_amount']:.2f} variance detected (gateway fee)"

    if row["resolution_status"] == "exception_timing":
        return "Matching amount found outside date tolerance – timing difference"

    if row["resolution_status"] == "exception_unknown":
        return "Unmatched transaction – no reliable match detected"

    return "Manual review required"

def apply_matching(bank_df, ledger_df, gateway_df):
    bank_df = bank_df.copy(); bank_df["source"] = "bank"
    ledger_df = ledger_df.copy(); ledger_df["source"] = "ledger"
    gateway_df = gateway_df.copy(); gateway_df["source"] = "gateway"

    master = pd.concat([bank_df, ledger_df, gateway_df], ignore_index=True)
    master = prepare_dataframe(master)
    master = make_match_key(master)

    grouped = master.groupby("match_key")["source"].apply(set).reset_index()
    reconciled_keys = set(
        grouped[grouped["source"].apply(lambda x: x == {"bank","ledger","gateway"})]["match_key"]
    )

    master["final_status"] = "Unmatched"
    master.loc[master["match_key"].isin(reconciled_keys), "final_status"] = "Matched"

    master = mark_fuzzy(master)
    master.loc[master["final_status"] == "Matched", "fuzzy_status"] = "NotFuzzy"

    master["final_status"] = master.apply(
        lambda r: "Matched" if r["final_status"] == "Matched"
        else "FuzzyMatched" if r.get("fuzzy_status") == "FuzzyMatched"
        else "Unmatched",
        axis=1
    )

    # -------- INTELLIGENT REASONING (NEW) --------
    master = master.apply(lambda r: classify_match_reason(r, master), axis=1)
    master["Match Reason"] = master.apply(generate_match_reason, axis=1)

    return (
        master,
        master[master["final_status"] == "Matched"],
        master[master["final_status"] == "FuzzyMatched"],
        master[master["final_status"] == "Unmatched"]
    )

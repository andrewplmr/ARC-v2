import pandas as pd
from engine.fuzzy import mark_fuzzy

FX_RATES = {"GBP": 1.0, "USD": 0.79, "EUR": 0.86}
KNOWN_GATEWAYS = ["stripe", "paypal", "square"]
SMALL_FEE_PENCE = 500  # £5.00


def normalise_reference(x):
    if pd.isna(x):
        return ""
    return "".join(c for c in str(x).lower() if c.isalnum())


def prepare(df, source):
    df = df.copy()
    df["source"] = source

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    else:
        df["date"] = None

    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0.0)
    df["currency"] = df.get("currency", "GBP")

    df["amount_gbp"] = df.apply(
        lambda r: r["amount"] * FX_RATES.get(r["currency"], 1.0), axis=1
    )
    df["amount_pence"] = (df["amount_gbp"] * 100).round().astype(int)

    df["reference"] = df.get("reference", "")
    df["ref_norm"] = df["reference"].apply(normalise_reference)

    return df


def make_key(df):
    df["match_key"] = df["ref_norm"] + "_" + df["amount_pence"].astype(str)
    return df


def reconcile_group(group):
    sources = set(group["source"])
    amounts = group["amount_pence"].tolist()
    ref = group["ref_norm"].iloc[0]

    status = "Unmatched"
    match_type = None
    resolution = "exception_unknown"
    variance = None
    reason = "Unmatched transaction – no reliable match detected"

    if len(sources) >= 2:
        max_amt = max(amounts)
        min_amt = min(amounts)
        diff = abs(max_amt - min_amt)

        if diff == 0 and sources == {"Bank", "Ledger", "Gateway"}:
            status = "Matched"
            match_type = "Exact reference match"
            resolution = "cleared"
            reason = "Exact reference match – cleared automatically"
        else:
            status = "Partially Matched"
            match_type = "Reference match with variance"
            variance = diff / 100
            resolution = "cleared_with_difference"
            reason = f"Matched by reference; £{variance:.2f} variance detected"

    elif (
        group["amount_pence"].iloc[0] <= SMALL_FEE_PENCE
        and any(g in ref for g in KNOWN_GATEWAYS)
    ):
        status = "Matched"
        match_type = "Gateway fee"
        variance = group["amount_pence"].iloc[0] / 100
        resolution = "cleared_with_difference"
        reason = f"Likely processing fee – £{variance:.2f} detected"

    return pd.Series({
        "Status": status,
        "Match Type": match_type,
        "Resolution Status": resolution,
        "Variance (£)": variance,
        "Match Reason": reason,
    })


def assign_group_date(master):
    def pick_date(g):
        d = g["date"].dropna()
        return d.iloc[0] if not d.empty else None

    master["Date"] = (
        master.groupby("match_key")
        .apply(pick_date)
        .reindex(master["match_key"])
        .values
    )
    return master


def apply_matching(bank_df, ledger_df, gateway_df):
    bank = prepare(bank_df, "Bank")
    ledger = prepare(ledger_df, "Ledger")
    gateway = prepare(gateway_df, "Gateway")

    master = pd.concat([bank, ledger, gateway], ignore_index=True)
    master = make_key(master)

    master = mark_fuzzy(master)

    summary = (
        master.groupby("match_key", dropna=False)
        .apply(reconcile_group)
        .reset_index()
    )

    master = master.merge(summary, on="match_key", how="left")
    master = assign_group_date(master)

    return (
        master,
        master[master["Status"] == "Matched"],
        master[master["Status"] == "Partially Matched"],
        master[master["Status"] == "Unmatched"],
    )

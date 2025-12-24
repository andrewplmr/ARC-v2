import pandas as pd
from engine.fuzzy import mark_fuzzy

FX_RATES = {"GBP": 1.0, "USD": 0.79, "EUR": 0.86}
KNOWN_GATEWAYS = ["stripe", "paypal", "square"]
SMALL_FEE_PENCE = 500
DATE_TOLERANCE_DAYS = 2


def _norm_ref(x):
    if pd.isna(x):
        return ""
    return "".join(c for c in str(x).lower() if c.isalnum())


def prepare(df, source):
    df = df.copy()
    df["source"] = source

    df["date"] = pd.to_datetime(df.get("date"), errors="coerce").dt.date
    df["amount"] = pd.to_numeric(df.get("amount"), errors="coerce").fillna(0.0)
    df["currency"] = df.get("currency", "GBP")

    df["amount_gbp"] = df.apply(
        lambda r: r["amount"] * FX_RATES.get(r["currency"], 1.0), axis=1
    )
    df["amount_pence"] = (df["amount_gbp"] * 100).round().astype(int)

    df["reference"] = df.get("reference", "")
    df["ref_norm"] = df["reference"].apply(_norm_ref)

    return df


def make_key(df):
    df["match_key"] = (
        df["amount_pence"].astype(str)
        + "_"
        + df["ref_norm"]
        + "_"
        + df["date"].astype(str)
    )
    return df


def reconcile_group(group):
    sources = set(group["source"])
    status = "Unmatched"
    match_type = None
    resolution = "exception_unknown"
    variance = None
    reason = "Unmatched transaction – no reliable match detected"

    if sources == {"bank", "ledger", "gateway"}:
        status = "Matched"
        match_type = "Exact reference match"
        resolution = "cleared"
        reason = "Exact reference match – cleared automatically"

    elif len(sources) >= 2:
        status = "Partially Matched"
        match_type = "Similar reference match"
        resolution = "cleared"
        reason = "Similar reference detected – cleared automatically"

    else:
        amt = group["amount_pence"].iloc[0]
        ref = group["ref_norm"].iloc[0]

        if amt <= SMALL_FEE_PENCE and any(g in ref for g in KNOWN_GATEWAYS):
            status = "Matched"
            resolution = "cleared_with_difference"
            variance = amt / 100
            reason = f"Likely processing fee – £{variance:.2f} detected"

    return pd.Series({
        "Status": status,
        "Match Type": match_type,
        "Resolution Status": resolution,
        "Variance (£)": variance,
        "Match Reason": reason
    })


def apply_matching(bank_df, ledger_df, gateway_df):
    bank = prepare(bank_df, "Bank")
    ledger = prepare(ledger_df, "Ledger")
    gateway = prepare(gateway_df, "Gateway")

    master = pd.concat([bank, ledger, gateway], ignore_index=True)
    master = make_key(master)

    master = mark_fuzzy(master)

    recon = (
        master
        .groupby("match_key", dropna=False)
        .apply(reconcile_group)
        .reset_index()
    )

    master = master.merge(recon, on="match_key", how="left")

    master["Date"] = (
        master.groupby("match_key")["date"]
        .transform(lambda x: x.dropna().iloc[0] if not x.dropna().empty else None)
    )

    return (
        master,
        master[master["Status"] == "Matched"],
        master[master["Status"] == "Partially Matched"],
        master[master["Status"] == "Unmatched"],
    )

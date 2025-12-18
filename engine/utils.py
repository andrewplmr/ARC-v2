import os
import logging
import yaml
import pandas as pd
import re
from datetime import datetime
from dateutil import parser

# -------------------------------
# COLUMN ALIASES (NEW)
# -------------------------------
COLUMN_ALIASES = {
    "reference": ["reference", "ref", "txn_id", "transaction id", "transaction_id", "id"],
    "amount": ["amount", "amt", "value", "total", "paid"],
    "date": ["date", "txn_date", "transaction_date", "posted_date"],
    "currency": ["currency", "ccy"]
}

# -------------------------------
# BASIC CLEANERS (UNCHANGED)
# -------------------------------
def clean_string(value):
    if pd.isna(value):
        return None
    s = str(value)
    s = s.replace("\u200b", "").replace("\n", "").replace("\r", "").replace("\t", "")
    s = s.strip()
    return s if s else None

def to_float(value):
    if pd.isna(value):
        return None
    s = clean_string(value)
    if s is None:
        return None
    s = re.sub(r"[£$€,]", "", s)
    s = s.replace(" ", "")
    try:
        return float(s)
    except ValueError:
        return None

def parse_date(value):
    if pd.isna(value):
        return None
    s = clean_string(value)
    if not s:
        return None
    try:
        return parser.parse(s, dayfirst=True).date()
    except Exception:
        return None

# -------------------------------
# COLUMN NAME NORMALISATION (NEW)
# -------------------------------
def normalise_column_names(columns):
    mapped = {}
    for col in columns:
        low = col.lower()
        found = False
        for canon, aliases in COLUMN_ALIASES.items():
            if any(a in low for a in aliases):
                mapped[col] = canon
                found = True
                break
        if not found:
            mapped[col] = low
    return mapped

# -------------------------------
# MAIN NORMALISATION (EXTENDED)
# -------------------------------
def normalise_dataframe(df, log_fn=logging):
    """
    Robust column + value normalisation with logging
    """
    df = df.copy()

    if df.empty:
        raise ValueError("File is empty or has no rows. Upload a valid CSV or Excel file.")

    # Column cleaning
    cleaned_cols = {c: clean_string(c).lower() for c in df.columns}
    df.rename(columns=cleaned_cols, inplace=True)

    # Column alias mapping
    alias_map = normalise_column_names(df.columns)
    df.rename(columns=alias_map, inplace=True)

    log_fn.info(f"Columns after normalisation: {list(df.columns)}")

    # Value normalisation
    for col in df.columns:
        if col == "date":
            df[col] = df[col].apply(parse_date)
        elif col == "amount":
            df[col] = df[col].apply(to_float)
        else:
            df[col] = df[col].apply(clean_string)

    # Validate required columns
    if "amount" not in df.columns:
        raise ValueError('Amount column not found. Expected one of "amount, amt, value, total"')

    if "date" not in df.columns:
        raise ValueError('Date column not found. Expected formats like DD/MM/YYYY')

    bad_dates = df["date"].isna().sum()
    if bad_dates:
        log_fn.warning(f"{bad_dates} rows contain unrecognised date formats")

    return df

# -------------------------------
# CONFIG / LOGGING (UNCHANGED)
# -------------------------------
def load_config():
    cfg_path = os.path.join("config", "config.yaml")
    if not os.path.exists(cfg_path):
        raise FileNotFoundError(f"Config missing at {cfg_path}")
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logging(client_name="global"):
    os.makedirs("logs", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logfile = os.path.join("logs", f"recon_{client_name}_{timestamp}.log")

    logging.basicConfig(
        filename=logfile,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s"
    )

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s — %(levelname)s — %(message)s")
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

    logging.info("Logging initialized.")
    return logfile

def ensure_client_folder(client_name, base=None):
    cfg = load_config()
    base = base or cfg["app"].get("default_client_folder", "./clients")
    safe = client_name.strip().replace(" ", "_") or "client"
    folder = os.path.join(base, safe)
    os.makedirs(folder, exist_ok=True)
    os.makedirs(os.path.join(folder, "input"), exist_ok=True)
    os.makedirs(os.path.join(folder, "output"), exist_ok=True)
    os.makedirs(os.path.join(folder, "logs"), exist_ok=True)
    return folder

# ===============================
# RUN FOLDERING (NEW, SAFE)
# ===============================
def create_run_folder(client_name, base=None):
    """
    Creates and returns a run-specific folder structure:

    output/
      ClientXYZ/
        runs/
          2025-01-24_13-55-02/
            input/
            output/
            metadata.json
    """
    cfg = load_config()
    base = base or cfg["app"].get("default_client_folder", "./output")

    safe_client = client_name.strip().replace(" ", "_") or "client"
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

    run_root = os.path.join(base, safe_client, "runs", timestamp)
    input_dir = os.path.join(run_root, "input")
    output_dir = os.path.join(run_root, "output")

    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    return {
        "client": safe_client,
        "run_id": timestamp,
        "run_root": run_root,
        "input_dir": input_dir,
        "output_dir": output_dir,
        "metadata_path": os.path.join(run_root, "metadata.json"),
    }
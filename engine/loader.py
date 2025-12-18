import pandas as pd
import os
import glob
import logging
from engine.utils import load_config, normalise_dataframe

cfg = load_config()

def detect_files(input_dir="./input"):
    ext_patterns = ["*.csv", "*.xlsx", "*.xls"]
    files = []
    for p in ext_patterns:
        files.extend(glob.glob(os.path.join(input_dir, p)))
    return [f for f in files if os.path.isfile(f)]

def identify_role_by_name(filename):
    name = os.path.basename(filename).lower()
    for role, keys in cfg["expected_files"].items():
        for k in keys:
            if k in name:
                return role
    return None

def read_any(path_or_filelike):
    if hasattr(path_or_filelike, "read"):
        try:
            return pd.read_csv(path_or_filelike)
        except Exception:
            path_or_filelike.seek(0)
            return pd.read_excel(path_or_filelike)

    return pd.read_excel(path_or_filelike) if str(path_or_filelike).endswith(".xlsx") else pd.read_csv(path_or_filelike)

def infer_and_load(paths_list):
    mapping = {"bank": None, "ledger": None, "gateway": None}

    for p in paths_list:
        try:
            role = identify_role_by_name(p)
            df = read_any(p)
            df = normalise_dataframe(df, logging)

            if not role:
                cols = " ".join(df.columns)
                for r in mapping:
                    if any(k in cols for k in cfg["expected_files"].get(r, [])):
                        role = r
                        break

            if role:
                mapping[role] = df
                logging.info(f"Assigned {p} → {role}")
        except Exception as e:
            logging.error(f"Failed to read {p}: {e}")

    return mapping

def smart_load_uploaded(filelike):
    df = read_any(filelike)
    return normalise_dataframe(df, logging)
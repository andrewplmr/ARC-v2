import sys
import streamlit as st
import os
import io
from datetime import datetime
import pandas as pd
import logging

from engine.utils import load_config, setup_logging, ensure_client_folder, create_run_folder
from engine.loader import detect_files, infer_and_load, smart_load_uploaded
from engine.matcher import apply_matching, prepare_dataframe
from engine.excel_writer import write_styled_workbook
from engine.pdf_report import generate_pdf_summary
from engine.emailer import send_report_via_email

cfg = load_config()

# --------- Helpers for mapping UI ----------
def suggest_column_mapping(df):
    """Return dictionary of suggested mappings for date/amount/reference based on column names."""
    cols = list(df.columns)
    lower = [c.lower() for c in cols]
    mapping = {}
    # date
    for cand in cfg["columns"]["date_candidates"]:
        if any(cand in c for c in lower):
            mapping["date"] = cols[lower.index(next(c for c in lower if cand in c))]
            break
    # amount
    for cand in cfg["columns"]["amount_candidates"]:
        if any(cand in c for c in lower):
            mapping["amount"] = cols[lower.index(next(c for c in lower if cand in c))]
            break
    # reference
    for cand in cfg["columns"]["ref_candidates"]:
        if any(cand in c for c in lower):
            mapping["reference"] = cols[lower.index(next(c for c in lower if cand in c))]
            break
    # fallback to first columns
    if "date" not in mapping and len(cols) >= 1:
        mapping["date"] = cols[0]
    if "amount" not in mapping and len(cols) >= 2:
        mapping["amount"] = cols[1]
    if "reference" not in mapping and len(cols) >= 3:
        mapping["reference"] = cols[2]
    return mapping

def rename_columns_to_standard(df, mapping):
    """Rename the dataframe columns according to mapping to standard names: date, amount, reference"""
    df = df.copy()
    rename_map = {}
    for std in ["date","amount","reference"]:
        if std in mapping and mapping[std] in df.columns:
            rename_map[mapping[std]] = std
    df = df.rename(columns=rename_map)
    return df

# --------- Streamlit UI ----------
def run_streamlit():
    st.set_page_config(page_title=cfg["app"].get("name","Reconciliation"), layout="wide")
    st.title(cfg["app"].get("name","Reconciliation Pro"))
    st.markdown("Upload Bank, Ledger, and Payment Gateway files (CSV/XLSX). The tool will auto-detect columns and propose mappings — confirm or edit before running reconciliation.")

    # client name & logging
    client_name = st.text_input("Client name (used for foldering)", value="ClientXYZ")
    logfile = setup_logging(client_name=client_name)
    st.sidebar.markdown(f"**Logfile:** {logfile}")

    # Upload or auto-detect
    st.sidebar.header("Input options")
    use_autodetect = st.sidebar.checkbox("Auto-detect files from ./input/", value=True)
    uploaded = st.file_uploader("Upload files (you can upload multiple)", accept_multiple_files=True, type=["csv","xlsx","xls"])

    files_map = {"bank": None, "ledger": None, "gateway": None}
    uploaded_count = 0

    # If auto-detect requested, show detected and allow loading
    if use_autodetect:
        files_in_input = detect_files("./input")
        st.sidebar.write(f"Files found in ./input/: {len(files_in_input)}")
        if st.sidebar.button("Load detected files"):
            mapping = infer_and_load(files_in_input)
            files_map.update(mapping)
            st.success("Loaded auto-detected files (check mapping below).")

    # If uploaded via drag & drop, read them and attempt to assign roles
    if uploaded:
        uploaded_count = len(uploaded)
        for f in uploaded:
            try:
                df = smart_load_uploaded(f)
                # guess role by filename
                assigned = None
                name = f.name.lower()
                for r, patterns in cfg["expected_files"].items():
                    if any(p in name for p in patterns):
                        assigned = r
                        break
                # heuristics by header
                if not assigned:
                    cols = " ".join([c.lower() for c in df.columns])
                    for r in cfg["expected_files"].keys():
                        if any(p in cols for p in cfg["expected_files"][r]):
                            assigned = r
                            break
                if not assigned:
                    st.warning(f"Could not detect role for {f.name}. Please choose below.")
                    # assign to None for manual selection
                    files_map.setdefault("unassigned", []).append((f.name, df))
                else:
                    files_map[assigned] = df
                    st.success(f"Assigned {f.name} -> {assigned}")
            except Exception as e:
                st.error(f"Failed to read {f.name}: {e}")

    st.markdown("### Suggested column mappings (confirm or edit)")
    # Show mapping UI for each role
    col1, col2, col3 = st.columns(3)
    mapping_store = {}
    for (role, container) in zip(["bank","ledger","gateway"], [col1, col2, col3]):
        with container:
            st.subheader(role.capitalize())
            df = files_map.get(role)
            if df is None:
                st.info("No file loaded for this role.")
                mapping_store[role] = None
                continue
            # show preview
            st.dataframe(df.head(5))
            suggested = suggest_column_mapping(df)
            date_sel = st.selectbox(f"{role} → Date column", options=list(df.columns), index=list(df.columns).index(suggested.get("date", list(df.columns)[0])), key=f"{role}_date")
            amount_sel = st.selectbox(f"{role} → Amount column", options=list(df.columns), index=list(df.columns).index(suggested.get("amount", list(df.columns)[1 if len(df.columns)>1 else 0])), key=f"{role}_amount")
            ref_sel = st.selectbox(f"{role} → Reference column", options=list(df.columns), index=list(df.columns).index(suggested.get("reference", list(df.columns)[2 if len(df.columns)>2 else 0])), key=f"{role}_ref")
            mapping_store[role] = {"date": date_sel, "amount": amount_sel, "reference": ref_sel}

    st.markdown("---")
    if st.button("Run reconciliation"):
        # validation: ensure mapping exists for each role
        missing = [r for r in ["bank","ledger","gateway"] if mapping_store.get(r) is None]
        if missing:
            st.error(f"Missing files or mappings for: {missing}")
            st.stop()

        # Apply mappings and run matching
        try:
            # create client folder
            run = create_run_folder(client_name, base=cfg["app"].get("default_client_folder"))
            ts = run["run_id"]

            # rename columns to standard in copies and save inputs
            mapped_dfs = {}
            for r in ["bank","ledger","gateway"]:
                df_orig = files_map[r]
                mapping = mapping_store[r]
                df_std = df_orig.copy()
                rename_map = {mapping["date"]: "date", mapping["amount"]: "amount", mapping["reference"]: "reference"}
                df_std = df_std.rename(columns=rename_map)
                # Save the standardized input to client's input folder
                in_path = os.path.join(run["input_dir"], f"{r}.csv")
                df_std.to_csv(in_path, index=False)
                mapped_dfs[r] = df_std

            # run matching
            master, rec, fuzzy, unmatched = apply_matching(mapped_dfs["bank"], mapped_dfs["ledger"], mapped_dfs["gateway"])

            # write excel
            out_name = cfg["excel"]["output_filename_template"].format(client=client_name.replace(" ","_"), ts=ts)
            out_path = os.path.join(run["output_dir"], out_name)
            write_styled_workbook(master, rec, fuzzy, unmatched, client_name, out_path)

            # pdf summary
            summary = {
                "Total Transactions": len(master),
                "Matched": len(rec),
                "FuzzyMatched": len(fuzzy),
                "Unmatched": len(unmatched),
                "Match Rate (%)": round(len(rec)/len(master)*100,2) if len(master) else 0.0
            }
            pdf_path = os.path.join(run["output_dir"], f"summary_{client_name}_{ts}.pdf")
            generate_pdf_summary(pdf_path, client_name, summary)

            st.success("Reconciliation complete — files saved to client folder.")
            with open(out_path, "rb") as f:
                st.download_button("Download Excel", data=f.read(), file_name=out_name, mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            with open(pdf_path, "rb") as f:
                st.download_button("Download PDF Summary", data=f.read(), file_name=os.path.basename(pdf_path), mime="application/pdf")

            # email if enabled in config (optional)
            if cfg.get("smtp", {}).get("enabled", False):
                st.info("Sending email to client via SMTP...")
                send_report_via_email([cfg["smtp"]["username"]], f"Reconciliation — {client_name}", "Please find attached.", [out_path, pdf_path])
                st.success("Email sent (see logs).")

            import json

            metadata = {
                "client": client_name,
                "run_id": ts,
                "timestamp_utc": ts,
                "files": {
                    "bank": "bank.csv",
                    "ledger": "ledger.csv",
                    "gateway": "gateway.csv",
                    "excel": os.path.basename(out_path),
                    "pdf": os.path.basename(pdf_path),
                },
                "summary": summary,
            }

            with open(run["metadata_path"], "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)

        except Exception as e:
            logging.exception("Reconciliation failed")
            st.error(f"Reconciliation failed: {e}")
            st.stop()

# CLI behavior if script run directly
def run_cli():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", required=True)
    parser.add_argument("--input-folder", default="./input")
    parser.add_argument("--out-folder", default="./clients")
    args = parser.parse_args()

    client_folder = ensure_client_folder(args.client, base=args.out_folder)
    logfile = setup_logging(client_name=args.client)
    files = detect_files(args.input_folder)
    mapping = infer_and_load(files)
    if not all(mapping.get(k) is not None for k in ["bank","ledger","gateway"]):
        raise SystemExit("Missing files for bank/ledger/gateway in input folder.")
    master, rec, fuzzy, unmatched = apply_matching(mapping["bank"], mapping["ledger"], mapping["gateway"])
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_name = cfg["excel"]["output_filename_template"].format(client=args.client.replace(" ","_"), ts=ts)
    out_path = os.path.join(client_folder, "output", out_name)
    write_styled_workbook(master, rec, fuzzy, unmatched, args.client, out_path)
    summary = {
        "Total Transactions": len(master),
        "Matched": len(rec),
        "FuzzyMatched": len(fuzzy),
        "Unmatched": len(unmatched),
        "Match Rate (%)": round(len(rec)/len(master)*100,2) if len(master) else 0.0
    }
    pdf_path = os.path.join(client_folder, "output", f"summary_{args.client}_{ts}.pdf")
    generate_pdf_summary(pdf_path, args.client, summary)
    print("Done. Files written to:", out_path)

if __name__ == "__main__":
    # If running under streamlit, Streamlit will import this module and we want run_streamlit to execute.
    # But when run as normal script, fall back to CLI.
    if "STREAMLIT_RUN" in os.environ or "streamlit" in sys.argv[0]:
        run_streamlit()
    else:
        import sys
        if len(sys.argv) > 1:
            run_cli()
        else:
            run_streamlit()
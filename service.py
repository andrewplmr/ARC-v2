# service.py
import os, json
from datetime import datetime
from engine.utils import create_run_folder
from engine.matcher import apply_matching
from engine.excel_writer import write_styled_workbook
from engine.pdf_report import generate_pdf_summary

def run_reconciliation(client_name: str, mapped_dfs: dict, cfg: dict):
    run = create_run_folder(client_name, base=cfg["app"].get("default_client_folder"))
    ts = run["run_id"]

    master, rec, fuzzy, unmatched = apply_matching(
        mapped_dfs["bank"],
        mapped_dfs["ledger"],
        mapped_dfs["gateway"]
    )

    out_name = cfg["excel"]["output_filename_template"].format(
        client=client_name.replace(" ", "_"), ts=ts
    )
    out_path = os.path.join(run["output_dir"], out_name)

    write_styled_workbook(master, rec, fuzzy, unmatched, client_name, out_path)

    summary = {
        "Total Transactions": len(master),
        "Matched": len(rec),
        "FuzzyMatched": len(fuzzy),
        "Unmatched": len(unmatched),
        "Match Rate (%)": round(len(rec)/len(master)*100,2) if len(master) else 0.0
    }

    pdf_path = os.path.join(run["output_dir"], f"summary_{client_name}_{ts}.pdf")
    generate_pdf_summary(pdf_path, client_name, summary)

    return {
        "excel_path": out_path,
        "pdf_path": pdf_path,
        "summary": summary,
        "run_id": ts
    }

import os
import shutil
import uuid
from datetime import datetime
from typing import Optional

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# --- your existing engine imports ---
from engine.utils import load_config, setup_logging, ensure_client_folder, create_run_folder
from engine.matcher import apply_matching
from engine.excel_writer import write_styled_workbook

# -------------------------------------------------
# App setup
# -------------------------------------------------

app = FastAPI(title="ARC Reconciliation API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # OK for now
    allow_methods=["*"],
    allow_headers=["*"],
)

cfg = load_config()

BASE_TMP = "./tmp"
os.makedirs(BASE_TMP, exist_ok=True)


# -------------------------------------------------
# Safety GET handler (prevents Method Not Allowed page)
# -------------------------------------------------

@app.get("/reconcile")
def reconcile_get():
    return {
        "message": "POST files to this endpoint to run reconciliation."
    }


# -------------------------------------------------
# Main POST endpoint (HTML form compatible)
# -------------------------------------------------

@app.post("/reconcile")
async def reconcile(
    client_name: str = Form(...),
    bank: UploadFile = File(...),
    ledger: UploadFile = File(...),
    gateway: UploadFile = File(...)
):
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]
    workdir = os.path.join(BASE_TMP, run_id)
    os.makedirs(workdir, exist_ok=True)

    try:
        # -------------------------------------------------
        # Save uploaded files
        # -------------------------------------------------
        paths = {}
        for role, file in {
            "bank": bank,
            "ledger": ledger,
            "gateway": gateway
        }.items():
            path = os.path.join(workdir, f"{role}_{file.filename}")
            with open(path, "wb") as f:
                shutil.copyfileobj(file.file, f)
            paths[role] = path

        # -------------------------------------------------
        # Load data
        # -------------------------------------------------
        def load_df(path):
            if path.lower().endswith(".csv"):
                return pd.read_csv(path)
            return pd.read_excel(path)

        bank_df = load_df(paths["bank"])
        ledger_df = load_df(paths["ledger"])
        gateway_df = load_df(paths["gateway"])

        # -------------------------------------------------
        # Run reconciliation
        # -------------------------------------------------
        master, rec, fuzzy, unmatched = apply_matching(
            bank_df, ledger_df, gateway_df
        )

        # -------------------------------------------------
        # Write Excel output
        # -------------------------------------------------
        out_name = f"reconciliation_{client_name.replace(' ', '_')}_{run_id}.xlsx"
        out_path = os.path.join(workdir, out_name)

        write_styled_workbook(
            master, rec, fuzzy, unmatched,
            client_name,
            out_path
        )

        # -------------------------------------------------
        # RETURN FILE (THIS IS THE CRITICAL PART)
        # -------------------------------------------------
        return FileResponse(
            path=out_path,
            filename=out_name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

    finally:
        # optional cleanup can be added later
        pass

import os
import shutil
import uuid
from datetime import datetime

import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ---- your existing engine imports ----
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

BASE_TMP = "./tmp"
os.makedirs(BASE_TMP, exist_ok=True)

# -------------------------------------------------
# PROBE ROUTE (REQUIRED FOR FRAMER / BROWSER PREFLIGHT)
# -------------------------------------------------

@app.get("/reconcile")
@app.head("/reconcile")
def reconcile_probe():
    return {"status": "ok"}

# -------------------------------------------------
# MAIN RECONCILIATION ENDPOINT
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
        # Load dataframes
        # -------------------------------------------------
        def load_df(path: str) -> pd.DataFrame:
            if path.lower().endswith(".csv"):
                return pd.read_csv(path)
            return pd.read_excel(path)

        bank_df = load_df(paths["bank"])
        ledger_df = load_df(paths["ledger"])
        gateway_df = load_df(paths["gateway"])

        # -------------------------------------------------
        # Run reconciliation engine
        # -------------------------------------------------
        master, rec, fuzzy, unmatched = apply_matching(
            bank_df,
            ledger_df,
            gateway_df
        )

        # -------------------------------------------------
        # Write Excel output
        # -------------------------------------------------
        safe_client = client_name.strip().replace(" ", "_")
        output_name = f"reconciliation_{safe_client}_{run_id}.xlsx"
        output_path = os.path.join(workdir, output_name)

        write_styled_workbook(
            master,
            rec,
            fuzzy,
            unmatched,
            client_name,
            output_path
        )

        # -------------------------------------------------
        # RETURN FILE (CRITICAL)
        # -------------------------------------------------
        return FileResponse(
            path=output_path,
            filename=output_name,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

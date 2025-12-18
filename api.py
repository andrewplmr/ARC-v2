from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse

from engine.utils import load_config
from engine.loader import smart_load_uploaded
from service import run_reconciliation

app = FastAPI()
cfg = load_config()

from io import BytesIO

@app.post("/reconcile")
async def reconcile(
    client_name: str,
    bank: UploadFile = File(...),
    ledger: UploadFile = File(...),
    gateway: UploadFile = File(...)
):
    try:
        dfs = {
            "bank": smart_load_uploaded(BytesIO(await bank.read())),
            "ledger": smart_load_uploaded(BytesIO(await ledger.read())),
            "gateway": smart_load_uploaded(BytesIO(await gateway.read())),
        }

        result = run_reconciliation(client_name, dfs, cfg)

        return FileResponse(
            result["excel_path"],
            filename="reconciliation.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
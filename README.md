Reconciliation Pro — Basic Business-ready Version

1) Setup
   python -m venv venv
   source venv/bin/activate        # Windows: venv\Scripts\activate
   pip install -r requirements.txt

2) Configure
   edit config/config.yaml (set app name, smtp, thresholds)

3) Start UI
   streamlit run app.py

4) CLI
   python app.py --client "ClientName" --input-folder ./input

Notes:
 - Place sample bank/ledger/gateway files into ./input for auto-detect, or upload via UI.
 - Confirm suggested column mappings before running.
 - Outputs saved into clients/<ClientName>/output
 - Logs are in logs/

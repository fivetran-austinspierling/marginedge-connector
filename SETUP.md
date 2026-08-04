# Setup Steps — MarginEdge Connector Local Test (Windows)

These steps get the connector running locally against your MarginEdge account, on Windows. Your API key stays on your machine the entire time — it is never sent to anyone outside MarginEdge/Fivetran, and should never be pasted into chat, email, or committed to version control.

Run all commands below in **PowerShell**.

## Prerequisites

- Python 3.9–3.12 installed and on your PATH ([check supported versions](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements))
  - Verify with: `python --version`
- Your MarginEdge Public API key

## 1. Unzip the package

```powershell
Expand-Archive marginedge-connector.zip -DestinationPath .
cd marginedge
```

## 2. Create and activate a virtual environment

Keeps the connector's dependencies isolated from anything else on your machine.

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

You'll know it worked because your prompt will be prefixed with `(venv)`. Run every remaining step from inside this activated environment.

> If PowerShell blocks the activation script with an "execution policy" error, run this once, then retry the activation command: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`

## 3. Install the Fivetran Connector SDK

```powershell
pip install fivetran-connector-sdk
```

No other dependencies are needed — `requirements.txt` is intentionally empty (the connector only uses `requests`, which the SDK provides).

## 4. Add your API key to the configuration

Do **not** open `configuration.json` and type the key in directly. Run:

```powershell
fivetran reset
fivetran debug --configuration configuration.json
```

The first run will prompt you interactively for any configuration values it doesn't yet have real values for (`api_key`, `initial_sync_start`). Enter your real API key when prompted — it's encrypted at rest, not stored in plain text in `configuration.json`.

> If your SDK version instead expects a separate encryption helper script, run that from this directory per the instructions in `README.md` under **Configuration file** before running `fivetran debug`.

## 5. Run the connector

```powershell
fivetran debug
```

This runs a full sync against the live MarginEdge API (there's no sandbox environment — this is real production data, but the connector only performs GET requests, so nothing on the MarginEdge side is modified) and writes the results to a local DuckDB file you can inspect.

## 6. Check the results

```powershell
pip install duckdb
python -c "import duckdb; print(duckdb.sql('SHOW TABLES').fetchall())"
```

Then work through the **Local testing checklist** in `README.md` and send back logs/row counts (not the key) so we can fold in any fixes.

## 7. Clean up when done

```powershell
deactivate
```

This exits the virtual environment. You can delete the `venv` folder entirely if you're done testing.

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pathlib import Path
import sqlite3

app = FastAPI(title="TX FH Allowed Medical Lookup")

DB_PATH = Path("data/allowed_amounts.sqlite")
UI_PATH = Path("frontend/index.html")


# -----------------------
# Database helper
# -----------------------
def get_connection():
    if not DB_PATH.exists():
        raise RuntimeError("Database not found. Run the build workflow first.")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# -----------------------
# UI (root page)
# -----------------------
@app.get("/", response_class=HTMLResponse)
def serve_ui():
    if not UI_PATH.exists():
        raise HTTPException(status_code=500, detail="UI file not found")
    return UI_PATH.read_text()


# -----------------------
# Lookup API
# -----------------------
@app.get("/lookup")
def lookup(
    geozip: int,
    code: int,
    modifier: int | None = Query(default=None)
):
    conn = get_connection()

    try:
        if modifier is not None:
            query = """
            SELECT *
            FROM allowed_amounts
            WHERE geozip = ?
              AND code = ?
              AND modifier = ?
            """
            params = (geozip, code, modifier)
        else:
            query = """
            SELECT *
            FROM allowed_amounts
            WHERE geozip = ?
              AND code = ?
              AND modifier IS NULL
            """
            params = (geozip, code)

        row = conn.execute(query, params).fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail="No matching record found"
            )

        return dict(row)

    finally:
        conn.close()



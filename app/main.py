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
        raise HTTPException(
            status_code=500,
            detail="Allowed amounts database not found. Data build may not have run."
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# -----------------------
# UI
# -----------------------
@app.get("/", response_class=HTMLResponse)
def serve_ui():
    if not UI_PATH.exists():
        raise HTTPException(status_code=500, detail="UI file not found")
    return UI_PATH.read_text()


# -----------------------
# Lookup API with validation + fallback
# -----------------------
@app.get("/lookup")
def lookup(
    geozip: int = Query(..., description="Geographic ZIP"),
    code: int = Query(..., description="Procedure code"),
    modifier: int | None = Query(default=None)
):
    conn = get_connection()

    try:
        # 1. Exact match if modifier provided
        if modifier is not None:
            exact_query = """
            SELECT *
            FROM allowed_amounts
            WHERE geozip = ?
              AND code = ?
              AND modifier = ?
            """
            row = conn.execute(
                exact_query, (geozip, code, modifier)
            ).fetchone()

            if row:
                result = dict(row)
                result["match_type"] = "Exact match on modifier"
                return result

        # 2. Fallback to no-modifier row
        fallback_query = """
        SELECT *
        FROM allowed_amounts
        WHERE geozip = ?
          AND code = ?
          AND modifier IS NULL
        """
        row = conn.execute(
            fallback_query, (geozip, code)
        ).fetchone()

        if row:
            result = dict(row)
            result["match_type"] = (
                "Fallback match (no modifier on file)"
                if modifier is not None
                else "Base match (no modifier)"
            )
            return result

        # 3. No match found
        raise HTTPException(
            status_code=404,
            detail=(
                f"No allowed amount found for GeoZip {geozip}, "
                f"Code {code}"
                + (f", Modifier {modifier}" if modifier else "")
            )
        )

    finally:
        conn.close()




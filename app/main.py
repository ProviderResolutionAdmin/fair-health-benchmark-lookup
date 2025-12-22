from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pathlib import Path
import sqlite3
from datetime import datetime

app = FastAPI(title="TX FH Allowed Medical Lookup")

DB_PATH = Path("data/allowed_amounts.sqlite")
UI_PATH = Path("frontend/index.html")


# -----------------------
# Database helpers
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


def ensure_log_table(conn):
    """
    Create usage log table if it does not exist.
    Code is stored as TEXT to match lookup behavior.
    """
    conn.execute("""
    CREATE TABLE IF NOT EXISTS lookup_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lookup_time TEXT NOT NULL,
        geozip INTEGER NOT NULL,
        code TEXT NOT NULL,
        modifier TEXT,
        match_type TEXT,
        success INTEGER NOT NULL
    )
    """)
    conn.commit()


def log_lookup(conn, geozip, code, modifier, match_type, success):
    conn.execute("""
    INSERT INTO lookup_log (
        lookup_time,
        geozip,
        code,
        modifier,
        match_type,
        success
    )
    VALUES (?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        geozip,
        code,
        modifier,
        match_type,
        success
    ))
    conn.commit()


# -----------------------
# UI
# -----------------------
@app.get("/", response_class=HTMLResponse)
def serve_ui():
    if not UI_PATH.exists():
        raise HTTPException(status_code=500, detail="UI file not found")
    return UI_PATH.read_text()


# -----------------------
# Lookup API (corrected + hardened)
# -----------------------
@app.get("/lookup")
def lookup(
    geozip: int = Query(..., description="Geographic ZIP"),
    code: str = Query(..., description="Procedure code"),
    modifier: str | None = Query(default=None)
):
    conn = get_connection()
    ensure_log_table(conn)

    # Normalize inputs
    code = code.strip()
    modifier = modifier.strip() if modifier else None

    try:
        # 1. Modifier-specific lookup (only if modifier entered)
        if modifier:
            row = conn.execute(
                """
                SELECT *
                FROM allowed_amounts
                WHERE geozip = ?
                  AND code = ?
                  AND modifier = ?
                """,
                (geozip, code, modifier)
            ).fetchone()

            if row:
                result = dict(row)
                result["match_type"] = "Modifier-specific rate"
                log_lookup(conn, geozip, code, modifier, result["match_type"], 1)
                return result

        # 2. Base rate (normal path — no modifier)
        row = conn.execute(
            """
            SELECT *
            FROM allowed_amounts
            WHERE geozip = ?
              AND code = ?
              AND (modifier IS NULL OR modifier = '')
            """,
            (geozip, code)
        ).fetchone()

        if row:
            match_type = (
                "Base rate (no modifier)"
                if modifier is None
                else "Base rate (modifier not on file)"
            )
            result = dict(row)
            result["match_type"] = match_type
            log_lookup(conn, geozip, code, modifier, match_type, 1)
            return result

        # 3. No match found
        log_lookup(conn, geozip, code, modifier, "No match found", 0)
        raise HTTPException(
            status_code=404,
            detail=(
                f"No allowed amount found for GeoZip {geozip} "
                f"and Procedure Code {code}"
                + (f" with Modifier {modifier}" if modifier else "")
            )
        )

    finally:
        conn.close()

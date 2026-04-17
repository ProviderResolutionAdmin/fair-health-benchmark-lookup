from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pathlib import Path
import sqlite3
from datetime import datetime
from typing import List, Optional

app = FastAPI(title="Fair Health Benchmark Lookup")

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


def ensure_log_table(conn):
    # Keep this if you still want runtime logging (even if you removed GitHub export)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS lookup_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lookup_time TEXT NOT NULL,
        geozip INTEGER NOT NULL,
        code TEXT NOT NULL,
        modifier TEXT,
        product TEXT,
        match_type TEXT,
        success INTEGER NOT NULL
    )
    """)
    conn.commit()


def log_lookup(conn, geozip, code, modifier, product, match_type, success):
    conn.execute("""
    INSERT INTO lookup_log (
        lookup_time,
        geozip,
        code,
        modifier,
        product,
        match_type,
        success
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        geozip,
        code,
        modifier,
        product,
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
# Lookup API (supports multi-code + product filter)
# -----------------------
@app.get("/lookup")
def lookup(
    geozip: int = Query(..., description="Geographic ZIP"),
    code: List[str] = Query(..., description="One or more procedure codes (repeat code=...)"),
    modifier: Optional[str] = Query(default=None, description="Modifier (optional)"),
    product: Optional[str] = Query(default=None, description="Product filter (optional)")
):
    conn = get_connection()
    ensure_log_table(conn)

    modifier_clean = modifier.strip() if modifier else None
    product_clean = product.strip() if product else None

    results_out = []
    any_success = False

    try:
        for c in code:
            code_clean = c.strip()

            # --- helper: product WHERE clause ---
            product_clause = ""
            params_base = [geozip, code_clean]
            if product_clean:
                product_clause = " AND product = ?"
                params_base.append(product_clean)

            # 1) If modifier provided, fetch all modifier-specific rows (could be multiple products)
modifier_rows = []
if modifier_clean:
    modifier_params = [geozip, code_clean, modifier_clean]
    if product_clean:
        modifier_params.append(product_clean)

    modifier_rows = conn.execute(
        f"""
        SELECT *
        FROM allowed_amounts
        WHERE geozip = ?
          AND code = ?
          AND modifier = ?
          {product_clause}
        """,
        modifier_params
    ).fetchall()

            # Map modifier rows by product for easy comparison
            modifier_by_product = {}
            for r in modifier_rows:
                modifier_by_product[(r["product"] if "product" in r.keys() else None)] = dict(r)

            # 2) Fetch all base rows (no modifier) — again could be multiple products
            base_rows = conn.execute(
                f"""
                SELECT *
                FROM allowed_amounts
                WHERE geozip = ?
                  AND code = ?
                  AND (modifier IS NULL OR modifier = '')
                  {product_clause}
                """,
                params_base
            ).fetchall()

            base_by_product = {}
            for r in base_rows:
                base_by_product[(r["product"] if "product" in r.keys() else None)] = dict(r)

            # Determine all products present across both sets
            products_seen = set(base_by_product.keys()) | set(modifier_by_product.keys())

            # If nothing exists at all for this code
            if not products_seen:
                results_out.append({
                    "code": code_clean,
                    "product": product_clean or "",
                    "description": "",
                    "match_type": "No match found",
                    "50th": "", "60th": "", "70th": "", "75th": "",
                    "80th": "", "85th": "", "90th": "", "95th": ""
                })
                log_lookup(conn, geozip, code_clean, modifier_clean, product_clean, "No match found", 0)
                continue

            # For each product, choose modifier row if available; else base row
            for p in sorted(products_seen, key=lambda x: ("" if x is None else str(x))):
                if modifier_clean and p in modifier_by_product:
                    row = modifier_by_product[p]
                    row["match_type"] = "Modifier-specific rate"
                    results_out.append(row)
                    any_success = True
                    log_lookup(conn, geozip, code_clean, modifier_clean, row.get("product"), row["match_type"], 1)
                elif p in base_by_product:
                    row = base_by_product[p]
                    row["match_type"] = "Base rate (no modifier)" if not modifier_clean else "Base rate (modifier not on file)"
                    results_out.append(row)
                    any_success = True
                    log_lookup(conn, geozip, code_clean, modifier_clean, row.get("product"), row["match_type"], 1)
                else:
                    # Product exists in modifier rows but not in base rows (rare edge case)
                    row = modifier_by_product[p]
                    row["match_type"] = "Modifier-specific rate"
                    results_out.append(row)
                    any_success = True
                    log_lookup(conn, geozip, code_clean, modifier_clean, row.get("product"), row["match_type"], 1)

        # Always return 200 so multi-code lookups can show partial matches
        return results_out

    finally:
        conn.close()

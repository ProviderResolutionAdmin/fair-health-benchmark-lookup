from fastapi import FastAPI, HTTPException, Query
from app.db import get_connection

app = FastAPI(title="TX FH Allowed Medical Lookup")

@app.get("/")
def health_check():
    return {"status": "ok"}

@app.get("/lookup")
def lookup(
    geozip: int,
    code: int,
    modifier: int | None = Query(default=None)
):
    conn = get_connection()

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
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="No matching record found")

    return dict(row)


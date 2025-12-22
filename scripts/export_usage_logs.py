import sqlite3
import csv
from pathlib import Path
from datetime import date

DB_PATH = Path("data/allowed_amounts.sqlite")
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(exist_ok=True)

today = date.today().isoformat()
output_file = REPORTS_DIR / f"usage_{today}.csv"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

rows = cursor.execute("""
    SELECT
        lookup_time,
        geozip,
        code,
        modifier,
        match_type,
        success
    FROM lookup_log
    ORDER BY lookup_time DESC
""").fetchall()

headers = [
    "lookup_time",
    "geozip",
    "code",
    "modifier",
    "match_type",
    "success"
]

with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(headers)
    writer.writerows(rows)

conn.close()

print(f"Usage report written to {output_file}")

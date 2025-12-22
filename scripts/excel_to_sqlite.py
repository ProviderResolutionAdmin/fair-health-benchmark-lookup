import pandas as pd
import sqlite3
from pathlib import Path

SOURCE_FILE = Path("data/source/allowed_amounts_2025_06.xlsx")
DB_PATH = Path("data/allowed_amounts.sqlite")

df = pd.read_excel(SOURCE_FILE)

# Normalize column names
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("%", "th")
)

# Clean modifier column if present
if "modifier" in df.columns:
    df["modifier"] = df["modifier"].replace("", None)

conn = sqlite3.connect(DB_PATH)
df.to_sql("allowed_amounts", conn, if_exists="replace", index=False)

conn.execute("""
CREATE INDEX IF NOT EXISTS idx_allowed_lookup
ON allowed_amounts (geozip, code, modifier);
""")

conn.commit()
conn.close()

print("SQLite database created at", DB_PATH)


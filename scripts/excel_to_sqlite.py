import pandas as pd
import sqlite3
from pathlib import Path

SOURCE_FILE = Path("data/source/allowed_amounts_2025_06.xlsx")
DB_PATH = Path("data/allowed_amounts.sqlite")

# -----------------------
# Load Excel
# -----------------------
df = pd.read_excel(SOURCE_FILE)

# -----------------------
# Normalize column names
# -----------------------
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("%", "th")
)
# Normalize product column (Column A)
if "product" not in df.columns:
    raise ValueError("Expected 'product' column not found in Excel (Column A)")

df["product"] = (
    df["product"]
    .astype(str)
    .str.strip()
)
# Normalize description column
if "full_description" in df.columns:
    df = df.rename(columns={"full_description": "description"})
elif "procedure_description" in df.columns:
    df = df.rename(columns={"procedure_description": "description"})
elif "description" not in df.columns:
    raise ValueError("No recognizable description column found in Excel")
# -----------------------
# Normalize procedure code as TEXT
# -----------------------
if "code" not in df.columns:
    raise ValueError("Expected column 'code' not found in Excel")

df["code"] = (
    df["code"]
    .astype(str)
    .str.strip()
    .str.replace(r"\.0$", "", regex=True)
)

# -----------------------
# Normalize GeoZip as INTEGER
# -----------------------
if "geozip" not in df.columns:
    raise ValueError("Expected column 'geozip' not found in Excel")

df["geozip"] = df["geozip"].astype(int)

# -----------------------
# Normalize modifier as TEXT or NULL
# -----------------------
if "modifier" in df.columns:
    df["modifier"] = (
        df["modifier"]
        .astype(str)
        .str.strip()
        .replace({"": None, "nan": None, "NaN": None})
    )
else:
    df["modifier"] = None

# -----------------------
# Write SQLite
# -----------------------
conn = sqlite3.connect(DB_PATH)

df.to_sql(
    "allowed_amounts",
    conn,
    if_exists="replace",
    index=False
)

# -----------------------
# Index for lookups
# -----------------------
conn.execute("""
CREATE INDEX IF NOT EXISTS idx_allowed_lookup
ON allowed_amounts (geozip, code, modifier);
""")

conn.commit()
conn.close()

print("SQLite database created at", DB_PATH)


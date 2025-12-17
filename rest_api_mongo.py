# rest_to_mongo.py
import dlt
import pymongo
import pandas as pd
import requests
import zipfile
import os
from pathlib import Path

# -------------------------------------------------
# 1. Transformer: read the correct sheet and yield rows
# -------------------------------------------------
@dlt.transformer(standalone=True)
def read_excel(items):
    """Yield each row from the 'Online Retail' sheet."""
    import pandas as pd
    for file_item in items:
        with file_item.open() as f:
            # The real sheet name in this dataset is "Online Retail"
            df = pd.read_excel(f, sheet_name="Online Retail")
            for record in df.to_dict(orient="records"):
                yield record

# -------------------------------------------------
# 2. Download + extract the UCI Online Retail dataset
# -------------------------------------------------
URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
ZIP_PATH = "online_retail.zip"
XLSX_FILE = "online_retail.xlsx"

print("Downloading dataset...")
r = requests.get(URL, timeout=60)
r.raise_for_status()
with open(ZIP_PATH, "wb") as f:
    f.write(r.content)

print("Extracting Excel file...")
with zipfile.ZipFile(ZIP_PATH) as z:
    excel_name = [n for n in z.namelist() if n.lower().endswith(".xlsx")][0]
    with z.open(excel_name) as src, open(XLSX_FILE, "wb") as dst:
        dst.write(src.read())
os.remove(ZIP_PATH)

# -------------------------------------------------
# 3. Load into DuckDB (correct table name!)
# -------------------------------------------------
from dlt.sources.filesystem import filesystem

pipeline = dlt.pipeline(
    pipeline_name="online_retail_to_mongo",
    destination="duckdb",
    dataset_name="retail_data",
)

source = (
    filesystem(bucket_url=f"file://{Path('.').absolute()}", file_glob=XLSX_FILE)
    | read_excel()
).with_name("transactions")   # final table/collection name

print("Running dlt pipeline (≈20–40 seconds)...")
info = pipeline.run(source, write_disposition="replace")
print(info)

# -------------------------------------------------
# 4. Copy from DuckDB → MongoDB
# -------------------------------------------------
df = pipeline.dataset().table("transactions").df()
records = [{k: None if pd.isna(v) else v for k, v in row.items()}
           for row in df.to_dict("records")]

client = pymongo.MongoClient("mongodb://rootuser:rootpass@127.0.0.1:27017/")
db = client["my_api_db"]
coll = db["online_retail"]

coll.delete_many({})  # clean old data
result = coll.insert_many(records)
print(f"Inserted {len(result.inserted_ids):,} documents into MongoDB")

os.remove(XLSX_FILE)
client.close()
print("All done! 541,909 transactions are now in MongoDB.")

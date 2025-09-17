import pandas as pd
import requests, time, random, pathlib, hashlib
from urllib.parse import urlparse

INPUT = "Ov_data_centers.csv"
OUTPUT = "doc_fetch_log.csv"
BASEDIR = pathlib.Path("docs_raw")
BASEDIR.mkdir(exist_ok=True)

# Load Overton export
df = pd.read_csv(INPUT)
df.reset_index(inplace=True)   # ensures we have a numeric row index
df.rename(columns={'index': 'row_id'}, inplace=True)

# Add a simple 'state' column if you don't already have one
# For now this assumes you have 'State' column in export; otherwise adapt
df['state'] = df['State']

# Initialize or load log
if pathlib.Path(OUTPUT).exists():
    log = pd.read_csv(OUTPUT)
else:
    log = pd.DataFrame(columns=["row_id","doc_url","state","status","local_path","mime_type"])

done = set(log["doc_url"])
todo = df.loc[~df["Document URL"].isin(done)]

def fetch_doc(row, timeout=30):
    url = row["Document URL"]
    state = str(row["state"]) if pd.notnull(row["state"]) else "UNKNOWN"
    state_dir = BASEDIR / state
    state_dir.mkdir(exist_ok=True)

    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        mime = r.headers.get("Content-Type","unknown").split(";")[0]

        # Choose extension
        ext = {
            "application/pdf": ".pdf",
            "text/html": ".html",
            "application/msword": ".doc",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx"
        }.get(mime, ".bin")

        # Prepend row_id for traceability
        safe_name = f"{row['row_id']}{ext}"
        fpath = state_dir / safe_name
        fpath.write_bytes(r.content)

        return "ok", str(fpath), mime
    except requests.exceptions.Timeout:
        return "timeout", None, None
    except Exception as e:
        return f"error:{type(e).__name__}", None, None

SAMPLE_SIZE = 500

for i, row in todo.sample(n=min(SAMPLE_SIZE, len(todo)), random_state=42).iterrows():
    status, path, mime = fetch_doc(row)
    log = pd.concat([log, pd.DataFrame([{
        "row_id": row["row_id"],
        "doc_url": row["Document URL"],
        "state": row["state"],
        "status": status,
        "local_path": path,
        "mime_type": mime
    }])], ignore_index=True)
    print(f"[{i}] {row['Document URL']} -> {status} ({mime})")
    if i % 20 == 0:
        log.to_csv(OUTPUT, index=False)
    time.sleep(random.uniform(0.5,1.5))

log.to_csv(OUTPUT, index=False)

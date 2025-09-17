import pandas as pd
import requests, time, random, pathlib, hashlib

INPUT = "Ov_data_centers.csv"
OUTPUT = "doc_fetch_log.csv"
BASEDIR = pathlib.Path("docs_raw")
BASEDIR.mkdir(exist_ok=True)

# Load export
df = pd.read_csv(INPUT)
df.reset_index(inplace=True)          # create row_id
df.rename(columns={'index': 'row_id'}, inplace=True)

# Init log
if pathlib.Path(OUTPUT).exists():
    log = pd.read_csv(OUTPUT)
else:
    log = pd.DataFrame(columns=["row_id","doc_url","status","local_path","mime_type"])

done = set(log["doc_url"])
todo = df.loc[~df["Document URL"].isin(done)]

def safe_filename(row, ext):
    # hash from URL
    h = hashlib.sha1(row["Document URL"].encode("utf-8")).hexdigest()[:8]  # short hash
    return f"{row['row_id']}_{h}{ext}"

def fetch_doc(row, timeout=30):
    url = row["Document URL"]
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        mime = r.headers.get("Content-Type","unknown").split(";")[0]

        # Pick extension
        ext = {
            "application/pdf": ".pdf",
            "text/html": ".html",
            "application/msword": ".doc",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx"
        }.get(mime, ".bin")

        # Bucket by first two chars of hash
        hfull = hashlib.sha1(url.encode("utf-8")).hexdigest()
        subdir = BASEDIR / hfull[:2]
        subdir.mkdir(exist_ok=True)

        fname = safe_filename(row, ext)
        fpath = subdir / fname
        fpath.write_bytes(r.content)

        return "ok", str(fpath), mime
    except requests.exceptions.Timeout:
        return "timeout", None, None
    except Exception as e:
        return f"error:{type(e).__name__}", None, None

SAMPLE_SIZE = 500  # or len(todo) for full crawl

for i, row in todo.sample(n=min(SAMPLE_SIZE, len(todo)), random_state=42).iterrows():
    status, path, mime = fetch_doc(row)
    log = pd.concat([log, pd.DataFrame([{
        "row_id": row["row_id"],
        "doc_url": row["Document URL"],
        "status": status,
        "local_path": path,
        "mime_type": mime
    }])], ignore_index=True)

    print(f"[{row['row_id']}] {row['Document URL']} -> {status} ({mime})")

    if i % 20 == 0:
        log.to_csv(OUTPUT, index=False)
    time.sleep(random.uniform(0.5,1.5))


log.to_csv(OUTPUT, index=False)

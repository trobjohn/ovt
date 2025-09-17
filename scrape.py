# %%

import pandas as pd
import requests
import time, random, pathlib

# Paths
INPUT = "Ov_data_centers.csv"
OUTPUT = "doc_fetch_log.csv"
TEXTDIR = pathlib.Path("docs_raw")
TEXTDIR.mkdir(exist_ok=True)

# Load your export
df = pd.read_csv(INPUT)
df.head()

# %%


# Initialize log
if pathlib.Path(OUTPUT).exists():
    log = pd.read_csv(OUTPUT)
else:
    log = pd.DataFrame(columns=["doc_url", "status", "local_path"])

# Figure out which URLs are unfinished
done = set(log["doc_url"])
todo = [u for u in df["Document URL"].dropna().unique() if u not in done]

# Randomize order
random.shuffle(todo)

# Downloader
def fetch_doc(url, timeout=30):
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        suffix = pathlib.Path(url).suffix or ".html"
        fname = f"{hash(url)}{suffix}"
        fpath = TEXTDIR / fname
        fpath.write_bytes(r.content)
        return "ok", str(fpath)
    except requests.exceptions.Timeout:
        return "timeout", None
    except Exception as e:
        return f"error: {type(e).__name__}", None

# Main loop (sample or full crawl)
SAMPLE_SIZE = 200  # adjust as needed
for i, url in enumerate(todo[:SAMPLE_SIZE], 1):
    status, path = fetch_doc(url)
    log = pd.concat([log, pd.DataFrame([{
        "doc_url": url,
        "status": status,
        "local_path": path
    }])], ignore_index=True)
    print(f"[{i}/{SAMPLE_SIZE}] {url} -> {status}")
    if i % 20 == 0:  # save progress every 20 docs
        log.to_csv(OUTPUT, index=False)
    time.sleep(random.uniform(1, 2))  # polite delay

# Final save
log.to_csv(OUTPUT, index=False)

# %%

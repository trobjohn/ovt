# %%

import requests, time, pathlib, json
import pandas as pd

API_KEY = "xx"
BASE_URL = "https://app.overton.io/documents.php"

RAW_DIR = pathlib.Path("overton_pages")
RAW_DIR.mkdir(exist_ok=True)

OUT_PARQUET = "overton_index.parquet"
OUT_JSONL = "overton_full.jsonl"   # line-delimited full JSON records

def fetch_page(page, query='"data center"'):
    params = {
        "query": query,
        "sort": "relevance",
        "format": "json",
        "page": page,
        "api_key": API_KEY
    }
    r = requests.get(BASE_URL, params=params)
    
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} on page {page}: {r.text[:200]}")
    
    try:
        return r.json()
    except Exception:
        # dump the first part of the response to debug
        raise RuntimeError(f"Failed to parse JSON on page {page}. First 200 chars:\n{r.text[:200]}")


N_PAGES = 1000
all_records = []
jsonl_file = open(OUT_JSONL, "a") if pathlib.Path(OUT_JSONL).exists() else open(OUT_JSONL, "w")

for page in range(1, N_PAGES + 1):
    out_file = RAW_DIR / f"page_{page:04d}.json"
    if out_file.exists():
        print(f"Skipping page {page}, cached")
        with open(out_file) as f:
            data = json.load(f)
    else:
        print(f"Fetching page {page}/{N_PAGES}")
        data = fetch_page(page)
        with open(out_file, "w") as f:
            json.dump(data, f)
        time.sleep(0.5)

    for item in data.get("results", []):
        # Write raw JSON line (preserves all fields)
        jsonl_file.write(json.dumps(item) + "\n")

        # Minimal index fields for fast lookups
        rec = {
            "policy_document_id": item.get("policy_document_id"),
            "title": item.get("title"),
            "state": item.get("source", {}).get("state"),
            "country": item.get("source", {}).get("country"),
            "published_on": item.get("published_on"),
            "document_url": item.get("document_url"),
            "overton_url": item.get("overton_url"),
        }
        all_records.append(rec)

jsonl_file.close()

# Save flattened index for pandas merges
df = pd.DataFrame(all_records)
df.to_parquet(OUT_PARQUET, engine='fastparquet', index=False)

print(f"Saved {len(df)} index rows to {OUT_PARQUET} and full JSON to {OUT_JSONL}")

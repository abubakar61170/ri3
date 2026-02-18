import json
import re
import time
from pathlib import Path
from typing import Dict, Any, Iterable, Set
import os
from dotenv import load_dotenv
import requests
from tqdm import tqdm
load_dotenv()

DBLP_DIR = Path(os.environ["DBLP_DIR"])
if not DBLP_DIR:
    raise ValueError("DBLP_DIR not set in .env file!")

RAW_DIR = DBLP_DIR / "raw"
INTERIM_DIR = DBLP_DIR / "interim"

DATASETS = {
  "jocs": {
    "query": "stream:streams/journals/jocs:",
    "raw_path": RAW_DIR / "jocs" / "jocs_dblp.jsonl",
    "doi_path": INTERIM_DIR / "jocs" / "jocs_dblp_dois.jsonl",
  },
  "iccs": {
    "query": "stream:streams/conf/iccS:",
    "raw_path": RAW_DIR / "iccs" / "iccs_dblp.jsonl",
    "doi_path": INTERIM_DIR / "iccs" / "iccs_dblp_dois.jsonl",
  },
}


BASE_URL = "https://dblp.org/search/publ/api"
PAGE_SIZE = 1000
SLEEP_SEC = 0.3
RESUME = True


def normalize_doi(doi: str) -> str:
    doi = doi.strip().lower()
    doi = re.sub(r"^https?://(dx\.)?doi\.org/", "", doi)
    doi = re.sub(r"^doi:\s*", "", doi)
    doi = doi.rstrip(" .;")
    return doi

def fetch_page(query: str, offset: int) -> Dict[str, Any]:
    params = {"q": query, "h": PAGE_SIZE, "f": offset, "format": "json"}
    r = requests.get(BASE_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def get_total_hits(query: str) -> int:
    first = fetch_page(query, 0)
    return int(first["result"]["hits"]["@total"])

def iter_hits(query: str) -> Iterable[Dict[str, Any]]:
    offset = 0
    while True:
        data = fetch_page(query, offset)
        hits = data.get("result", {}).get("hits", {}).get("hit", [])
        if not hits:
            break
        for h in hits:
            yield h.get("info", {})
        offset += PAGE_SIZE
        time.sleep(SLEEP_SEC)

def load_seen(path: Path, field: str) -> Set[str]:
    seen = set()
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                rec = json.loads(line)
                v = rec.get(field)
                if v:
                    seen.add(v)
    return seen

def download_dblp(query: str, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    seen = load_seen(out_path, "key") if RESUME else set()
    total = get_total_hits(query)

    mode = "a" if RESUME else "w"
    written = skipped = 0

    with out_path.open(mode, encoding="utf-8") as f:
        for rec in tqdm(iter_hits(query), total=total):
            k = rec.get("key")
            if RESUME and k in seen:
                skipped += 1
                continue
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            if k:
                seen.add(k)
            written += 1

    print(f"[DBLP] {out_path} | written={written}, skipped={skipped}")

def normalize_dois(in_path: Path, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    seen = load_seen(out_path, "doi_normalized") if RESUME else set()

    mode = "a" if RESUME else "w"
    written = skipped = missing = 0

    with in_path.open("r", encoding="utf-8") as f_in, out_path.open(mode, encoding="utf-8") as f_out:
        for line in f_in:
            rec = json.loads(line)
            raw = rec.get("doi")
            if not raw:
                missing += 1
                continue

            doi = normalize_doi(raw)
            if not doi or (RESUME and doi in seen):
                skipped += 1
                continue

            rec["doi_normalized"] = doi
            f_out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            seen.add(doi)
            written += 1

    print(f"[DOI]  {out_path} | written={written}, missing={missing}, skipped={skipped}")

def validate(path: Path):
    n = n_bad = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            doi = rec.get("doi_normalized")
            n += 1
            if not doi or doi != doi.lower() or doi.startswith("http"):
                n_bad += 1
    print(f"[CHECK] {path} | total={n}, problematic={n_bad}")



def main():
    for name, cfg in DATASETS.items():
        print("\n" + "=" * 70)
        print(f"PROCESSING: {name.upper()}")

        download_dblp(cfg["query"], cfg["raw_path"])
        normalize_dois(cfg["raw_path"], cfg["doi_path"])
        validate(cfg["doi_path"])
        
if __name__ == "__main__":
    main()
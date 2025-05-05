#!/usr/bin/env python3
"""
fetch_data_gov.py 

```bash
python fetch_data_gov.py -o ./downloads \
       --rows 150 --search-fallback --any-format
````
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
DEFAULT_API_KEY = os.getenv("DATA_GOV_API_KEY")
API_URL = "https://catalog.data.gov/api/3/action/package_search"

DEFAULT_CATEGORIES: Dict[str, str] = {
    "Energy": "energy",
    "Finance & Economics": "finance",
    "Demographics & Housing": "demographics",
}
PREFERRED_FORMATS: List[str] = ["CSV", "JSON", "XLSX", "XLS", "GEOJSON", "ZIP"]
FALLBACK_EXTENSIONS = (
    ".csv", ".json", ".xls", ".xlsx", ".zip", ".geojson", ".xml", ".txt", ".parquet", ".gz",
)
CHUNK = 8192


def simple_slugify(text: str) -> str:
    import unicodedata

    text = (
        unicodedata.normalize("NFKD", str(text))
        .encode("ascii", "ignore")
        .decode()
    )
    text = re.sub(r"[^\w\s-]", "", text.lower()).strip()
    return re.sub(r"[-\s]+", "-", text)


def ckan_request(params: dict, api_key: str | None) -> dict:
    headers = {"X-Api-Key": api_key} if api_key else {}
    resp = requests.get(API_URL, headers=headers, params=params, timeout=25)
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise RuntimeError(payload)
    return payload["result"]


def select_resource(dataset: dict, any_format: bool) -> Optional[dict]:
    resources = dataset.get("resources", [])
    for fmt in PREFERRED_FORMATS:
        hit = next((r for r in resources if r.get("format", "").upper() == fmt), None)
        if hit:
            return hit
    if any_format:
        for r in resources:
            url = r.get("url", "")
            if any(url.lower().endswith(ext) for ext in FALLBACK_EXTENSIONS):
                return r
    return None


def find_dataset(group_alias: str, api_key: str, rows: int, any_format: bool, search_fallback: bool) -> Optional[dict]:
    params = {"fq": f"group:{group_alias}", "rows": rows, "sort": "metadata_created desc"}
    results = ckan_request(params, api_key).get("results", [])
    for ds in results:
        res = select_resource(ds, any_format)
        if res:
            return {"dataset": ds, "resource": res}

    if not search_fallback:
        return None

    params = {"q": group_alias, "rows": rows, "sort": "metadata_created desc"}
    results = ckan_request(params, api_key).get("results", [])
    for ds in results:
        res = select_resource(ds, any_format)
        if res:
            return {"dataset": ds, "resource": res}

    return None


def stream_download(url: str, dest: Path):
    """Stream *url* into *dest* with a tqdm progress bar."""
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0)) or None 
        desc = (dest.name[:40] + "…") if len(dest.name) > 43 else dest.name
        with tqdm.wrapattr(r.raw, "read", total=total, desc=desc) as raw, open(dest, "wb") as fh:
            for chunk in iter(lambda: raw.read(CHUNK), b""):
                fh.write(chunk)

def main():
    parser = argparse.ArgumentParser(description="Download newest data files from Data.gov topic groups.")
    parser.add_argument("--topics", default=", ".join(DEFAULT_CATEGORIES.keys()), help="Comma‑separated list of topic labels")
    parser.add_argument("-o", "--output", default="./downloads", help="Destination folder")
    parser.add_argument("--api-key", default=DEFAULT_API_KEY, help="Data.gov API key")
    parser.add_argument("--rows", type=int, default=100, help="How many records to scan per query")
    parser.add_argument("--any-format", action="store_true", help="Accept first resource with data‑like extension if nothing matches preferred list")
    parser.add_argument("--search-fallback", action="store_true", help="After group search, retry plain keyword search")
    parser.add_argument("--max-size-mb", type=int, default=500, help="Skip files larger than this size (MB)")
    args = parser.parse_args()

    out_dir = Path(args.output).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nFetching datasets – {datetime.utcnow().isoformat(timespec='seconds')}Z\n")

    for label in [t.strip() for t in args.topics.split(",") if t.strip()]:
        alias = DEFAULT_CATEGORIES.get(label) or simple_slugify(label)
        rec = find_dataset(alias, args.api_key, args.rows, args.any_format, args.search_fallback)
        if not rec:
            print(f"🗂  {label}\n    No downloadable resource found.")
            continue

        ds, res = rec["dataset"], rec["resource"]
        title = ds.get("title", "[No title]")
        created = ds.get("metadata_created", "N/A")
        org = ds.get("organization", {}).get("title", "Unknown org")
        url = f"https://catalog.data.gov/dataset/{ds.get('name', '')}"
        res_url = res.get("url")
        fmt = res.get("format", "?").upper()
        size_mb = int(res.get("size", 0)) / (1024 * 1024) if res.get("size") else None
        if size_mb and size_mb > args.max_size_mb:
            print(f"🗂  {label}\n    Resource {size_mb:.0f} MB > limit – skipped (URL: {res_url})")
            continue

        filename = simple_slugify(f"{label}-{ds.get('name')}-{res.get('id')}")
        ext = os.path.splitext(res_url)[1] or f".{fmt.lower()}"
        dest = out_dir / f"{filename}{ext}"

        try:
            stream_download(res_url, dest)
            status = f"    ✓ Downloaded → {dest}"
        except Exception as exc:
            status = f"    ⚠️  Download failed: {exc} (URL: {res_url})"

        print(
            f"\n🗂  {label}\n    Title : {title}\n    Added : {created}\n    Org   : {org}\n    File  : {fmt}\n    URL   : {url}\n{status}")


if __name__ == "__main__":
    main()

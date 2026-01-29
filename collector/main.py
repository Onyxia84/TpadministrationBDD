import argparse
import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError, PyMongoError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------------------------
# Utils: hashing / canonical JSON
# -------------------------
def canonical_json(obj: Any) -> str:
    # JSON stable: keys sorted, no spaces
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# -------------------------
# HTTP client with retries
# -------------------------
def build_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent})

    # Retry policy: timeouts/5xx/429
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def off_search(
    session: requests.Session,
    base_url: str,
    page: int,
    page_size: int,
    category_en: Optional[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Calls OpenFoodFacts search v2 endpoint.
    Returns (products, full_response_json).
    """
    url = base_url.rstrip("/") + "/api/v2/search"

    params = {
        "page": page,
        "page_size": page_size,
        # Optional sort for stable results
        "sort_by": "last_modified_t",
        # IMPORTANT: do NOT set fields=... if you want full payloads
    }
    if category_en:
        # Example: "Snacks", "Beverages", "Breakfasts"
        params["categories_tags_en"] = category_en

    resp = session.get(url, params=params, timeout=(5, 15))
    # If still bad after retries:
    if resp.status_code != 200:
        raise RuntimeError(f"OFF HTTP {resp.status_code}: {resp.text[:200]}")

    data = resp.json()
    products = data.get("products")
    if not isinstance(products, list):
        raise RuntimeError("OFF response missing 'products' list")

    return products, data

# -------------------------
# Mongo RAW repository
# -------------------------
def ensure_indexes(raw_col: Collection) -> None:
    # Unique anti-duplicate key
    raw_col.create_index("raw_hash", unique=True)
    # Useful indexes (optional)
    raw_col.create_index("source")
    raw_col.create_index("fetched_at")
    # If payload has a "code", index it (may help later)
    raw_col.create_index("payload.code")

def insert_raw(raw_col: Collection, source: str, payload: Dict[str, Any]) -> bool:
    """
    Inserts a RAW document. Returns True if inserted, False if duplicate.
    """
    raw_json = canonical_json(payload)
    raw_hash = sha256_hex(raw_json)

    doc = {
        "source": source,
        "fetched_at": utc_now_iso(),
        "raw_hash": raw_hash,
        "payload": payload,  # EXACT payload as received
    }

    try:
        raw_col.insert_one(doc)
        return True
    except DuplicateKeyError:
        return False

# -------------------------
# Main collector logic
# -------------------------
def main() -> int:
    load_dotenv()

    import os

    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGO_DB", "food_pipeline")
    mongo_raw_collection = os.getenv("MONGO_RAW_COLLECTION", "raw_products")

    off_base_url = os.getenv("OFF_BASE_URL", "https://world.openfoodfacts.org")
    off_user_agent = os.getenv("OFF_USER_AGENT", "").strip()
    if not off_user_agent:
        print("ERROR: OFF_USER_AGENT is required in .env", file=sys.stderr)
        return 2

    parser = argparse.ArgumentParser(description="OpenFoodFacts -> Mongo RAW collector")
    parser.add_argument("--limit", type=int, default=300, help="Minimum number of RAW docs to insert")
    parser.add_argument("--page-size", type=int, default=100, help="OFF page size (suggest 100)")
    parser.add_argument("--category-en", type=str, default=None, help='Example: "Snacks", "Beverages", "Breakfasts"')
    parser.add_argument("--sleep-ms", type=int, default=150, help="Sleep between page requests (polite)")
    parser.add_argument("--max-pages", type=int, default=50, help="Safety stop")
    args = parser.parse_args()

    # Mongo connect
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    raw_col = db[mongo_raw_collection]

    # Indexes
    ensure_indexes(raw_col)

    session = build_session(off_user_agent)

    inserted = 0
    duplicates = 0
    requested = 0
    failed_pages = 0

    start = time.time()
    page = 1

    print(f"Collector started: limit={args.limit}, page_size={args.page_size}, category_en={args.category_en}")
    print(f"Mongo: {mongo_uri} / db={mongo_db} / collection={mongo_raw_collection}")
    print(f"OFF: {off_base_url} / endpoint=/api/v2/search")

    while inserted < args.limit and page <= args.max_pages:
        try:
            products, _full = off_search(
                session=session,
                base_url=off_base_url,
                page=page,
                page_size=args.page_size,
                category_en=args.category_en,
            )
        except Exception as e:
            failed_pages += 1
            print(f"[PAGE {page}] ERROR: {e}")
            page += 1
            time.sleep(args.sleep_ms / 1000.0)
            continue

        if not products:
            print(f"[PAGE {page}] No products returned -> stopping.")
            break

        print(f"[PAGE {page}] received {len(products)} products")
        for p in products:
            requested += 1
            if not isinstance(p, dict):
                print("  - skip: product is not an object")
                continue

            try:
                ok = insert_raw(raw_col, "openfoodfacts", p)
                if ok:
                    inserted += 1
                else:
                    duplicates += 1
            except PyMongoError as me:
                # Mongo error shouldn't crash the whole run; log and continue
                print(f"  - mongo error: {me}")
                continue

            if inserted >= args.limit:
                break

        page += 1
        time.sleep(args.sleep_ms / 1000.0)

    duration = time.time() - start
    print("\n=== SUMMARY ===")
    print(f"requested_products: {requested}")
    print(f"inserted:          {inserted}")
    print(f"duplicates:        {duplicates}")
    print(f"failed_pages:      {failed_pages}")
    print(f"duration_sec:      {duration:.2f}")

    if inserted >= args.limit:
        print("DONE: minimum inserted reached.")
        return 0
    else:
        print("STOPPED: did not reach limit (increase max-pages or change category).")
        return 1

if __name__ == "__main__":
    raise SystemExit(main())

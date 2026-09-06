import os
import json
import requests
import pyarrow.parquet as pq
import io
from fastapi import FastAPI, Query, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException as FastAPIHTTPException
from starlette.responses import JSONResponse
from datetime import datetime, timedelta

# ── Config ──────────────────────────────────────────────
API_KEY = "psychoxd"  # 🔥 Hardcoded
DEVELOPER = "@psychopathmc"
SUPPORT_MSG = "For API purchase, contact @psychopathmc"
BASE_URL = "https://huggingface.co/datasets/Kzr0xx/icrm-hitek-full-db-mixed/resolve/main"  # 🔥 FIXED

app = FastAPI(title="PsychopathMC OSINT API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Custom Exception Handler (adds support in errors) ──
@app.exception_handler(FastAPIHTTPException)
async def custom_http_exception_handler(request: Request, exc: FastAPIHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "developer": DEVELOPER,
            "support": SUPPORT_MSG,
        }
    )

# ── Simple Cache (speed ke liye) ────────────────────────
_cache = {}
_cache_time = {}
CACHE_TTL = 300  # 5 minutes

def get_cache(url, column, value):
    key = f"{url}|{column}|{value}"
    if key in _cache and (datetime.now() - _cache_time[key]).seconds < CACHE_TTL:
        return _cache[key]
    return None

def set_cache(url, column, value, data):
    key = f"{url}|{column}|{value}"
    _cache[key] = data
    _cache_time[key] = datetime.now()
    if len(_cache) > 100:
        oldest = min(_cache_time, key=_cache_time.get)
        del _cache[oldest]
        del _cache_time[oldest]

# ── Helper: fetch & filter Parquet (optimized) ─────────
def fetch_filter(url: str, column: str, value: str, limit: int = 5):
    # Check cache first
    cached = get_cache(url, column, value)
    if cached is not None:
        return cached

    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return []
        # 🔥 Selective columns – sirf jaruri fields (speed improve)
        needed_cols = ["name", "fathersName", "phoneNumber", "aadharNumber", "otherNumber", "address"]
        table = pq.read_table(io.BytesIO(resp.content), columns=needed_cols)
        df = table.to_pandas()
        if column not in df.columns:
            return []
        filtered = df[df[column] == value]
        results = filtered.head(limit).to_dict(orient="records")
        set_cache(url, column, value, results)
        return results
    except Exception as e:
        print(f"Error in fetch_filter: {e}")
        return []

# ── Endpoints ───────────────────────────────────────────
@app.get("/")
def root():
    return {
        "app": "PsychopathMC OSINT API",
        "records": 2_504_793_870,
        "indexes": {"phone": True, "aadhar": True},
        "index_source": "remote",
        "columns": ["name", "fathersName", "phoneNumber", "aadharNumber", "otherNumber", "address", "district", "pincode", "state", "town", "source"],
        "docs": "/docs",
        "developer": DEVELOPER,
        "support": SUPPORT_MSG,
    }

@app.get("/health")
def health():
    return {"status": "ok", "developer": DEVELOPER, "support": SUPPORT_MSG}

@app.get("/search")
def search(
    q: str | None = Query(None),
    mobile: str | None = Query(None),
    key: str = Query(None),  # Optional, but checked
    limit: int = Query(5, ge=1, le=20),
):
    # 🔥 Key check
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    query = (q or mobile or "").strip()
    if not query:
        raise HTTPException(422, "Provide q or mobile")

    last_digit = query[-1]
    shard = int(last_digit) % 7

    phone_url = f"{BASE_URL}/idx_phone.{shard}.parquet"
    results = fetch_filter(phone_url, "phoneNumber", query, limit)

    if not results:
        aadhar_url = f"{BASE_URL}/idx_aadhar.{shard}.parquet"
        results = fetch_filter(aadhar_url, "aadharNumber", query, limit)

    # Deduplicate by aadhar (optional)
    seen = set()
    unique_results = []
    for row in results:
        aadhar_val = row.get("aadharNumber")
        if aadhar_val not in seen:
            seen.add(aadhar_val)
            unique_results.append(row)
    results = unique_results

    return {
        "success": len(results) > 0,
        "query": query,
        "count": len(results),
        "results": results,
        "developer": DEVELOPER,
        "support": SUPPORT_MSG,
    }

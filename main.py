import os
import asyncio
import httpx
import pyarrow.parquet as pq
import io
from datetime import datetime, timedelta
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException as FastAPIHTTPException
from starlette.responses import JSONResponse

# ── CONFIG ──────────────────────────────────────────────
API_KEY = os.environ.get("API_KEY", "psychoxd")
DEVELOPER = "@psychopathmc"
SUPPORT_MSG = "For API purchase, contact @psychopathmc"  # 🔥 Support message
BASE_URL = "https://huggingface.co/datasets/Kzr0xx/icrm-hitek-full-db-mixed/resolve/main"
CACHE_TTL = 300
SESSION_TIMEOUT = 10

app = FastAPI(title="PsychopathMC OSINT API", version="9.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(FastAPIHTTPException)
async def custom_http_exception_handler(request: Request, exc: FastAPIHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "developer": DEVELOPER,
            "support": SUPPORT_MSG,  # 🔥 Support added in error too
        }
    )

# ── Helper: Circle / Operator Lookup ────────────────────
def get_circle(num: str) -> str:
    prefixes = {
        "9810": "AIRTEL DELHI", "9871": "AIRTEL DELHI", "9818": "AIRTEL DELHI",
        "9910": "VI DELHI", "8826": "JIO DELHI", "9999": "AIRTEL DELHI",
        "9971": "AIRTEL DELHI", "9883": "JIO WB", "9564": "JIO WB",
    }
    pref = num[:4]
    return prefixes.get(pref, "UNKNOWN CIRCLE")

# ── Async HTTP Client ──────────────────────────────────
_client = None

async def get_client():
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(SESSION_TIMEOUT),
            limits=httpx.Limits(max_keepalive_connections=5),
            headers={"User-Agent": "Mozilla/5.0"}
        )
    return _client

# ── Cache ────────────────────────────────────────────────
_cache = {}
_cache_timestamps = {}

async def get_cache(url: str, column: str, value: str):
    key = f"{url}|{column}|{value}"
    if key in _cache and (datetime.now() - _cache_timestamps[key]).seconds < CACHE_TTL:
        return _cache[key]
    return None

async def set_cache(url: str, column: str, value: str, data):
    key = f"{url}|{column}|{value}"
    _cache[key] = data
    _cache_timestamps[key] = datetime.now()
    if len(_cache) > 100:
        oldest = min(_cache_timestamps, key=_cache_timestamps.get)
        del _cache[oldest]
        del _cache_timestamps[oldest]

# ── Fetch Logic ──────────────────────────────────────────
async def fetch_data(url: str, column: str, value: str, limit: int = 15):
    cached = await get_cache(url, column, value)
    if cached is not None:
        return cached

    try:
        client = await get_client()
        resp = await client.get(url)
        if resp.status_code != 200:
            return []
        
        needed_cols = ["name", "fathersName", "phoneNumber", "aadharNumber", "otherNumber", "address"]
        table = pq.read_table(io.BytesIO(resp.content), columns=needed_cols)
        df = table.to_pandas()
        
        if column not in df.columns:
            return []
        filtered = df[df[column] == value]
        results = filtered.head(limit).to_dict(orient="records")
        await set_cache(url, column, value, results)
        return results
    except Exception as e:
        print(f"Fetch error: {e}")
        return []

# ── Endpoints ────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "message": "PsychoAPI is live. Use /psychoapi?Number=XXX&key=psychoxd",
        "developer": DEVELOPER,
        "support": SUPPORT_MSG,  # 🔥 Support added
    }

@app.get("/psychoapi")
async def psychoapi(
    Number: str = Query(..., description="Phone number"),
    key: str = Query(..., description="API Key"),
    limit: int = Query(15, ge=1, le=50)
):
    # 1. Key Check
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    # 2. Validate Number
    if not Number.isdigit():
        raise HTTPException(status_code=400, detail="Invalid number format")

    # 3. Determine shard (0-6)
    last_digit = Number[-1]
    shard = int(last_digit) % 7

    # 4. Search Phone Index
    phone_url = f"{BASE_URL}/idx_phone.{shard}.parquet"
    results = await fetch_data(phone_url, "phoneNumber", Number, limit)

    # 5. If no result, search Aadhar Index (fallback)
    if not results:
        aadhar_url = f"{BASE_URL}/idx_aadhar.{shard}.parquet"
        results = await fetch_data(aadhar_url, "aadharNumber", Number, limit)

    # 6. Deduplicate results (remove exact duplicates based on aadhar)
    seen = set()
    unique_results = []
    for row in results:
        aadhar_val = row.get("aadharNumber")
        if aadhar_val not in seen:
            seen.add(aadhar_val)
            unique_results.append(row)
    results = unique_results

    # 7. Format response
    formatted = []
    for row in results:
        formatted.append({
            "num": row.get("phoneNumber"),
            "name": row.get("name"),
            "fname": row.get("fathersName"),
            "aadhar": row.get("aadharNumber"),
            "address": row.get("address"),
            "circle": get_circle(Number),
            "email": None,
            "alt": row.get("otherNumber"),
        })

    # 8. Final Response Wrapper
    return {
        "response": {
            "parameters": {
                "value": Number,
                "service": "phone number info",
                "success": len(formatted) > 0
            },
            "data": formatted
        },
        "developer": DEVELOPER,
        "support": SUPPORT_MSG,  # 🔥 Support added here too
    }

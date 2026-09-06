import os
import duckdb
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import HTTPException as FastAPIHTTPException
from starlette.responses import JSONResponse

# ── CONFIG ──────────────────────────────────────────────
API_KEY = os.environ.get("API_KEY", "psychoxd")
DEVELOPER = "@psychopathmc"
SUPPORT = "Discord: psychopathmc"
BASE_URL = ""

app = FastAPI(title="PsychopathMC OSINT API", version="3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Custom Exception Handler ────────────────────────────
@app.exception_handler(FastAPIHTTPException)
async def custom_http_exception_handler(request: Request, exc: FastAPIHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "developer": DEVELOPER,
            "support": SUPPORT,
        }
    )

# ── DuckDB Connection (Reused for speed) ────────────────
_conn = None

def get_conn():
    global _conn
    if _conn is None:
        _conn = duckdb.connect()
        # Vercel fix: /tmp is writable
        _conn.execute("SET home_directory='/tmp'")
        _conn.execute("SET extension_directory='/tmp/duckdb_extensions'")
        _conn.execute("INSTALL httpfs; LOAD httpfs;")
        # Optional: set threads for parallel processing
        _conn.execute("SET threads = 2;")
    return _conn

# ── Endpoints ────────────────────────────────────────────
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
        "support": SUPPORT,
    }

@app.get("/health")
def health():
    return {"status": "ok", "developer": DEVELOPER, "support": SUPPORT}

@app.get("/search")
def search(
    q: str | None = Query(None),
    mobile: str | None = Query(None),
    key: str = Query(..., description="API Key required"),
    limit: int = Query(5, ge=1, le=20),
):
    # 1. API Key Check
    if key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    query = (q or mobile or "").strip()
    if not query:
        raise HTTPException(status_code=422, detail="Provide q or mobile")

    # 2. Determine shard (0-6)
    last_digit = query[-1]
    shard = int(last_digit) % 7

    con = get_conn()

    # 3. 🔥 FAST QUERY: DuckDB pushes the WHERE clause to the remote file!
    #    It ONLY downloads the matching row groups, not the whole file.
    phone_url = f"{BASE_URL}/idx_phone.{shard}.parquet"
    sql_phone = f"SELECT * FROM read_parquet('{phone_url}') WHERE phoneNumber = '{query}' LIMIT {limit}"
    
    try:
        results = con.execute(sql_phone).fetchall()
        cols = [desc[0] for desc in con.description]
        main_records = [dict(zip(cols, row)) for row in results]
    except Exception as e:
        main_records = []

    # 4. If no phone, try Aadhar
    if not main_records:
        aadhar_url = f"{BASE_URL}/idx_aadhar.{shard}.parquet"
        sql_aadhar = f"SELECT * FROM read_parquet('{aadhar_url}') WHERE aadharNumber = '{query}' LIMIT {limit}"
        try:
            results = con.execute(sql_aadhar).fetchall()
            cols = [desc[0] for desc in con.description]
            main_records = [dict(zip(cols, row)) for row in results]
        except Exception as e:
            main_records = []

    # 5. Response
    return {
        "success": len(main_records) > 0,
        "query": query,
        "count": len(main_records),
        "results": main_records,
        "developer": DEVELOPER,
        "support": SUPPORT,
    }

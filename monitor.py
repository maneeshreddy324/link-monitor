import os
import io
import csv
import time
import hashlib
import logging
import threading
import xml.etree.ElementTree as ET

from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone

import requests
import redis
import openpyxl

from bs4 import BeautifulSoup
from pypdf import PdfReader
from flask import Flask, jsonify

# Cloudflare Bypass Extension: Adds standard browser TLS fingerprints
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

class TLSAdapter(HTTPAdapter):
    """Forces the network engine to execute real web browser SSL/TLS handshakes."""
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        context.load_default_certs()
        kwargs['ssl_context'] = context
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

# ============================================================
# 1. CONFIGURATION
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LINKS_FILE = os.path.join(BASE_DIR, "links.txt")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
REDIS_URL = os.getenv("REDIS_URL", "").strip()

CHECK_INTERVAL = max(60, int(os.getenv("CHECK_INTERVAL_SECONDS", "300")))

REQUEST_TIMEOUT = 25
MAX_ITEMS_PER_SOURCE = 100
MAX_ALERTS_PER_SOURCE = 30
FINGERPRINT_TTL = 30 * 24 * 60 * 60

SAFETY_FILTER = os.getenv("SAFETY_FILTER", "true").lower() == "true"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("aviation-monitor")

app = Flask(__name__)

# Configure an advanced browser session profile to unlock Cloudflare links
session = requests.Session()
session.mount("https://", TLSAdapter())
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
})

# ============================================================
# 2. AVIATION SAFETY KEYWORDS
# ============================================================

SAFETY_TERMS = [
    "aircraft accident", "aviation accident", "plane crash", "airplane crash",
    "aircraft crash", "emergency landing", "forced landing", "mayday",
    "aircraft incident", "aviation incident", "engine failure", "engine fire",
    "engine shutdown", "bird strike", "runway excursion", "runway incursion",
    "hard landing", "emergency evacuation", "smoke in cabin", "fire on board",
    "loss of control", "loss of cabin pressure", "cabin depressurization",
    "flight control failure", "structural failure", "landing gear failure",
    "near collision", "midair collision", "turbulence injuries", "safety investigation",
    "airworthiness directive", "safety warning", "faa safety alert", "emergency declaration",
    "aborted takeoff", "diverted due to", "hydraulic failure", "fuel leak",
    "windshield cracked", "flight returned", "aircraft missing", "aircraft disappeared",
    "black box", "fatal crash", "fatal accident", "aircraft fire", "aviation safety",
    "flight safety", "safety alert", "safety notice", "safety bulletin",
    "accident investigation", "incident investigation", "emergency descent",
    "uncommanded descent", "engine malfunction", "pressurization failure",
    "electrical failure", "aircraft damage", "aircraft malfunction", "emergency landing gear",
    "runway overrun", "collision with aircraft", "flight diverted", "aircraft recall"
]

# ============================================================
# 3. REDIS CONNECTION
# ============================================================

db = None
local_seen = set()
local_baselines = set()
local_lock = threading.Lock()

def connect_redis():
    global db
    if not REDIS_URL:
        log.warning("REDIS_URL is missing. Using temporary local memory.")
        return
    try:
        candidate = redis.Redis.from_url(
            REDIS_URL, decode_responses=True,
            socket_connect_timeout=5, socket_timeout=5, health_check_interval=30,
        )
        candidate.ping()
        db = candidate
        log.info("Redis connection successful.")
    except Exception:
        db = None
        log.exception("Redis connection failed. Using temporary local memory.")

def redis_key(name):
    return "aviation_monitor:v3:" + name

connect_redis()

# ============================================================
# 4. HEALTH CHECK ENDPOINTS
# ============================================================

@app.get("/")
def home():
    redis_status = False
    if db is not None:
        try:
            redis_status = bool(db.ping())
        except redis.RedisError:
            redis_status = False
    return jsonify({
        "service": "Aviation Safety Monitor",
        "status": "running",
        "redis_connected": redis_status,
        "check_interval_seconds": CHECK_INTERVAL,
        "utc_time": datetime.now(timezone.utc).isoformat(),
    })

@app.get("/health")
def health():
    return jsonify({"status": "healthy", "service": "Aviation Safety Monitor"}), 200

# ============================================================
# 5. FINGERPRINT GENERATION
# ============================================================

def create_hash(value):
    return hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()

def item_fingerprint(title, link):
    return create_hash(title.strip() + "\n" + link.strip())

def baseline_exists(source_id):
    if db is not None:
        try:
            return bool(db.exists(redis_key("baseline:" + source_id)))
        except redis.RedisError:
            log.exception("Redis baseline check failed.")
    with local_lock:
        return source_id in local_baselines

def save_baseline(source_id, fingerprints):
    if db is not None:
        try:
            pipe = db.pipeline()
            for fingerprint in fingerprints:
                pipe.set(redis_key("item:" + source_id + ":" + fingerprint), "sent", ex=FINGERPRINT_TTL)
            pipe.set(redis_key("baseline:" + source_id), "1")
            pipe.execute()
            return
        except redis.RedisError:
            log.exception("Redis baseline save failed.")
    with local_lock:
        for fingerprint in fingerprints:
            local_seen.add(source_id + ":" + fingerprint)
        local_baselines.add(source_id)

def reserve_item(source_id, fingerprint):
    storage_key = redis_key("item:" + source_id + ":" + fingerprint)
    if db is not None:
        try:
            return bool(db.set(storage_key, "pending", nx=True, ex=600))
        except redis.RedisError:
            log.exception("Redis item reservation failed.")
    local_key = source_id + ":" + fingerprint
    with local_lock:
        if local_key in local_seen:
            return False
        local_seen.add(local_key)
        return True

def finish_item(source_id, fingerprint, delivered):
    storage_key = redis_key("item:" + source_id + ":" + fingerprint)
    if db is not None:
        try:
            if delivered:
                db.set(storage_key, "sent", ex=FINGERPRINT_TTL)
            elif db.get(storage_key) == "pending":
                db.delete(storage_key)
            return
        except redis.RedisError:
            log.exception("Redis delivery status update failed.")
    if not delivered:
        with local_lock:
            local_seen.discard(source_id + ":" + fingerprint)

# ============================================================
# 6. TELEGRAM ALERT DELIVERY
# ============================================================

def send_telegram_alert(message):
    if not BOT_TOKEN or not CHAT_ID:
        log.error("Telegram configuration missing. Check environment variables.")
        return
    url = f"https://telegram.org{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID, "text": message, "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    try:
        res = requests.post(url, json=payload, timeout=10)
        if res.status_code != 200:
            log.error(f"Telegram API failure: {res.status_code} - {res.text}")
    except Exception:
        log.exception("Failed to dispatch alert to Telegram endpoint.")

# ============================================================
# 7. MULTI-DOCUMENT SMART PARSERS
# ============================================================

def matches_safety_filter(text):
    if not SAFETY_FILTER:
        return True
    text_lower = text.lower()
    return any(term in text_lower for term in SAFETY_TERMS)

def extract_document_items(url):
    extracted = []
    
    # Strip messy social anchors (like #google_vignette) before processing network calls
    clean_target_url = url.split('#')[0]
    
    response = session.get(clean_target_url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    
    content_type = response.headers.get("Content-Type", "").lower()

    # A. PDF PARSER
    if "application/pdf" in content_type or clean_target_url.lower().endswith(".pdf"):
        with io.BytesIO(response.content) as f:
            reader = PdfReader(f)
            text_pool = "".join([page.extract_text() for page in reader.pages if page.extract_text()])
            for line in [lnk.strip() for lnk in text_pool.split("\n") if len(lnk.strip()) > 15]:
                extracted.append((f"PDF Content Line: {line}", clean_target_url))

    # B. EXCEL SPREADSHEET PARSER
    elif "spreadsheet" in content_type or clean_target_url.lower().endswith(".xlsx"):
        wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=True)
        sheet = wb.active

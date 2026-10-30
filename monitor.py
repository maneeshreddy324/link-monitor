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


# =========================================================
# CONFIGURATION
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LINKS_FILE = os.path.join(BASE_DIR, "links.txt")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
REDIS_URL = os.getenv("REDIS_URL", "").strip()

CHECK_INTERVAL = max(
    60, int(os.getenv("CHECK_INTERVAL_SECONDS", "300"))
)
REQUEST_TIMEOUT = 20
MAX_ITEMS_PER_SOURCE = 100
MAX_ALERTS_PER_SCAN = 30
FINGERPRINT_TTL = 60 * 60 * 24 * 30  # 30 days

# Set to false to monitor all newly published items.
SAFETY_FILTER = (
    os.getenv("SAFETY_FILTER", "true").lower() == "true"
)

SAFETY_TERMS = [
    "aircraft accident",
    "aviation accident",
    "plane crash",
    "airplane crash",
    "emergency landing",
    "forced landing",
    "mayday",
    "aircraft incident",
    "aviation incident",
    "engine failure",
    "engine fire",
    "engine shutdown",
    "bird strike",
    "runway excursion",
    "runway incursion",
    "hard landing",
    "emergency evacuation",
    "smoke in cabin",
    "fire on board",
    "loss of control",
    "loss of cabin pressure",
    "cabin depressurization",
    "flight control failure",
    "structural failure",
    "landing gear failure",
    "near collision",
    "midair collision",
    "turbulence injuries",
    "safety investigation",
    "airworthiness directive",
    "safety warning",
    "faa safety alert",
    "emergency declaration",
    "aborted takeoff",
    "diverted due to",
    "hydraulic failure",
    "fuel leak",
    "windshield cracked",
    "flight returned",
    "aircraft missing",
    "aircraft disappeared",
    "black box",
    "fatal crash",
    "fatal accident",
    "fatalities",
]

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("aviation-monitor")

app = Flask(__name__)

session = requests.Session()
session.headers.update({
    "User-Agent": "AviationSafetyMonitor/2.0 (+scheduled monitoring)"
})

# =========================================================
# REDIS CONNECTION AND FALLBACK
# =========================================================

db = None
local_seen = set()
local_baselines = set()
local_lock = threading.Lock()


def connect_redis():
    global db

    if not REDIS_URL:
        log.warning("REDIS_URL is missing; using temporary memory.")
        return

    try:
        candidate = redis.Redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            health_check_interval=30,
        )
        candidate.ping()
        db = candidate
        log.info("Redis connection successful.")
    except Exception:
        db = None
        log.exception(
            "Redis connection failed. Using temporary memory."
        )


def key(name):
    return "aviation_monitor:v2:" + name


def has_baseline(source_id):
    if db is not None:
        try:
            return bool(db.exists(key("baseline:" + source_id)))
        except redis.RedisError:
            log.exception("Redis baseline lookup failed.")

    with local_lock:
        return source_id in local_baselines


def save_baseline(source_id, fingerprints):
    if db is not None:
        try:
            pipe = db.pipeline()
            for fingerprint in fingerprints:
                pipe.set(
                    key("item:" + source_id + ":" + fingerprint),
                    "sent",
                    ex=FINGERPRINT_TTL,
                )
            pipe.set(key("baseline:" + source_id), "1")
            pipe.execute()
            return
        except redis.RedisError:
            log.exception("Could not save baseline to Redis.")

    with local_lock:
        for fingerprint in fingerprints:
            local_seen.add(source_id + ":" + fingerprint)
        local_baselines.add(source_id)


def reserve_item(source_id, fingerprint):
    """
    Reserve an item before delivery.
    A pending reservation expires so failed sends can be retried.
    """
    item_key = key("item:" + source_id + ":" + fingerprint)

    if db is not None:
        try:
            return bool(db.set(item_key, "pending", nx=True, ex=600))
        except redis.RedisError:
            log.exception("Redis reservation failed.")

    local_key = source_id + ":" + fingerprint
    with local_lock:
        if local_key in local_seen:
            return False
        local_seen.add(local_key)
        return True


def finish_item(source_id, fingerprint, delivered):
    item_key = key("item:" + source_id + ":" + fingerprint)

    if db is not None:
        try:
            if delivered:
                db.set(item_key, "sent", ex=FINGERPRINT_TTL)
            elif db.get(item_key) == "pending":
                db.delete(item_key)
            return
        except redis.RedisError:
            log.exception("Could not update Redis item status.")

    if not delivered:
        with local_lock:
            local_seen.discard(source_id + ":" + fingerprint)


connect_redis()


# =========================================================
# FLASK HEALTH ENDPOINTS
# =========================================================

@app.get("/")
def home():
    return jsonify({
        "service": "Aviation Safety Monitor",
        "status": "running",
        "scan_interval_seconds": CHECK_INTERVAL,
        "redis_connected": (
            db is not None and db.ping()
        ) if db is not None else False,
        "utc_time": datetime.now(timezone.utc).isoformat(),
    })


@app.get("/health")
def health():
    return jsonify({"status": "healthy"}), 200


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram_alert(message):
    if not BOT_TOKEN or not CHAT_ID:
        log.error("Telegram token or chat ID is missing.")
        return False

    # Correct Telegram Bot API endpoint.
    api_url = (
        "https://api.telegram.org/bot"
        + BOT_TOKEN
        + "/sendMessage"
    )

    payload = {
        "chat_id": CHAT_ID,
        "text": message[:4000],
        "disable_web_page_preview": True,
    }

    for attempt in range(3):
        try:
            response = session.post(
                api_url,
                json=payload,
                timeout=15,
            )

            if response.status_code == 429:
                try:
                    delay = int(
                        response.json().get("parameters", {}).get(
                            "retry_after", 5
                        )
                    )
                except (ValueError, TypeError):
                    delay = 5

                time.sleep(min(delay, 30))
                continue

            response.raise_for_status()
            result = response.json()

            if result.get("ok") is True:
                log.info("Telegram message delivered.")
                return True

            log.error("Telegram API error: %s", result)
            return False

        except requests.RequestException:
            log.exception(
                "Telegram send attempt %d failed.", attempt + 1
            )
            if attempt < 2:
                time.sleep(2 ** attempt)

    return False


# =========================================================
# HELPERS
# =========================================================

def fingerprint(title, link):
    value = title.strip() + "\n" + link.strip()
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def is_safety_related(title):
    if not SAFETY_FILTER:
        return True

    text = title.lower()
    return any(term in text for term in SAFETY_TERMS)


def xml_local_name(tag):
    return tag.rsplit("}", 1)[-1].lower()


def unique_records(records):
    output = []
    seen = set()

    for title, link in records:
        title = " ".join(str(title).split())
        link = str(link).strip()

        if not title or not link:
            continue

        pair = (title, link)
        if pair not in seen:
            seen.add(pair)
            output.append(pair)

    return output[:MAX_ITEMS_PER_SOURCE]


# =========================================================
# SOURCE PARSER: RSS, ATOM, HTML, PDF, CSV, XLSX
# =========================================================

def parse_source(url):
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    content = response.content
    content_type = response.headers.get(
        "Content-Type", ""
    ).lower()
    path = urlparse(url).path.lower()
    records = []

    # PDF
    if "application/pdf" in content_type or path.endswith(".pdf"):
        reader = PdfReader(io.BytesIO(content))

        for page_no, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""

            for line in text.splitlines():
                line = " ".join(line.split())
                if len(line) >= 20:
                    records.append((
                        line[:400],
                        f"{url}#page={page_no}",
                    ))

    # Excel
    elif (
        "spreadsheet" in content_type
        or "excel" in content_type
        or path.endswith((".xlsx", ".xlsm"))
    ):
        workbook = openpyxl.load_workbook(
            io.BytesIO(content),
            read_only=True,
            data_only=True,
        )

        try:
            for row in workbook.active.iter_rows(values_only=True):
                values = [
                    str(cell).strip()
                    for cell in row
                    if cell is not None
                ]
                text = " | ".join(values)

                if len(text) >= 15:
                    records.append((text[:400], url))
        finally:
            workbook.close()

    # CSV
    elif "text/csv" in content_type or path.endswith(".csv"):
        decoded = content.decode("utf-8-sig", errors="replace")

        for row in csv.reader(io.StringIO(decoded)):
            text = " | ".join(
                cell.strip() for cell in row if cell.strip()
            )
            if len(text) >= 15:
                records.append((text[:400], url))

    # XML, RSS and Atom
    elif (
        "xml" in content_type
        or "rss" in content_type
        or path.endswith((".xml", ".rss", ".atom"))
    ):
        root = ET.fromstring(content)

        for item in root.iter():
            if xml_local_name(item.tag) not in ("item", "entry"):
                continue

            title = ""
            link = url

            for child in item:
                name = xml_local_name(child.tag)

                if name == "title":
                    title = "".join(child.itertext()).strip()

                elif name == "link":
                    href = child.attrib.get("href", "").strip()
                    text_link = "".join(child.itertext()).strip()
                    link = urljoin(url, href or text_link or url)

            if title:
                records.append((title, link))

    # HTML
    else:
        soup = BeautifulSoup(content, "html.parser")

        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()

        for anchor in soup.find_all("a", href=True):
            title = anchor.get_text(" ", strip=True)
            title = " ".join(title.split())

            if len(title) < 15:
                continue

            link = urljoin(url, anchor["href"])

            if urlparse(link).scheme not in ("http", "https"):
                continue

            records.append((title[:400], link))

    return unique_records(records)


# =========================================================
# SCAN A SOURCE
# =========================================================

def scan_source(url):
    source_id = hashlib.sha256(url.encode("utf-8")).hexdigest()
    source_name = urlparse(url).netloc or url

    try:
        records = parse_source(url)

        if not records:
            log.warning("No readable items found: %s", url)
            return

        # Apply the safety filter to the title/text.
        records = [
            (title, link)
            for title, link in records
            if is_safety_related(title)
        ]

        if not records:
            log.info("No safety-keyword matches: %s", source_name)
            # Do not establish the baseline based on an empty
            # filtered result. The next scan will try again.
            return

        item_fingerprints = [
            fingerprint(title, link) for title, link in records
        ]

        # Initial baseline: remember current records without alerts.
        if not has_baseline(source_id):
            save_baseline(source_id, item_fingerprints)
            log.info(
                "Baseline established: %s (%d matching items)",
                source_name,
                len(records),
            )
            return

        sent_this_scan = 0

        for (title, link), item_fp in zip(records, item_fingerprints):
            if sent_this_scan >= MAX_ALERTS_PER_SCAN:
                log.warning(
                    "Alert cap reached for %s; remaining items "
                    "will be checked on a later scan.",
                    source_name,
                )
                break

            if not reserve_item(source_id, item_fp):
                continue

            message = (
                "✈️ AVIATION SAFETY UPDATE\n\n"
                f"Source: {source_name}\n\n"
                f"{title}\n\n"
                f"Article: {link}"
            )

            delivered = send_telegram_alert(message)
            finish_item(source_id, item_fp, delivered)

            if delivered:
                sent_this_scan += 1

            time.sleep(1)

        log.info(
            "Finished %s; new alerts delivered: %d",
            source_name,
            sent_this_scan,
        )

    except requests.RequestException as exc:
        log.error("Source request failed (%s): %s", url, exc)

    except Exception:
        log.exception("Source processing failed: %s", url)


# =========================================================
# LOAD URLS
# =========================================================

def load_urls():
    if not os.path.isfile(LINKS_FILE):
        log.error("links.txt not found at %s", LINKS_FILE)
        return []

    urls = []

    with open(LINKS_FILE, "r", encoding="utf-8-sig") as file:
        for line in file:
            url = line.strip()

            if not url or url.startswith("#"):
                continue

            if urlparse(url).scheme not in ("http", "https"):
                log.warning("Skipping invalid URL: %s", url)
                continue

            urls.append(url)

    return list(dict.fromkeys(urls))


# =========================================================
# CONTINUOUS MONITORING
# =========================================================

def monitor_loop():
    log.info("Monitoring thread started.")

    while True:
        started = time.monotonic()

        try:
            urls = load_urls()
            log.info("Scanning %d URLs.", len(urls))

            for url in urls:
                scan_source(url)

        except Exception:
            log.exception("Unexpected monitoring loop error.")

        elapsed = time.monotonic() - started
        wait_seconds = max(1, CHECK_INTERVAL - int(elapsed))

        log.info(
            "Scan cycle finished in %.1f seconds. "
            "Waiting %d seconds.",
            elapsed,
            wait_seconds,
        )

        time.sleep(wait_seconds)


# =========================================================
# START
# =========================================================

if __name__ == "__main__":
    worker = threading.Thread(
        target=monitor_loop,
        name="aviation-monitor",
        daemon=True,
    )
    worker.start()

    port = int(os.getenv("PORT", "10000"))

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
        threaded=True,
    )

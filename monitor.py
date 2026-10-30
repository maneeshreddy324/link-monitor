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


# ============================================================
# 1. CONFIGURATION
# ============================================================

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
MAX_ALERTS_PER_SOURCE = 30
FINGERPRINT_TTL = 30 * 24 * 60 * 60

SAFETY_FILTER = (
    os.getenv("SAFETY_FILTER", "true").lower() == "true"
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)

log = logging.getLogger("aviation-monitor")

app = Flask(__name__)

session = requests.Session()
session.headers.update({
    "User-Agent": "AviationSafetyMonitor/2.0"
})


# ============================================================
# 2. AVIATION SAFETY KEYWORDS
# ============================================================

SAFETY_TERMS = [
    "aircraft accident",
    "aviation accident",
    "plane crash",
    "airplane crash",
    "aircraft crash",
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
    "aircraft fire",
    "aviation safety",
    "flight safety",
    "safety alert",
    "safety notice",
    "safety bulletin",
    "accident investigation",
    "incident investigation",
    "emergency descent",
    "uncommanded descent",
    "engine malfunction",
    "pressurization failure",
    "electrical failure",
    "aircraft damage",
    "aircraft malfunction",
    "emergency landing gear",
    "runway overrun",
    "collision with aircraft",
    "flight diverted",
    "aircraft recall",
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
        log.warning(
            "REDIS_URL is missing. Using temporary local memory."
        )
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
            "Redis connection failed. "
            "Using temporary local memory."
        )


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
    return jsonify({
        "status": "healthy",
        "service": "Aviation Safety Monitor",
    }), 200


# ============================================================
# 5. FINGERPRINT GENERATION
# ============================================================

def create_hash(value):
    return hashlib.sha256(
        value.encode("utf-8", errors="replace")
    ).hexdigest()


def item_fingerprint(title, link):
    return create_hash(
        title.strip() + "\n" + link.strip()
    )


def baseline_exists(source_id):
    if db is not None:
        try:
            return bool(
                db.exists(redis_key("baseline:" + source_id))
            )
        except redis.RedisError:
            log.exception("Redis baseline check failed.")

    with local_lock:
        return source_id in local_baselines


def save_baseline(source_id, fingerprints):
    if db is not None:
        try:
            pipe = db.pipeline()

            for fingerprint in fingerprints:
                pipe.set(
                    redis_key(
                        "item:" + source_id + ":" + fingerprint
                    ),
                    "sent",
                    ex=FINGERPRINT_TTL,
                )

            pipe.set(
                redis_key("baseline:" + source_id),
                "1",
            )

            pipe.execute()
            return

        except redis.RedisError:
            log.exception("Redis baseline save failed.")

    with local_lock:
        for fingerprint in fingerprints:
            local_seen.add(source_id + ":" + fingerprint)

        local_baselines.add(source_id)


def reserve_item(source_id, fingerprint):
    """
    Reserve an item before sending.

    Redis reservation expires after 10 minutes. If a process
    crashes before completing delivery, a future scan can retry.
    """

    storage_key = redis_key(
        "item:" + source_id + ":" + fingerprint
    )

    if db is not None:
        try:
            return bool(
                db.set(
                    storage_key,
                    "pending",
                    nx=True,
                    ex=600,
                )
            )

        except redis.RedisError:
            log.exception("Redis item reservation failed.")

    local_key = source_id + ":" + fingerprint

    with local_lock:
        if local_key in local_seen:
            return False

        local_seen.add(local_key)
        return True


def finish_item(source_id, fingerprint, delivered):
    storage_key = redis_key(
        "item:" + source_id + ":" + fingerprint
    )

    if db is not None:
        try:
            if delivered:
                db.set(
                    storage_key,
                    "sent",
                    ex=FINGERPRINT_TTL,
                )

            elif db.get(storage_key) == "pending":
                db.delete(storage_key)

            return

        except redis.RedisError:
            log.exception("Redis delivery status update failed.")

    if not delivered:
        with local_lock:
            local_seen.discard(
                source_id + ":" + fingerprint
            )


# ============================================================
# 6. TELEGRAM ALERT DELIVERY
# ============================================================

def send_telegram_alert(message):
    if not BOT_TOKEN or not CHAT_ID:
        log.error(
            "Telegram configuration missing. "
            "Check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
        )
        return False

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
                    wait_time = int(
                        response.json()
                        .get("parameters", {})
                        .get("retry_after", 5)
                    )
                except (ValueError, TypeError):
                    wait_time = 5

                time.sleep(min(wait_time, 30))
                continue

            response.raise_for_status()

            result = response.json()

            if result.get("ok") is True:
                log.info("Telegram alert delivered.")
                return True

            log.error("Telegram API rejected alert: %s", result)

            return False

        except requests.RequestException:
            log.exception(
                "Telegram attempt %d failed.",
                attempt + 1,
            )

            if attempt < 2:
                time.sleep(2 ** attempt)

    return False


# ============================================================
# 7. AVIATION SAFETY FILTER
# ============================================================

def is_safety_related(title):
    if not SAFETY_FILTER:
        return True

    text = title.lower()

    return any(
        term in text
        for term in SAFETY_TERMS
    )


# ============================================================
# 8. REMOVE DUPLICATE RECORDS
# ============================================================

def unique_records(records):
    result = []
    seen = set()

    for title, link in records:
        title = " ".join(str(title).split())
        link = str(link).strip()

        if not title or not link:
            continue

        item = (title, link)

        if item not in seen:
            seen.add(item)
            result.append(item)

    return result[:MAX_ITEMS_PER_SOURCE]


def xml_local_name(tag):
    return tag.rsplit("}", 1)[-1].lower()


# ============================================================
# 9. READ RSS, ATOM, HTML, PDF, CSV AND EXCEL
# ============================================================

def parse_source(url):
    response = session.get(
        url,
        timeout=REQUEST_TIMEOUT,
    )

    response.raise_for_status()

    content = response.content

    content_type = response.headers.get(
        "Content-Type", ""
    ).lower()

    path = urlparse(url).path.lower()

    records = []

    # --------------------------------------------------------
    # PDF
    # --------------------------------------------------------

    if (
        "application/pdf" in content_type
        or path.endswith(".pdf")
    ):

        reader = PdfReader(io.BytesIO(content))

        for page_number, page in enumerate(
            reader.pages,
            start=1,
        ):

            page_text = page.extract_text() or ""

            for line in page_text.splitlines():
                line = " ".join(line.split())

                if len(line) >= 20:
                    records.append((
                        line[:400],
                        f"{url}#page={page_number}",
                    ))

    # --------------------------------------------------------
    # EXCEL
    # --------------------------------------------------------

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
            sheet = workbook.active

            for row in sheet.iter_rows(values_only=True):

                values = [
                    str(cell).strip()
                    for cell in row
                    if cell is not None
                ]

                row_text = " | ".join(values)

                if len(row_text) >= 15:
                    records.append((
                        row_text[:400],
                        url,
                    ))

        finally:
            workbook.close()

    # --------------------------------------------------------
    # CSV
    # --------------------------------------------------------

    elif (
        "text/csv" in content_type
        or path.endswith(".csv")
    ):

        decoded = content.decode(
            "utf-8-sig",
            errors="replace",
        )

        reader = csv.reader(io.StringIO(decoded))

        for row in reader:

            row_text = " | ".join(
                cell.strip()
                for cell in row
                if cell.strip()
            )

            if len(row_text) >= 15:
                records.append((
                    row_text[:400],
                    url,
                ))

    # --------------------------------------------------------
    # RSS / ATOM / XML
    # --------------------------------------------------------

    elif (
        "xml" in content_type
        or "rss" in content_type
        or path.endswith((".xml", ".rss", ".atom"))
    ):

        root = ET.fromstring(content)

        for item in root.iter():

            if xml_local_name(item.tag) not in (
                "item",
                "entry",
            ):
                continue

            title = ""
            link = url

            for child in item:

                name = xml_local_name(child.tag)

                if name == "title":
                    title = "".join(
                        child.itertext()
                    ).strip()

                elif name == "link":

                    href = child.attrib.get(
                        "href", ""
                    ).strip()

                    text_link = "".join(
                        child.itertext()
                    ).strip()

                    link = urljoin(
                        url,
                        href or text_link or url,
                    )

            if title:
                records.append((
                    title,
                    link,
                ))

    # --------------------------------------------------------
    # HTML
    # --------------------------------------------------------

    else:

        soup = BeautifulSoup(
            content,
            "html.parser",
        )

        for tag in soup([
            "script",
            "style",
            "noscript",
            "svg",
        ]):
            tag.decompose()

        for anchor in soup.find_all(
            "a",
            href=True,
        ):

            title = anchor.get_text(
                " ",
                strip=True,
            )

            title = " ".join(title.split())

            if len(title) < 15:
                continue

            link = urljoin(
                url,
                anchor["href"],
            )

            if urlparse(link).scheme not in (
                "http",
                "https",
            ):
                continue

            records.append((
                title[:400],
                link,
            ))

    return unique_records(records)


# ============================================================
# 10. SCAN INDIVIDUAL SOURCE
# ============================================================

def scan_source(url):
    source_id = create_hash(url)

    source_name = (
        urlparse(url).netloc or url
    )

    try:
        records = parse_source(url)

        if not records:
            log.warning(
                "No readable records found: %s",
                url,
            )
            return

        # Apply safety filter to extracted text.
        filtered_records = [
            (title, link)
            for title, link in records
            if is_safety_related(title)
        ]

        # Do not create an empty baseline if no items match.
        if not filtered_records:
            log.info(
                "No safety-keyword matches at %s.",
                source_name,
            )
            return

        fingerprints = [
            item_fingerprint(title, link)
            for title, link in filtered_records
        ]

        # ----------------------------------------------------
        # INITIAL BASELINE
        # ----------------------------------------------------

        if not baseline_exists(source_id):

            save_baseline(
                source_id,
                fingerprints,
            )

            log.info(
                "Initial baseline saved for %s: %d items.",
                source_name,
                len(filtered_records),
            )

            return

        # ----------------------------------------------------
        # DETECT NEW ITEMS
        # ----------------------------------------------------

        alerts_sent = 0

        for (title, link), fingerprint in zip(
            filtered_records,
            fingerprints,
        ):

            if alerts_sent >= MAX_ALERTS_PER_SOURCE:
                log.warning(
                    "Alert limit reached for %s.",
                    source_name,
                )
                break

            reserved = reserve_item(
                source_id,
                fingerprint,
            )

            if not reserved:
                continue

            message = (
                "AVIATION SAFETY UPDATE\n\n"
                f"Source: {source_name}\n\n"
                f"{title}\n\n"
                f"Article: {link}"
            )

            delivered = send_telegram_alert(
                message
            )

            finish_item(
                source_id,
                fingerprint,
                delivered,
            )

            if delivered:
                alerts_sent += 1

            time.sleep(1)

        log.info(
            "Source scan completed: %s; alerts sent: %d.",
            source_name,
            alerts_sent,
        )

    except requests.RequestException as exc:

        log.error(
            "Source download failed: %s | %s",
            url,
            exc,
        )

    except Exception:

        log.exception(
            "Source processing failed: %s",
            url,
        )


# ============================================================
# 11. LOAD MONITORING URLS
# ============================================================

def load_urls():

    if not os.path.isfile(LINKS_FILE):

        log.error(
            "links.txt not found: %s",
            LINKS_FILE,
        )

        return []

    urls = []

    with open(
        LINKS_FILE,
        "r",
        encoding="utf-8-sig",
    ) as file:

        for line in file:

            url = line.strip()

            if not url or url.startswith("#"):
                continue

            parsed = urlparse(url)

            if parsed.scheme not in (
                "http",
                "https",
            ) or not parsed.netloc:

                log.warning(
                    "Skipping invalid URL: %s",
                    url,
                )

                continue

            urls.append(url)

    # Remove duplicate URLs while preserving order.
    return list(dict.fromkeys(urls))


# ============================================================
# 12. CONTINUOUS MONITORING LOOP
# ============================================================

def monitor_loop():

    log.info(
        "Aviation monitoring background thread started."
    )

    while True:

        started = time.monotonic()

        try:

            urls = load_urls()

            log.info(
                "Starting scan of %d URLs.",
                len(urls),
            )

            for url in urls:
                scan_source(url)

        except Exception:

            log.exception(
                "Unexpected monitoring loop error."
            )

        elapsed = time.monotonic() - started

        wait_seconds = max(
            1,
            CHECK_INTERVAL - int(elapsed),
        )

        log.info(
            "Scan finished in %.1f seconds. "
            "Next scan in %d seconds.",
            elapsed,
            wait_seconds,
        )

        time.sleep(wait_seconds)


# ============================================================
# 13. START APPLICATION
# ============================================================

if __name__ == "__main__":

    log.info(
        "Starting Aviation Safety Monitor..."
    )

    worker = threading.Thread(
        target=monitor_loop,
        name="aviation-monitor",
        daemon=True,
    )

    worker.start()

    port = int(
        os.getenv("PORT", "10000")
    )

    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
        threaded=True,
    )

import os
import re
import json
import time
import html
import hashlib
import logging
import smtplib
import threading
import xml.etree.ElementTree as ET

from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse, urldefrag
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from flask import Flask, jsonify


# =========================================================
# 1. CONFIGURATION
# =========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

LINKS_FILE = os.getenv(
    "LINKS_FILE",
    os.path.join(BASE_DIR, "links.txt")
)

STATE_FILE = os.getenv(
    "STATE_FILE",
    os.path.join(BASE_DIR, "state.json")
)

CHECK_INTERVAL = max(
    60,
    int(os.getenv("CHECK_INTERVAL", "300"))
)

REQUEST_TIMEOUT = 12
MAX_WORKERS = 10
MAX_ARTICLES_PER_SOURCE = 100
MAX_STORED_ARTICLES = 20000
MAX_SUMMARY_LENGTH = 1200

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
MAIL_TO = os.getenv("MAIL_TO")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; AircraftSafetyMonitor/1.0)"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

app = Flask(__name__)

thread_local = threading.local()
state_lock = threading.Lock()
cycle_lock = threading.Lock()

status = {
    "started": False,
    "last_check": None,
    "last_success": None,
    "last_error": None,
    "sources_configured": 0,
    "sources_successful": 0,
    "articles_checked": 0,
    "matches_detected": 0,
    "notifications_sent": 0,
}


# =========================================================
# 2. AIRCRAFT SAFETY KEYWORDS
# =========================================================

AIRCRAFT_TERMS = [
    "aircraft", "airplane", "aeroplane", "airliner",
    "aviation", "aircraft engine", "flight crew",
    "pilot", "passenger jet", "cargo aircraft",
    "helicopter", "flight deck", "landing gear",
    "airbus", "boeing", "embraer", "bombardier",
    "commercial flight", "airline flight",
    "airworthiness", "runway", "flight control",
]

SAFETY_TERMS = [
    "crash", "crashed", "aircraft accident",
    "aviation accident", "aircraft incident",
    "emergency landing", "forced landing",
    "precautionary landing", "engine failure",
    "engine fire", "engine shutdown",
    "in-flight shutdown", "smoke in cabin",
    "cabin pressure", "loss of cabin pressure",
    "hydraulic failure", "landing gear failure",
    "flight control failure", "runway excursion",
    "runway overrun", "runway incursion",
    "bird strike", "hard landing", "tail strike",
    "mid-air collision", "near miss",
    "emergency declared", "mayday",
    "aircraft damage", "structural damage",
    "windscreen cracked", "windshield cracked",
    "tire burst", "tyre burst", "engine malfunction",
    "technical failure", "mechanical failure",
    "emergency diversion", "airworthiness directive",
    "safety directive", "safety investigation",
    "aircraft grounded", "fleet grounded",
    "fleet grounding", "takeoff aborted",
    "rejected takeoff", "loss of control",
    "overshot runway", "flight returned",
    "returned to airport", "emergency evacuation",
    "fuel leak", "oil leak",
    "uncontained engine failure", "engine issue",
    "bird ingestion", "turbulence injury",
    "pressurization problem", "pressurisation problem",
    "cracked fuselage", "fuselage damage",
    "faa directive", "easa directive",
    "engine separation", "wing damage",
    "emergency slide", "runway collision",
]

SPECIFIC_SAFETY_TERMS = [
    "emergency landing", "forced landing",
    "engine failure", "engine fire",
    "in-flight shutdown", "cabin pressure",
    "hydraulic failure", "landing gear failure",
    "flight control failure", "runway excursion",
    "runway overrun", "runway incursion",
    "bird strike", "hard landing", "tail strike",
    "mid-air collision", "airworthiness directive",
    "aircraft grounded", "fleet grounded",
    "rejected takeoff", "loss of control",
    "emergency evacuation", "uncontained engine failure",
    "fuel leak", "oil leak", "cracked fuselage",
    "faa directive", "easa directive",
]


def compile_terms(terms):
    return [
        re.compile(
            r"(?<!\w)" + re.escape(term) + r"(?!\w)",
            re.IGNORECASE,
        )
        for term in terms
    ]


AIRCRAFT_PATTERNS = compile_terms(AIRCRAFT_TERMS)
SAFETY_PATTERNS = compile_terms(SAFETY_TERMS)
SPECIFIC_PATTERNS = compile_terms(SPECIFIC_SAFETY_TERMS)


def matches_any(text, patterns):
    return any(pattern.search(text) for pattern in patterns)


def is_aircraft_safety_news(title, summary, url=""):
    text = f"{title} {summary} {url}"

    safety_match = matches_any(text, SAFETY_PATTERNS)
    aircraft_match = matches_any(text, AIRCRAFT_PATTERNS)
    specific_match = matches_any(text, SPECIFIC_PATTERNS)

    return safety_match and (
        aircraft_match or specific_match
    )


# =========================================================
# 3. HTTP SESSION AND RETRIES
# =========================================================

def get_session():
    if not hasattr(thread_local, "session"):
        session = requests.Session()

        retry = Retry(
            total=2,
            connect=2,
            read=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry)

        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(HEADERS)

        thread_local.session = session

    return thread_local.session


def fetch(url):
    response = get_session().get(
        url,
        timeout=REQUEST_TIMEOUT,
        allow_redirects=True,
    )

    response.raise_for_status()
    return response


# =========================================================
# 4. LOAD NEWS SOURCES
# =========================================================

def load_sources():
    sources = []
    seen_urls = set()

    if not os.path.exists(LINKS_FILE):
        logging.error("Missing source file: %s", LINKS_FILE)
        return sources

    with open(LINKS_FILE, "r", encoding="utf-8-sig") as file:
        for line in file:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            if "|" in line:
                label, url = line.split("|", 1)
                label = label.strip()
                url = url.strip()
            else:
                url = line
                label = urlparse(url).netloc

            parsed = urlparse(url)

            if parsed.scheme not in ("http", "https"):
                continue

            if not parsed.netloc or url in seen_urls:
                continue

            seen_urls.add(url)

            sources.append({
                "label": label or parsed.netloc,
                "url": url,
            })

    return sources


# =========================================================
# 5. SAVE AND LOAD STATE
# =========================================================

def new_state():
    return {
        "initialized": False,
        "seen_articles": {},
    }


def load_state():
    if not os.path.exists(STATE_FILE):
        return new_state()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as file:
            state = json.load(file)

        state.setdefault("initialized", False)
        state.setdefault("seen_articles", {})

        return state

    except (OSError, json.JSONDecodeError):
        logging.exception("Could not load saved state.")
        return new_state()


def save_state(state):
    directory = os.path.dirname(
        os.path.abspath(STATE_FILE)
    )

    os.makedirs(directory, exist_ok=True)

    temp_file = STATE_FILE + ".tmp"

    with open(temp_file, "w", encoding="utf-8") as file:
        json.dump(
            state,
            file,
            indent=2,
            ensure_ascii=False,
        )

    os.replace(temp_file, STATE_FILE)


def normalize_url(url):
    url = urldefrag(url.strip())[0]
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        return ""

    if not parsed.netloc:
        return ""

    return url


def make_article_id(url):
    return hashlib.sha256(
        url.encode("utf-8")
    ).hexdigest()


# =========================================================
# 6. ARTICLE EXTRACTION
# =========================================================

def clean_text(value):
    if not value:
        return ""

    return " ".join(
        BeautifulSoup(
            str(value),
            "html.parser"
        ).get_text(" ", strip=True).split()
    )


def parse_date(value):
    if not value:
        return ""

    try:
        dt = parsedate_to_datetime(value)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc).isoformat()

    except (TypeError, ValueError, OverflowError):
        pass

    try:
        dt = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(timezone.utc).isoformat()

    except (TypeError, ValueError, OverflowError):
        return ""


def extract_feed_articles(content, source):
    soup = BeautifulSoup(content, "xml")
    entries = soup.find_all(["item", "entry"])

    articles = []

    for entry in entries[:MAX_ARTICLES_PER_SOURCE]:
        title_tag = entry.find("title")
        title = clean_text(
            title_tag.get_text() if title_tag else ""
        )

        link = ""
        link_tag = entry.find("link", recursive=False)

        if link_tag:
            link = (
                link_tag.get("href", "")
                or link_tag.get_text(strip=True)
            )

        if not link:
            for candidate in entry.find_all("link"):
                if candidate.get("rel", "alternate") == "alternate":
                    link = candidate.get("href", "")

                    if link:
                        break

        link = normalize_url(
            urljoin(source["url"], link)
        )

        if not title or not link:
            continue

        summary_tag = (
            entry.find("description")
            or entry.find("summary")
            or entry.find("content")
        )

        summary = clean_text(
            summary_tag.get_text(" ", strip=True)
            if summary_tag else ""
        )

        date_tag = (
            entry.find("pubDate")
            or entry.find("published")
            or entry.find("updated")
        )

        published = parse_date(
            date_tag.get_text(strip=True)
            if date_tag else ""
        )

        articles.append({
            "title": title,
            "summary": summary[:MAX_SUMMARY_LENGTH],
            "url": link,
            "source": source["label"],
            "published": published,
        })

    return articles


def extract_html_articles(content, source):
    soup = BeautifulSoup(content, "html.parser")
    articles = []
    seen = set()

    for anchor in soup.find_all("a", href=True):
        title = clean_text(
            anchor.get_text(" ", strip=True)
        )

        if len(title) < 15:
            continue

        link = normalize_url(
            urljoin(source["url"], anchor["href"])
        )

        if not link or link in seen:
            continue

        # Ignore external navigation links.
        if urlparse(link).netloc != urlparse(
            source["url"]
        ).netloc:
            continue

        seen.add(link)

        articles.append({
            "title": title,
            "summary": "",
            "url": link,
            "source": source["label"],
            "published": "",
        })

        if len(articles) >= MAX_ARTICLES_PER_SOURCE:
            break

    return articles


def fetch_source_articles(source):
    try:
        response = fetch(source["url"])
        content = response.text

        content_type = response.headers.get(
            "Content-Type", ""
        ).lower()

        likely_feed = (
            "xml" in content_type
            or "rss" in content_type
            or "atom" in content_type
            or source["url"].lower().split("?")[0].endswith(
                (".xml", ".rss", ".atom")
            )
        )

        if likely_feed:
            articles = extract_feed_articles(
                content, source
            )

            if articles:
                return articles

        beginning = content[:5000].lower()

        if "<rss" in beginning or "<feed" in beginning:
            articles = extract_feed_articles(
                content, source
            )

            if articles:
                return articles

        return extract_html_articles(
            content, source
        )

    except Exception as exc:
        logging.warning(
            "Source failed: %s | %s",
            source["url"],
            str(exc)[:200],
        )

        return None


# =========================================================
# 7. TELEGRAM ALERTS
# =========================================================

def send_telegram(article):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram configuration is missing.")
        return False

    title = html.escape(article["title"])
    source = html.escape(article["source"])
    summary = html.escape(
        article["summary"]
        or "No article summary was available."
    )
    url = html.escape(article["url"], quote=True)

    message = (
        "🚨 <b>AIRCRAFT SAFETY ALERT</b>\n\n"
        f"<b>Title:</b> {title}\n\n"
        f"<b>Source:</b> {source}\n\n"
        f"<b>Summary:</b> {summary}\n\n"
        f'<a href="{url}">Read full article</a>'
    )

    endpoint = (
        "https://api.telegram.org/bot"
        f"{TELEGRAM_TOKEN}/sendMessage"
    )

    try:
        response = requests.post(
            endpoint,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )

        response.raise_for_status()

        if not response.json().get("ok"):
            logging.error("Telegram rejected the notification.")
            return False

        logging.info(
            "Telegram alert sent: %s",
            article["title"][:100],
        )

        return True

    except Exception:
        logging.exception("Telegram delivery failed.")
        return False


# =========================================================
# 8. EMAIL ALERTS
# =========================================================

def send_email(article):
    if not all([
        SMTP_USER,
        SMTP_PASSWORD,
        MAIL_TO,
    ]):
        logging.warning(
            "Email is not configured; skipping email alert."
        )
        return False

    message = EmailMessage()

    message["Subject"] = (
        "[Aircraft Safety Alert] "
        + article["title"][:150]
    )

    message["From"] = SMTP_USER
    message["To"] = MAIL_TO

    message.set_content(
        "AIRCRAFT SAFETY ALERT\n\n"
        f"Title: {article['title']}\n"
        f"Source: {article['source']}\n"
        f"Published: {article['published'] or 'Unknown'}\n\n"
        f"Summary:\n"
        f"{article['summary'] or 'No summary available.'}\n\n"
        f"Article URL:\n{article['url']}\n"
    )

    try:
        with smtplib.SMTP_SSL(
            SMTP_HOST,
            SMTP_PORT,
            timeout=15,
        ) as server:
            server.login(
                SMTP_USER,
                SMTP_PASSWORD,
            )

            server.send_message(message)

        logging.info(
            "Email alert sent: %s",
            article["title"][:100],
        )

        return True

    except Exception:
        logging.exception("Email delivery failed.")
        return False


# =========================================================
# 9. MONITORING CYCLE
# =========================================================

def monitor_once():
    if not cycle_lock.acquire(blocking=False):
        logging.warning("A monitoring cycle is already running.")
        return

    try:
        sources = load_sources()

        with state_lock:
            state = load_state()

        initialized_before = state["initialized"]
        previously_seen = state["seen_articles"]

        status["last_check"] = datetime.now(
            timezone.utc
        ).isoformat()

        status["sources_configured"] = len(sources)

        logging.info(
            "Starting monitoring cycle for %d sources.",
            len(sources),
        )

        all_articles = []
        successful_sources = 0

        with ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:

            futures = {
                executor.submit(
                    fetch_source_articles, source
                ): source
                for source in sources
            }

            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception:
                    logging.exception(
                        "Unexpected source-processing error."
                    )
                    continue

                if result is None:
                    continue

                successful_sources += 1
                all_articles.extend(result)

        status["sources_successful"] = successful_sources
        status["articles_checked"] = len(all_articles)

        updated_seen = dict(previously_seen)
        now = datetime.now(timezone.utc).isoformat()

        matches = 0
        sent = 0

        for article in all_articles:
            article["url"] = normalize_url(
                article["url"]
            )

            if not article["url"]:
                continue

            key = make_article_id(article["url"])

            if key in previously_seen:
                continue

            # Record new URLs, including articles that do not match
            # the safety filter, so they aren't reprocessed forever.
            updated_seen[key] = now

            if not is_aircraft_safety_news(
                article["title"],
                article["summary"],
                article["url"],
            ):
                continue

            matches += 1

            logging.info(
                "Safety keyword match: %s",
                article["title"],
            )

            # First successful run establishes the baseline.
            if not initialized_before:
                continue

            telegram_ok = send_telegram(article)
            email_ok = send_email(article)

            if telegram_ok or email_ok:
                sent += 1

        # Keep the most recently seen article IDs.
        if len(updated_seen) > MAX_STORED_ARTICLES:
            updated_seen = dict(
                sorted(
                    updated_seen.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )[:MAX_STORED_ARTICLES]
            )

        state["seen_articles"] = updated_seen
        state["initialized"] = True

        with state_lock:
            save_state(state)

        status["matches_detected"] = matches
        status["notifications_sent"] += sent

        status["last_success"] = datetime.now(
            timezone.utc
        ).isoformat()

        status["last_error"] = None

        logging.info(
            "Cycle complete: %d/%d sources succeeded; "
            "%d articles collected; %d matches; "
            "%d articles notified.",
            successful_sources,
            len(sources),
            len(all_articles),
            matches,
            sent,
        )

    except Exception as exc:
        status["last_error"] = str(exc)[:500]
        logging.exception("Monitoring cycle failed.")

    finally:
        cycle_lock.release()


def monitor_loop():
    logging.info("Aircraft safety monitoring loop started.")

    while True:
        started = time.monotonic()

        monitor_once()

        elapsed = time.monotonic() - started

        time.sleep(
            max(1, CHECK_INTERVAL - elapsed)
        )


# =========================================================
# 10. STATUS WEBPAGE AND HEALTH CHECK
# =========================================================

@app.route("/")
def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Aircraft Safety Monitor</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 850px;
                margin: 35px auto;
                padding: 20px;
                background: #f3f6fa;
                color: #182333;
            }
            .card {
                background: white;
                border-radius: 12px;
                padding: 22px;
                margin-bottom: 18px;
                box-shadow: 0 2px 12px #00000010;
            }
            h1 { color: #174b83; }
            .status { font-weight: bold; }
            pre {
                white-space: pre-wrap;
                overflow-wrap: anywhere;
            }
        </style>
    </head>
    <body>
        <div class="card">
            <h1>Aircraft Safety News Monitor</h1>
            <p>Monitoring status and notification service.</p>
            <p class="status" id="service">Loading status...</p>
        </div>

        <div class="card">
            <h2>Monitoring statistics</h2>
            <pre id="details">Loading...</pre>
            <button onclick="refreshStatus()">Refresh</button>
        </div>

        <div class="card">
            <h2>How it works</h2>
            <p>News sources are checked every five minutes.</p>
            <p>New article URLs are checked against aircraft safety keywords.</p>
            <p>Matching new articles can generate Telegram and email alerts.</p>
        </div>

        <script>
            async function refreshStatus() {
                try {
                    const response = await fetch('/status');
                    const data = await response.json();

                    document.getElementById('service').textContent =
                        data.monitor_started
                            ? 'Monitoring worker started'
                            : 'Monitoring worker not started';

                    document.getElementById('details').textContent =
                        JSON.stringify(data, null, 2);
                } catch (error) {
                    document.getElementById('service').textContent =
                        'Unable to load status';
                }
            }

            refreshStatus();
            setInterval(refreshStatus, 30000);
        </script>
    </body>
    </html>
    """


@app.route("/health")
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/status")
def get_status():
    result = dict(status)

    result["check_interval_seconds"] = CHECK_INTERVAL
    result["telegram_configured"] = bool(
        TELEGRAM_TOKEN and TELEGRAM_CHAT_ID
    )
    result["email_configured"] = bool(
        SMTP_USER and SMTP_PASSWORD and MAIL_TO
    )

    return jsonify(result), 200


# =========================================================
# 11. START BACKGROUND MONITOR
# =========================================================

def start_monitor():
    # Avoid starting a second monitor thread in the same process.
    if status["started"]:
        return

    status["started"] = True

    thread = threading.Thread(
        target=monitor_loop,
        name="aircraft-safety-monitor",
        daemon=True,
    )

    thread.start()


# Start when Gunicorn imports this module.
start_monitor()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True,
        use_reloader=False,
    )

import os
import re
import json
import time
import hashlib
import logging
import smtplib
import threading
import xml.etree.ElementTree as ET

from datetime import datetime, timezone, timedelta
from email.message import EmailMessage
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


# ---------------- CONFIGURATION ----------------

LINKS_FILE = "links.txt"
STATE_FILE = "state.json"

POLL_SECONDS = 120
REQUEST_TIMEOUT = 15
MAX_WORKERS = 12
MAX_ARTICLES_PER_FEED = 100

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
MAIL_TO = os.getenv("MAIL_TO")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

thread_local = threading.local()


# ---------------- AIRCRAFT SAFETY FILTER ----------------

AIRCRAFT_TERMS = [
    "aircraft", "airplane", "aeroplane", "plane", "airliner",
    "aviation", "aircraft engine", "flight crew", "pilot",
    "airline flight", "passenger jet", "cargo aircraft",
    "helicopter", "flight deck", "landing gear", "airbus",
    "boeing", "embraer", "bombardier", "flight",
]

SAFETY_TERMS = [
    "crash", "crashed", "accident", "incident",
    "emergency landing", "forced landing", "precautionary landing",
    "engine failure", "engine fire", "engine shutdown",
    "in-flight shutdown", "smoke in cabin", "cabin pressure",
    "loss of cabin pressure", "hydraulic failure",
    "landing gear failure", "flight control failure",
    "runway excursion", "runway overrun", "runway incursion",
    "bird strike", "hard landing", "tail strike",
    "mid-air collision", "near miss", "emergency declared",
    "mayday", "distress call", "aircraft damage",
    "structural damage", "windscreen cracked", "windshield cracked",
    "tire burst", "tyre burst", "engine problem",
    "technical fault", "technical failure", "mechanical failure",
    "diverted after", "emergency diversion",
    "airworthiness directive", "safety directive",
    "safety warning", "safety investigation",
    "aircraft grounded", "fleet grounded", "grounding",
    "takeoff aborted", "rejected takeoff",
    "loss of control", "overshot runway",
    "flight returned", "returned to airport",
    "emergency evacuation", "evacuated using slides",
    "fuel leak", "oil leak", "uncontained engine failure",
]

# These phrases already strongly imply an aircraft safety event.
SPECIFIC_SAFETY_TERMS = [
    "emergency landing", "forced landing", "engine failure",
    "engine fire", "in-flight shutdown", "cabin pressure",
    "hydraulic failure", "landing gear failure",
    "flight control failure", "runway excursion",
    "runway overrun", "runway incursion", "bird strike",
    "hard landing", "tail strike", "mid-air collision",
    "airworthiness directive", "aircraft grounded",
    "fleet grounded", "rejected takeoff", "loss of control",
    "emergency evacuation", "uncontained engine failure",
]


def is_aircraft_safety_news(title, summary, url=""):
    text = f"{title} {summary} {url}".lower()

    has_safety_term = any(term in text for term in SAFETY_TERMS)
    has_aircraft_term = any(term in text for term in AIRCRAFT_TERMS)
    has_specific_term = any(
        term in text for term in SPECIFIC_SAFETY_TERMS
    )

    return has_safety_term and (has_aircraft_term or has_specific_term)


# ---------------- HTTP ----------------

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


# ---------------- INPUT AND STATE ----------------

def load_sources():
    sources = []

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

            if url.startswith(("http://", "https://")):
                sources.append({
                    "label": label or urlparse(url).netloc,
                    "url": url,
                })

    return sources


def load_state():
    if not os.path.exists(STATE_FILE):
        return {
            "seen_articles": {},
            "feeds": {},
            "initialized": False,
        }

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as file:
            state = json.load(file)

        # Migrate the older monitor's state format.
        if "seen_articles" not in state:
            state = {
                "seen_articles": {},
                "feeds": {},
                "initialized": False,
            }

        state.setdefault("seen_articles", {})
        state.setdefault("feeds", {})
        state.setdefault("initialized", False)

        return state

    except (OSError, json.JSONDecodeError):
        logging.warning("Could not load state; creating a new state.")
        return {
            "seen_articles": {},
            "feeds": {},
            "initialized": False,
        }


def save_state(state):
    temp = STATE_FILE + ".tmp"

    with open(temp, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2, ensure_ascii=False)

    os.replace(temp, STATE_FILE)


# ---------------- FEED DISCOVERY ----------------

def local_name(tag):
    return tag.rsplit("}", 1)[-1].lower()


def is_feed_xml(content):
    try:
        root = ET.fromstring(content)
        names = {local_name(node.tag) for node in root.iter()}

        return bool(
            names.intersection({"rss", "feed", "rdf", "item", "entry"})
        )
    except ET.ParseError:
        return False


def discover_feed(homepage):
    """Try published RSS/Atom links and common feed paths."""
    candidates = []

    try:
        response = fetch(homepage)
        soup = BeautifulSoup(response.text, "html.parser")

        for element in soup.find_all("link", href=True):
            rel = element.get("rel", [])
            rel = [str(value).lower() for value in rel]

            feed_type = element.get("type", "").lower()

            if "alternate" in rel and (
                "rss" in feed_type
                or "atom" in feed_type
                or "xml" in feed_type
            ):
                candidates.append(urljoin(response.url, element["href"]))

    except Exception as error:
        logging.warning("Homepage unavailable: %s (%s)", homepage, error)

    base = homepage.rstrip("/")

    candidates.extend([
        base + "/feed",
        base + "/feed/",
        base + "/rss",
        base + "/rss/",
        base + "/rss.xml",
        base + "/feed.xml",
        base + "/news/rss",
        base + "/news/feed/",
    ])

    checked = set()

    for candidate in candidates:
        if candidate in checked:
            continue

        checked.add(candidate)

        try:
            response = fetch(candidate)

            if is_feed_xml(response.content):
                logging.info("Feed found: %s", candidate)
                return response.url

        except Exception:
            continue

    return None


# ---------------- FEED READING ----------------

def child_text(element, names):
    for child in element.iter():
        if local_name(child.tag) in names:
            text = "".join(child.itertext()).strip()

            if text:
                return text

    return ""


def entry_link(entry):
    # RSS link
    for child in list(entry):
        if local_name(child.tag) == "link":
            href = child.attrib.get("href")

            if href:
                if child.attrib.get("rel", "alternate") == "alternate":
                    return href

            text = "".join(child.itertext()).strip()

            if text.startswith(("http://", "https://")):
                return text

    # Atom ID may be a URL.
    for child in list(entry):
        if local_name(child.tag) == "id":
            value = "".join(child.itertext()).strip()

            if value.startswith(("http://", "https://")):
                return value

    return ""


def parse_feed(feed_url, source):
    response = fetch(feed_url)
    root = ET.fromstring(response.content)

    entries = [
        node for node in root.iter()
        if local_name(node.tag) in ("item", "entry")
    ]

    articles = []

    for entry in entries[:MAX_ARTICLES_PER_FEED]:
        title = child_text(entry, {"title"})
        link = entry_link(entry)

        if not link:
            continue

        link = urljoin(feed_url, link)

        summary = child_text(
            entry,
            {"description", "summary", "encoded", "content"},
        )

        summary = BeautifulSoup(
            summary, "html.parser"
        ).get_text(" ", strip=True)

        published = child_text(
            entry,
            {"pubdate", "published", "updated", "date", "modified"},
        )

        if not title:
            continue

        articles.append({
            "source": source["label"],
            "title": title.strip(),
            "url": link,
            "summary": summary[:3000],
            "published": published,
        })

    return articles


# ---------------- PAGE FALLBACK ----------------

def scrape_page_links(source):
    """Fallback for pages without a usable RSS/Atom feed."""
    response = fetch(source["url"])
    soup = BeautifulSoup(response.text, "html.parser")
    articles = []

    for anchor in soup.find_all("a", href=True):
        title = anchor.get_text(" ", strip=True)

        if len(title) < 15:
            continue

        link = urljoin(response.url, anchor["href"])

        if urlparse(link).scheme not in ("http", "https"):
            continue

        if not is_aircraft_safety_news(title, "", link):
            continue

        articles.append({
            "source": source["label"],
            "title": title,
            "url": link,
            "summary": "",
            "published": "",
        })

    return articles


def check_source(source, cached_feed):
    feed_url = cached_feed

    try:
        if feed_url:
            try:
                articles = parse_feed(feed_url, source)
                return source["url"], feed_url, articles, None
            except Exception as error:
                logging.warning(
                    "Cached feed failed for %s: %s",
                    source["label"], error
                )

        feed_url = discover_feed(source["url"])

        if feed_url:
            articles = parse_feed(feed_url, source)
            return source["url"], feed_url, articles, None

        articles = scrape_page_links(source)
        return source["url"], "", articles, None

    except Exception as error:
        return source["url"], feed_url or "", [], str(error)


# ---------------- PUBLICATION DATE ----------------

def parse_publication_date(value):
    if not value:
        return None

    try:
        result = parsedate_to_datetime(value)

        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)

        return result.astimezone(timezone.utc)

    except (TypeError, ValueError, OverflowError):
        pass

    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))

        if result.tzinfo is None:
            result = result.replace(tzinfo=timezone.utc)

        return result.astimezone(timezone.utc)

    except (TypeError, ValueError, OverflowError):
        return None


def article_id(article):
    canonical = article["url"].split("#", 1)[0].rstrip("/")
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------- ALERTS ----------------

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram secrets are missing.")
        return False

    url = (
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    )

    try:
        response = requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message[:4000],
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        response.raise_for_status()
        return response.json().get("ok", False)

    except Exception as error:
        logging.error("Telegram send failed: %s", error)
        return False


def send_email(subject, body):
    if not all([SMTP_USER, SMTP_PASSWORD, MAIL_TO]):
        logging.error("Email secrets are missing.")
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = SMTP_USER
    message["To"] = MAIL_TO
    message.set_content(body)

    try:
        with smtplib.SMTP_SSL(
            SMTP_HOST, SMTP_PORT, timeout=20
        ) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(message)

        return True

    except Exception as error:
        logging.error("Email send failed: %s", error)
        return False


def notify(article):
    published = article.get("published") or "Not provided by source"

    message = (
        "🚨 AIRCRAFT SAFETY NEWS\n\n"
        f"Source: {article['source']}\n"
        f"Headline: {article['title']}\n"
        f"Published: {published}\n\n"
        f"Summary: {article.get('summary', '')[:1000]}\n\n"
        f"Article: {article['url']}\n\n"
        "Please verify the report with the original source."
    )

    telegram_ok = send_telegram(message)

    email_ok = send_email(
        f"AIRCRAFT SAFETY ALERT: {article['title'][:150]}",
        message,
    )

    logging.info(
        "Alert result: Telegram=%s Email=%s",
        telegram_ok, email_ok
    )


# ---------------- CONTINUOUS MONITOR ----------------

def run_cycle(state):
    sources = load_sources()

    if not sources:
        logging.error("No valid sources in links.txt.")
        return

    logging.info("Checking %d sources.", len(sources))

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                check_source,
                source,
                state["feeds"].get(source["url"]),
            ): source
            for source in sources
        }

        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as error:
                logging.error("Source worker failed: %s", error)

    now = datetime.now(timezone.utc)
    initial_run = not state.get("initialized", False)
    seen = state["seen_articles"]
    new_alerts = []

    for source_url, feed_url, articles, error in results:
        if feed_url:
            state["feeds"][source_url] = feed_url

        if error:
            logging.warning(
                "Source failed: %s — %s", source_url, error
            )
            continue

        for article in articles:
            if not is_aircraft_safety_news(
                article["title"],
                article.get("summary", ""),
                article["url"],
            ):
                continue

            key = article_id(article)

            if key in seen:
                continue

            # On the first launch, alert only for matching stories
            # published in the last 30 minutes. Older items become
            # the baseline, avoiding a large historical alert burst.
            if initial_run:
                published = parse_publication_date(
                    article.get("published", "")
                )

                if published is None or now - timedelta(minutes=30) > published:
                    seen[key] = now.isoformat()
                    continue

            seen[key] = now.isoformat()
            new_alerts.append(article)

    state["initialized"] = True

    # Keep state bounded.
    if len(seen) > 20000:
        newest = sorted(
            seen.items(),
            key=lambda item: item[1],
            reverse=True,
        )[:15000]
        state["seen_articles"] = dict(newest)

    save_state(state)

    for article in new_alerts:
        notify(article)

    logging.info(
        "Cycle complete. New aircraft safety alerts: %d",
        len(new_alerts),
    )


def main():
    logging.info("Aircraft safety monitor starting.")
    logging.info("Polling interval: %d seconds", POLL_SECONDS)

    state = load_state()

    while True:
        started = time.monotonic()

        try:
            run_cycle(state)
        except Exception:
            logging.exception("Monitoring cycle failed.")

        elapsed = time.monotonic() - started
        time.sleep(max(5, POLL_SECONDS - elapsed))


if __name__ == "__main__":
    main()

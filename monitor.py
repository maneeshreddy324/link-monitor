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
from flask import Flask

# ---------------- CONFIGURATION ----------------

LINKS_FILE = "links.txt"
STATE_FILE = "state.json"

REQUEST_TIMEOUT = 15
MAX_WORKERS = 12
MAX_ARTICLES_PER_FEED = 100

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
MAIL_TO = os.getenv("MAIL_TO")
SMTP_HOST = os.getenv("SMTP_HOST", "://gmail.com")
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

def compile_terms(terms):
    # Whole-word match, allowing a plural ending (plane -> planes).
    return [
        re.compile(r"\b" + re.escape(term) + r"(?:s|es)?\b")
        for term in terms
    ]

AIRCRAFT_PATTERNS = compile_terms(AIRCRAFT_TERMS)
SAFETY_PATTERNS = compile_terms(SAFETY_TERMS)
SPECIFIC_PATTERNS = compile_terms(SPECIFIC_SAFETY_TERMS)

def matches_any(text, patterns):
    return any(pattern.search(text) for pattern in patterns)

def is_aircraft_safety_news(title, summary, url=""):
    text = f"{title} {summary} {url}".lower()

    has_safety_term = matches_any(text, SAFETY_PATTERNS)
    has_aircraft_term = matches_any(text, AIRCRAFT_PATTERNS)
    has_specific_term = matches_any(text, SPECIFIC_PATTERNS)

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
    if not os.path.exists(LINKS_FILE):
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

            if url.startswith(("http://", "https://")):
                sources.append({
                    "label": label or urlparse(url).netloc,
                    "url": url,
                })

    return sources

def new_state():
    return {
        "seen_articles": {},
        "feeds": {},
        "initialized": False,
    }

def load_state():
    if not os.path.exists(STATE_FILE):
        return new_state()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as file:
            state = json.load(file)

        state.setdefault("seen_articles", {})
        state.setdefault("feeds", {})
        state.setdefault("initialized", False)

        return state

    except (OSError, json.JSONDecodeError):
        logging.warning("Could not load state; creating a new state.")
        return new_state()

def save_state(state):
    temp = STATE_FILE + ".tmp"

    with open(temp, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2, ensure_ascii=False)

    os.replace(temp, STATE_FILE)

# ---------------- FEED PROCESSOR & TELEGRAM ----------------

def send_telegram(title, summary, url):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram configuration missing. Skipping message.")
        return
    
    message = f"🚨 <b>Aircraft Safety Update</b>\n\n<b>Title:</b> {title}\n\n<b>Summary:</b> {summary}\n\n<a href='{url}'>Read full article</a>"
    telegram_url = f"https://telegram.org{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    
    try:
        res = requests.post(telegram_url, json=payload, timeout=10)
        res.raise_for_status()
        logging.info(f"Notification sent successfully for: {title[:30]}...")
    except Exception as e:
        logging.error(f"Failed to send Telegram notification: {e}")

def process_source(source, state):
    try:
        response = fetch(source["url"])
        soup = BeautifulSoup(response.text, "html.parser")
        
        found_articles = 0
        for anchor in soup.find_all("a", href=True):
            if found_articles >= MAX_ARTICLES_PER_FEED:
                break
                
            title = anchor.get_text(strip=True)
            link = urljoin(source["url"], anchor["href"])
            
            if len(title) < 15 or not urlparse(link).scheme.startswith('http'):
                continue
                
            article_id = hashlib.md5(link.encode('utf-8')).hexdigest()
            
            if article_id not in state["seen_articles"]:
                if is_aircraft_safety_news(title, title, link):
                    logging.info(f"Match found: {title}")
                    if state.get("initialized", False):
                        send_telegram(title, "Aircraft safety event detected via tracking keywords.", link)
                
                state["seen_articles"][article_id] = datetime.now(timezone.utc).isoformat()
                found_articles += 1
                
    except Exception as e:
        logging.error(f"Error scraping resource {source['url']}: {e}")

# ---------------- RENDER COMPATIBILITY HUB & LOOP ----------------

app = Flask(__name__)

@app.route('/')
def health_check():
    return "Aircraft Safety Link Monitor is actively processing items!", 200

def run_continuous_monitor():
    logging.info("Continuous background worker initialization successful.")
    
    # Let the service boot cleanly before running the first validation pass
    time.sleep(5)
    
    while True:
        try:
            sources = load_sources()
            state = load_state()
            
            logging.info(f"Evaluating framework parameters across {len(sources)} tracking resources.")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(process_source, src, state) for src in sources]
                for future in as_completed(futures):
                    pass 
            
            state["initialized"] = True
            save_state(state)
            

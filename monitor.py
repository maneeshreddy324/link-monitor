import os
import re
import json
import time
import hashlib
import logging
import threading
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


# ================= SETTINGS (set these as Environment Variables on Render) =================

LINKS_URL = os.getenv("LINKS_URL", "")            # raw GitHub link of links.txt (optional)
LINKS_FILE = os.getenv("LINKS_FILE", "links.txt")  # used if LINKS_URL is empty or unreachable
STATE_FILE = os.getenv("STATE_FILE", "state.json") # on Render: /data/state.json (persistent disk)
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "12"))
MAX_MESSAGES_PER_CYCLE = int(os.getenv("MAX_MESSAGES_PER_CYCLE", "30"))
PAGE_CHANGE_ALERTS = os.getenv("PAGE_CHANGE_ALERTS", "true").lower() == "true"

# Leave KEYWORDS empty to get EVERY new article.
# Or set e.g. KEYWORDS=crash,emergency landing,engine failure to get only matching ones.
KEYWORDS = [k.strip().lower() for k in os.getenv("KEYWORDS", "").split(",") if k.strip()]

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

TRACKING_PARAMS = {"fbclid", "gclid", "igshid", "mc_cid", "mc_eid", "ref", "ref_src", "cmp", "ocid"}
RECHECK_FEED_SECONDS = 24 * 3600

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
thread_local = threading.local()


# ================= HTTP =================

def get_session():
    if not hasattr(thread_local, "session"):
        session = requests.Session()
        retry = Retry(
            total=2, connect=2, read=2, backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"], respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(HEADERS)
        thread_local.session = session
    return thread_local.session


def fetch(url, timeout=None):
    response = get_session().get(url, timeout=timeout or REQUEST_TIMEOUT, allow_redirects=True)
    response.raise_for_status()
    return response


# ================= LINKS AND STATE =================

def read_links_text():
    if LINKS_URL:
        try:
            r = requests.get(LINKS_URL, timeout=20, headers={"Cache-Control": "no-cache"})
            r.raise_for_status()
            return r.text
        except Exception as error:
            logging.warning("Could not read LINKS_URL (%s); using local file.", type(error).__name__)
    with open(LINKS_FILE, "r", encoding="utf-8-sig") as file:
        return file.read()


def load_sources():
    sources, seen_urls = [], set()
    for line in read_links_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "|" in line:
            label, url = [part.strip() for part in line.split("|", 1)]
        else:
            label, url = "", line
        if not url.startswith(("http://", "https://")) or url in seen_urls:
            continue
        seen_urls.add(url)
        sources.append({"label": label or urlparse(url).netloc, "url": url})
    return sources


def new_state():
    return {"sources": {}, "seen": {}, "pending": [], "welcome_sent": False}


def load_state():
    if not os.path.exists(STATE_FILE):
        return new_state()
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as file:
            state = json.load(file)
        base = new_state()
        for key, value in base.items():
            state.setdefault(key, value)
        return state
    except (OSError, json.JSONDecodeError):
        logging.warning("Could not read state file; starting a new one.")
        return new_state()


def save_state(state):
    folder = os.path.dirname(STATE_FILE)
    if folder:
        os.makedirs(folder, exist_ok=True)
    temp = STATE_FILE + ".tmp"
    with open(temp, "w", encoding="utf-8") as file:
        json.dump(state, file, ensure_ascii=False)
    os.replace(temp, STATE_FILE)


# ================= ARTICLE IDENTITY (this is what stops repeats) =================

def normalize_url(url):
    p = urlparse(url.strip())
    query = [
        (k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
        if not (k.lower().startswith("utm_") or k.lower() in TRACKING_PARAMS)
    ]
    path = p.path.rstrip("/") or "/"
    return urlunparse((p.scheme.lower(), p.netloc.lower(), path, "", urlencode(query), ""))


def article_id(article):
    key = normalize_url(article["url"])
    if article.get("sig"):
        key += "#" + article["sig"]
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def matches_keywords(article):
    if not KEYWORDS:
        return True
    text = f"{article['title']} {article.get('summary', '')}".lower()
    return any(word in text for word in KEYWORDS)


# ================= FEEDS =================

def local_name(tag):
    return tag.rsplit("}", 1)[-1].lower()


def is_feed_xml(content):
    try:
        root = ET.fromstring(content)
        names = {local_name(node.tag) for node in root.iter()}
        return bool(names & {"rss", "feed", "rdf", "item", "entry"})
    except ET.ParseError:
        return False


def child_text(element, names):
    for child in element.iter():
        if local_name(child.tag) in names:
            text = "".join(child.itertext()).strip()
            if text:
                return text
    return ""


def entry_link(entry):
    for child in list(entry):
        if local_name(child.tag) == "link":
            href = child.attrib.get("href")
            if href and child.attrib.get("rel", "alternate") == "alternate":
                return href
            text = "".join(child.itertext()).strip()
            if text.startswith(("http://", "https://")):
                return text
    for child in list(entry):
        if local_name(child.tag) == "id":
            value = "".join(child.itertext()).strip()
            if value.startswith(("http://", "https://")):
                return value
    return ""


def parse_feed_content(content, feed_url, source):
    root = ET.fromstring(content)
    entries = [n for n in root.iter() if local_name(n.tag) in ("item", "entry")]
    articles = []
    for entry in entries[:100]:
        title = child_text(entry, {"title"})
        link = entry_link(entry)
        if not title or not link:
            continue
        summary = child_text(entry, {"description", "summary", "encoded", "content"})
        summary = BeautifulSoup(summary, "html.parser").get_text(" ", strip=True)
        articles.append({
            "source": source["label"],
            "title": title.strip(),
            "url": urljoin(feed_url, link),
            "summary": summary[:2000],
        })
    return articles


def discover_feed(page_url, soup):
    candidates = []
    if soup is not None:
        for element in soup.find_all("link", href=True):
            rel = [str(v).lower() for v in element.get("rel", [])]
            kind = element.get("type", "").lower()
            if "alternate" in rel and ("rss" in kind or "atom" in kind or "xml" in kind):
                candidates.append(urljoin(page_url, element["href"]))
    p = urlparse(page_url)
    origin = f"{p.scheme}://{p.netloc}"
    here = page_url.split("?", 1)[0].rstrip("/")
    for base in (here, origin):
        for suffix in ("/feed", "/rss", "/rss.xml", "/feed.xml"):
            candidates.append(base + suffix)
    tried = set()
    for candidate in candidates:
        if candidate in tried:
            continue
        tried.add(candidate)
        try:
            response = fetch(candidate, timeout=10)
            if is_feed_xml(response.content):
                return response.url
        except Exception:
            continue
    return ""


# ================= PAGES WITHOUT A FEED =================

def headline_links(soup, base_url):
    host = urlparse(base_url).netloc.split(":")[0].lower()
    base = ".".join(host.split(".")[-2:])
    own = normalize_url(base_url)
    found = {}
    for anchor in soup.find_all("a", href=True):
        text = " ".join(anchor.get_text(" ", strip=True).split())
        if len(text) < 25 or len(text.split()) < 4:
            continue
        link = urljoin(base_url, anchor["href"])
        p = urlparse(link)
        if p.scheme not in ("http", "https"):
            continue
        if not p.netloc.split(":")[0].lower().endswith(base):
            continue
        if normalize_url(link) == own:
            continue
        found.setdefault(link, text)
    return found


def page_articles(response, source):
    soup = BeautifulSoup(response.text, "html.parser")
    title = (soup.title.string or "").strip() if soup.title else source["label"]
    links = headline_links(soup, response.url)
    if len(links) >= 5:  # a news list page: every headline is an article
        return [
            {"source": source["label"], "title": text, "url": link, "summary": ""}
            for link, text in links.items()
        ]
    if not PAGE_CHANGE_ALERTS:
        return []
    for tag in soup(["script", "style", "noscript", "nav", "footer", "header", "aside", "iframe", "form"]):
        tag.decompose()
    root = soup.find("article") or soup.find("main") or soup.body or soup
    text = re.sub(r"\s+", " ", root.get_text(" ")).strip()
    if len(text) < 50:
        raise ValueError("no readable content (login/blocked page)")
    sig = hashlib.sha256((title + "|" + text[:150000]).encode("utf-8")).hexdigest()[:16]
    return [{
        "source": source["label"], "title": title or source["url"],
        "url": source["url"], "summary": "", "sig": sig, "kind": "page",
    }]


# ================= ONE SOURCE =================

def check_source(source, src_state):
    url = source["url"]
    feed_url = src_state.get("feed", "")
    feed_checked = src_state.get("feed_checked", 0)
    try:
        if feed_url:
            try:
                response = fetch(feed_url)
                return {"url": url, "feed": feed_url, "feed_checked": feed_checked,
                        "articles": parse_feed_content(response.content, feed_url, source), "error": None}
            except Exception:
                feed_url = ""

        response = fetch(url)
        if is_feed_xml(response.content):
            return {"url": url, "feed": response.url, "feed_checked": time.time(),
                    "articles": parse_feed_content(response.content, response.url, source), "error": None}

        soup = BeautifulSoup(response.text, "html.parser")
        if time.time() - feed_checked > RECHECK_FEED_SECONDS:
            feed_checked = time.time()
            feed_url = discover_feed(response.url, soup)
            if feed_url:
                feed_response = fetch(feed_url)
                return {"url": url, "feed": feed_url, "feed_checked": feed_checked,
                        "articles": parse_feed_content(feed_response.content, feed_url, source), "error": None}

        return {"url": url, "feed": "", "feed_checked": feed_checked,
                "articles": page_articles(response, source), "error": None}
    except Exception as error:
        return {"url": url, "feed": feed_url, "feed_checked": feed_checked,
                "articles": [], "error": f"{type(error).__name__}: {str(error)[:120]}"}


# ================= TELEGRAM =================

def send_telegram(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.error("Telegram secrets are missing.")
        return False
    api = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    for _ in range(3):
        try:
            r = requests.post(
                api,
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000], "disable_web_page_preview": True},
                timeout=20,
            )
            if r.status_code == 429:
                wait = r.json().get("parameters", {}).get("retry_after", 5)
                time.sleep(min(wait + 1, 60))
                continue
            r.raise_for_status()
            return True
        except Exception as error:
            # never log the exception text: it can contain the bot token
            logging.error("Telegram send failed (%s)", type(error).__name__)
            time.sleep(2)
    return False


def format_message(article):
    head = "🔄 Page updated" if article.get("kind") == "page" else "🚨 New article"
    return f"{head}\n\n{article['title']}\n\nSource: {article['source']}\n{article['url']}"


def flush_pending(state):
    sent = 0
    while state["pending"] and sent < MAX_MESSAGES_PER_CYCLE:
        if not send_telegram(format_message(state["pending"][0])):
            logging.warning("Telegram failed; the alert stays queued and will be retried.")
            break
        state["pending"].pop(0)   # removed only AFTER a successful send
        sent += 1
        save_state(state)
        time.sleep(1.2)
    if state["pending"]:
        logging.info("%d alert(s) still waiting for the next cycle.", len(state["pending"]))
    return sent


# ================= ONE CHECK CYCLE =================

def run_cycle(state):
    sources = load_sources()
    if not sources:
        logging.error("No valid links found.")
        return

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = [
            pool.submit(check_source, s, dict(state["sources"].get(s["url"], {})))
            for s in sources
        ]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as error:
                logging.error("Worker failed (%s)", type(error).__name__)

    now = datetime.now(timezone.utc).isoformat()
    seen = state["seen"]
    unreadable = queued = 0

    for r in results:
        src = state["sources"].setdefault(r["url"], {})
        if r["error"]:
            unreadable += 1
            src["fails"] = src.get("fails", 0) + 1
            logging.warning("UNREADABLE %s - %s", r["url"], r["error"])
            continue
        src["fails"] = 0
        src["feed"] = r["feed"]
        src["feed_checked"] = r["feed_checked"]
        first_time = not src.get("baselined")   # first read of a link = silent baseline
        for article in r["articles"]:
            aid = article_id(article)
            if aid in seen:
                continue
            seen[aid] = now                      # marked as seen immediately => never alerted twice
            if first_time:
                continue
            if matches_keywords(article):
                state["pending"].append(article)
                queued += 1
        src["baselined"] = True

    if len(seen) > 60000:
        keep = sorted(seen.items(), key=lambda item: item[1], reverse=True)[:40000]
        state["seen"] = dict(keep)

    save_state(state)
    sent = flush_pending(state)
    logging.info("Cycle done: %d links, %d unreadable, %d new, %d sent.",
                 len(sources), unreadable, queued, sent)


def main():
    logging.info("Link monitor starting. Interval: %ds. Keywords: %s",
                 POLL_SECONDS, ", ".join(KEYWORDS) or "none (all new articles)")
    state = load_state()

    if not state.get("welcome_sent"):
        count = len(load_sources())
        if send_telegram(f"✅ Link monitor is running and watching {count} links.\n"
                         "You will only get a message when something NEW appears."):
            state["welcome_sent"] = True
            save_state(state)

    while True:
        started = time.monotonic()
        try:
            run_cycle(state)
        except Exception as error:
            logging.error("Cycle failed (%s)", type(error).__name__)
        time.sleep(max(10, POLL_SECONDS - (time.monotonic() - started)))


if __name__ == "__main__":
    main()

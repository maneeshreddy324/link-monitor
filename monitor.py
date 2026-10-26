import os
import re
import json
import hashlib
import logging
import smtplib
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.message import EmailMessage
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

STATE_FILE = "state.json"
LINKS_FILE = "links.txt"

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT = os.getenv("TELEGRAM_CHAT_ID")

SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
MAIL_TO = os.getenv("MAIL_TO")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))

REQUEST_TIMEOUT = 25
MAX_WORKERS = 10
MAX_TELEGRAM_LENGTH = 4000
SIGNATURE_VERSION = 2

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

_thread_local = threading.local()


def get_session():
    if not hasattr(_thread_local, "session"):
        session = requests.Session()

        retry = Retry(
            total=3,
            connect=3,
            read=2,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=20,
            pool_maxsize=20,
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(HEADERS)

        _thread_local.session = session

    return _thread_local.session


def load_links():
    if not os.path.exists(LINKS_FILE):
        logging.error("%s was not found.", LINKS_FILE)
        return []

    links = []
    seen = set()

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
                label = ""
                url = line

            if not url.startswith(("http://", "https://")):
                logging.warning("Skipping invalid URL: %s", url)
                continue

            if url not in seen:
                links.append({
                    "url": url,
                    "label": label or urlparse(url).netloc,
                })
                seen.add(url)

    return links


def load_state():
    if not os.path.exists(STATE_FILE):
        return {}

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)

        return data if isinstance(data, dict) else {}

    except (json.JSONDecodeError, OSError) as error:
        logging.warning("Could not read state file: %s", error)
        return {}


def save_state(state):
    temporary_file = STATE_FILE + ".tmp"

    with open(temporary_file, "w", encoding="utf-8") as file:
        json.dump(state, file, indent=2, ensure_ascii=False)

    os.replace(temporary_file, STATE_FILE)


def normalize_text(value):
    if not value:
        return ""

    return re.sub(r"\s+", " ", str(value)).strip()


def get_meta_content(soup, *selectors):
    for selector in selectors:
        element = soup.select_one(selector)

        if element:
            content = (
                element.get("content")
                or element.get("datetime")
                or element.get_text(" ", strip=True)
            )

            if content:
                return normalize_text(content)

    return ""


def extract_jsonld_metadata(soup):
    metadata = []
    scripts = soup.find_all(
        "script",
        attrs={"type": re.compile(r"application/ld\+json", re.I)},
    )

    for script in scripts:
        raw = script.string or script.get_text()

        if not raw:
            continue

        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            continue

        objects = data if isinstance(data, list) else [data]
        index = 0

        while index < len(objects):
            obj = objects[index]
            index += 1

            if not isinstance(obj, dict):
                continue

            graph = obj.get("@graph")
            if isinstance(graph, list):
                objects.extend(
                    item for item in graph
                    if isinstance(item, dict)
                )

            article_type = obj.get("@type", "")
            if isinstance(article_type, list):
                article_type = " ".join(article_type)

            if any(
                word in str(article_type).lower()
                for word in ("article", "news", "blogposting")
            ):
                for key in (
                    "headline",
                    "datePublished",
                    "dateModified",
                    "description",
                ):
                    value = obj.get(key)

                    if isinstance(value, str) and value.strip():
                        metadata.append(
                            f"{key}: {normalize_text(value)}"
                        )

    return metadata


def extract_page_content(html, url):
    soup = BeautifulSoup(html, "html.parser")

    title = (
        get_meta_content(soup, 'meta[property="og:title"]')
        or get_meta_content(soup, 'meta[name="twitter:title"]')
        or normalize_text(
            soup.title.get_text(" ", strip=True)
            if soup.title else ""
        )
    )

    metadata = {
        "title": title,
        "description": get_meta_content(
            soup,
            'meta[name="description"]',
            'meta[property="og:description"]',
            'meta[name="twitter:description"]',
        ),
        "published": get_meta_content(
            soup,
            'meta[property="article:published_time"]',
            'meta[name="date"]',
            'meta[name="publish-date"]',
            'meta[itemprop="datePublished"]',
        ),
        "modified": get_meta_content(
            soup,
            'meta[property="article:modified_time"]',
            'meta[name="last-modified"]',
            'meta[itemprop="dateModified"]',
        ),
    }

    jsonld = extract_jsonld_metadata(soup)

    for element in soup.find_all([
        "script", "style", "noscript", "svg", "iframe",
        "nav", "footer", "header", "aside", "form",
        "button", "input", "select", "textarea",
    ]):
        element.decompose()

    for element in soup.find_all(
        attrs={
            "class": re.compile(
                r"advert|advertisement|cookie|popup|modal|"
                r"social-share|related-post|comment|sidebar",
                re.I,
            )
        }
    ):
        element.decompose()

    selectors = [
        "article",
        '[itemprop="articleBody"]',
        '[class*="article-body"]',
        '[class*="article-content"]',
        '[class*="story-body"]',
        '[class*="story-content"]',
        '[class*="post-content"]',
        '[class*="entry-content"]',
        '[id*="article-body"]',
        '[id*="article-content"]',
        "main",
        '[role="main"]',
    ]

    content = ""

    for selector in selectors:
        elements = soup.select(selector)

        if elements:
            candidate = normalize_text(
                " ".join(
                    element.get_text(" ", strip=True)
                    for element in elements
                )
            )

            if len(candidate) > len(content):
                content = candidate

            if len(content) >= 200:
                break

    if len(content) < 100:
        body = soup.body or soup
        content = normalize_text(body.get_text(" ", strip=True))

    combined_metadata = "\n".join(
        f"{key}: {value}"
        for key, value in metadata.items()
        if value
    )

    combined = "\n".join(
        part for part in [
            url,
            combined_metadata,
            "\n".join(jsonld),
            content,
        ]
        if part
    )

    combined = re.sub(
        r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
        r"(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?\b",
        "",
        combined,
    )

    signature = hashlib.sha256(
        combined.encode("utf-8", errors="ignore")
    ).hexdigest()

    return {
        "title": title or url,
        "description": metadata["description"],
        "published": metadata["published"],
        "modified": metadata["modified"],
        "signature": signature,
        "version": SIGNATURE_VERSION,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "content_length": len(content),
    }


def check_link(link):
    url = link["url"]

    try:
        response = get_session().get(
            url,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        response.raise_for_status()

        content_type = response.headers.get("Content-Type", "").lower()

        if "html" not in content_type and "xml" not in content_type:
            raise ValueError(
                f"Unsupported content type: "
                f"{content_type or 'unknown'}"
            )

        result = extract_page_content(response.text, url)
        result["url"] = url
        result["label"] = link["label"]

        logging.info("Checked: %s", url)
        return result

    except Exception as error:
        logging.warning("Failed to check %s: %s", url, error)

        return {
            "url": url,
            "label": link["label"],
            "error": str(error),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }


def send_telegram(message):
    if not TOKEN or not CHAT:
        logging.warning(
            "Telegram is not configured. Check GitHub repository secrets."
        )
        return False

    api_url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"

    chunks = [
        message[i:i + MAX_TELEGRAM_LENGTH]
        for i in range(0, len(message), MAX_TELEGRAM_LENGTH)
    ]

    success = True

    for chunk in chunks:
        try:
            response = requests.post(
                api_url,
                json={
                    "chat_id": CHAT,
                    "text": chunk,
                    "disable_web_page_preview": True,
                },
                timeout=20,
            )
            response.raise_for_status()

            if not response.json().get("ok", False):
                logging.error("Telegram API returned an unsuccessful response.")
                success = False

        except Exception as error:
            logging.error("Telegram notification failed: %s", error)
            success = False

    return success


def send_email(subject, body):
    if not all([SMTP_USER, SMTP_PASSWORD, MAIL_TO]):
        return False

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = SMTP_USER
    message["To"] = MAIL_TO
    message.set_content(body)

    try:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(message)

        return True

    except Exception as error:
        logging.error("Email notification failed: %s", error)
        return False


def format_change_message(changes):
    lines = ["🔔 ARTICLE UPDATE DETECTED", ""]

    for item in changes:
        lines.extend([
            f"📰 {item.get('label') or item['url']}",
            f"Title: {item.get('title', 'Unknown')}",
            f"Link: {item['url']}",
        ])

        if item.get("description"):
            lines.append(
                f"Description: {item['description'][:300]}"
            )

        if item.get("modified"):
            lines.append(f"Modified: {item['modified']}")

        lines.append("")

    lines.append(
        "The monitored page content or article metadata has changed."
    )

    return "\n".join(lines)


def main():
    links = load_links()

    if not links:
        logging.warning("No valid URLs found in links.txt.")
        return

    state = load_state()
    new_state = {}
    changes = []
    failures = []

    logging.info("Starting monitoring for %d URLs.", len(links))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(check_link, link): link
            for link in links
        }

        for future in as_completed(futures):
            result = future.result()
            url = result["url"]
            previous = state.get(url, {})

            if result.get("error"):
                failures.append(result)
                new_state[url] = previous or {
                    "error": result["error"],
                    "checked_at": result["checked_at"],
                }
                continue

            old_signature = previous.get("signature")
            new_signature = result["signature"]

            if not previous:
                logging.info("Baseline created: %s", url)

            elif (
                old_signature
                and previous.get("version") != SIGNATURE_VERSION
            ):
                logging.info("Baseline refreshed: %s", url)

            elif old_signature and old_signature != new_signature:
                logging.info("CHANGE DETECTED: %s", url)
                changes.append(result)

            new_state[url] = result

    save_state(new_state)

    if changes:
        message = format_change_message(changes)
        send_telegram(message)
        send_email("Article Update Detected", message)

    if failures and TOKEN and CHAT:
        failure_lines = [
            "⚠️ SOME MONITORED PAGES COULD NOT BE CHECKED",
            "",
        ]

        for item in failures[:15]:
            failure_lines.append(
                f"{item['url']}\nError: {item['error'][:250]}\n"
            )

        if len(failures) > 15:
            failure_lines.append(
                f"...and {len(failures) - 15} additional failures."
            )

        send_telegram("\n".join(failure_lines))

    logging.info(
        "Monitoring complete. Checked: %d | Changes: %d | Failures: %d",
        len(links),
        len(changes),
        len(failures),
    )


if __name__ == "__main__":
    main()

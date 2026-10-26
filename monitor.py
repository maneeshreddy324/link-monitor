import os, json
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

STATE_FILE = "state.json"
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT = os.getenv("TELEGRAM_CHAT_ID")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


def load_links():
    links = []
    with open("links.txt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "|" in line:
                name, url = [p.strip() for p in line.split("|", 1)]
            else:
                name, url = urlparse(line).netloc, line
            if url.startswith("http"):
                links.append((name, url))
    return links


def telegram(text):
    if not (TOKEN and CHAT):
        print("Telegram secrets missing")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            data={"chat_id": CHAT, "text": text[:4000],
                  "disable_web_page_preview": True},
            timeout=20,
        )
        if not r.ok:
            print("Telegram error", r.status_code, r.text[:200])
    except Exception as e:
        print("Telegram exception", e)


def check(item):
    name, url = item
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return name, url, None, f"HTTP {r.status_code}"
        soup = BeautifulSoup(r.text, "html.parser")
        host = urlparse(r.url).netloc.replace("www.", "")
        found = {}
        for a in soup.find_all("a", href=True):
            title = " ".join(a.get_text(" ", strip=True).split())
            if len(title) < 25:
                continue
            full = urljoin(r.url, a["href"]).split("#")[0]
            p = urlparse(full)
            if host not in p.netloc or len(p.path) < 10:
                continue
            found.setdefault(full, title)
            if len(found) >= 80:
                break
        if not found:
            return name, url, None, "no articles found"
        return name, url, found, None
    except Exception as e:
        return name, url, None, type(e).__name__


def main():
    try:
        with open(STATE_FILE) as f:
            state = json.load(f)
        if not isinstance(state, dict):
            state = {}
    except Exception:
        state = {}

    links = load_links()
    with ThreadPoolExecutor(max_workers=20) as ex:
        results = list(ex.map(check, links))

    changed, unreadable, alerts = 0, [], 0
    for name, url, found, err in results:
        if err:
            unreadable.append(f"{name} ({err})")
            continue
        entry = state.get(url)
        if not isinstance(entry, dict) or "seen" not in entry:
            state[url] = {"seen": list(found.keys())}  # baseline
            continue
        seen = set(entry["seen"])
        new = [(u, t) for u, t in found.items() if u not in seen]
        if new:
            changed += 1
            alerts += 1
            msg = f"🔔 NEW UPDATE: {name}\n"
            for u, t in new[:5]:
                msg += f"\n• {t}\n{u}\n"
            if len(new) > 5:
                msg += f"\n+{len(new) - 5} more on {url}"
            telegram(msg)
            entry["seen"] = list((seen | set(found.keys())))[-500:]
            state[url] = entry

    # one summary when the unreadable list changes
    if sorted(unreadable) != sorted(state.get("_unreadable", [])):
        if unreadable:
            telegram("⚠️ Cannot read these sites right now:\n" + "\n".join(unreadable))
        state["_unreadable"] = unreadable

    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

    print(f"Checked {len(links)} links, {changed} changed, {len(unreadable)} unreadable")
    for u in unreadable:
        print("  unreadable:", u)


if __name__ == "__main__":
    main()

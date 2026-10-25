import os, re, json, hashlib, smtplib, ssl
from concurrent.futures import ThreadPoolExecutor
from email.mime.text import MIMEText
import requests
from bs4 import BeautifulSoup

UA = {"User-Agent": "Mozilla/5.0 (compatible; LinkMonitor/1.0)"}
STATE = "state.json"

def load_links():
    out = []
    for line in open("links.txt", encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and line not in out:
            out.append(line)
    return out

def fetch(url):
    try:
        r = requests.get(url, headers=UA, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        title = (soup.title.string or "").strip() if soup.title else ""
        for t in soup(["script", "style", "noscript", "nav", "footer", "header", "aside", "iframe", "form"]):
            t.decompose()
        root = soup.find("article") or soup.find("main") or soup.body or soup
        text = re.sub(r"\s+", " ", root.get_text(" ")).strip()
        if len(text) < 50:
            raise ValueError("no readable content (login/blocked page)")
        sig = hashlib.sha256((title + "|" + text).encode()).hexdigest()
        return url, {"sig": sig, "title": title, "err": None}
    except Exception as e:
        return url, {"err": str(e)[:150]}

def send_telegram(msg):
    tok, chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if tok and chat:
        for i in range(0, len(msg), 3800):
            requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          data={"chat_id": chat, "text": msg[i:i+3800], "disable_web_page_preview": "true"}, timeout=20)

def send_email(subject, msg):
    user, pw, to = os.getenv("SMTP_USER"), os.getenv("SMTP_PASSWORD"), os.getenv("MAIL_TO")
    if user and pw and to:
        m = MIMEText(msg)
        m["Subject"], m["From"], m["To"] = subject, user, to
        with smtplib.SMTP_SSL(os.getenv("SMTP_HOST", "smtp.gmail.com"), 465, context=ssl.create_default_context()) as s:
            s.login(user, pw)
            s.sendmail(user, [x.strip() for x in to.split(",")], m.as_string())

def main():
    links = load_links()
    state = json.load(open(STATE)) if os.path.exists(STATE) else {}
    with ThreadPoolExecutor(15) as ex:
        results = list(ex.map(fetch, links))
    changed, failed = [], 0
    for url, res in results:
        old = state.get(url, {})
        if res["err"]:
            failed += 1
            old["err"] = res["err"]
            state[url] = old
            continue
        if old.get("sig") and old["sig"] != res["sig"]:
            changed.append((url, res["title"] or url))
        state[url] = res
    state = {u: v for u, v in state.items() if u in links}
    json.dump(state, open(STATE, "w"), indent=1)
    print(f"Checked {len(links)} links, {len(changed)} changed, {failed} unreadable")
    if changed:
        body = "New update detected on:\n\n" + "\n\n".join(f"{t}\n{u}" for u, t in changed)
        send_telegram("\U0001F6A8 " + body)
        send_email(f"Update alert: {len(changed)} link(s) changed", body)

main()

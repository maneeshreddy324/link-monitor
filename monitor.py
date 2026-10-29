import os
import time
import hashlib
import requests
import io
import csv
from bs4 import BeautifulSoup
from pypdf import PdfReader
import openpyxl
import redis

# 1. Initialization and Core Connections
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
REDIS_URL = os.getenv("REDIS_URL")

# Connect to Redis Memory (Fallback to internal RAM dict if variables are missing)
if REDIS_URL:
    try:
        db = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        print("💾 Connected successfully to Render Redis permanent memory database.")
    except Exception as e:
        print(f"⚠️ Redis connection failed: {e}. Falling back to temporary local memory.")
        db = {}
else:
    print("⚠️ REDIS_URL environment variable missing. Falling back to temporary local memory.")
    db = {}

def is_duplicate(unique_key):
    """Checks if this specific item fingerprint has been broadcasted before."""
    if isinstance(db, redis.Redis):
        if db.exists(unique_key):
            return True
        # Store hash in Redis for 30 days (2592000 seconds) to save space
        db.setex(unique_key, 2592000, "sent")
        return False
    else:
        if unique_key in db:
            return True
        db[unique_key] = "sent"
        return False

def send_telegram_alert(message):
    """Dispatches unique content blocks directly to Telegram."""
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ Telegram configuration environment variables missing.")
        return
    url = f"https://telegram.org{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"❌ Failed to dispatch message to Telegram: {e}")

def create_hash(text):
    """Generates an explicit signature string for any arbitrary data block."""
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

# 2. Universal Multi-Document Smart Reader Logic
def parse_and_notify_document(url, label="Airway Update"):
    """Downloads and unifies standard text, HTML, RSS feeds, PDFs, CSVs, or Excel targets."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        content_type = response.headers.get('Content-Type', '').lower()
        
        extracted_items = []  # Tuples of (title/text, deep_link)

        # CASE A: Portable Document Format (.pdf)
        if "application/pdf" in content_type or url.lower().endswith(".pdf"):
            with io.BytesIO(response.content) as f:
                reader = PdfReader(f)
                full_pdf_text = "".join([page.extract_text() for page in reader.pages if page.extract_text()])
                lines = [line.strip() for line in full_pdf_text.split('\n') if len(line.strip()) > 15]
                # Extract first few descriptive structural content blocks
                for line in lines[:10]:
                    extracted_items.append((f"PDF Record: {line}", url))

        # CASE B: Microsoft Excel Sheets (.xlsx)
        elif "spreadsheet" in content_type or url.lower().endswith(".xlsx"):
            wb = openpyxl.load_workbook(io.BytesIO(response.content), data_only=True)
            sheet = wb.active
            for row in sheet.iter_rows(values_only=True):
                row_str = " | ".join([str(cell) for cell in row if cell is not None])
                if len(row_str.strip()) > 10:
                    extracted_items.append((f"Spreadsheet Row: {row_str}", url))

        # CASE C: Comma Separated Tables (.csv)
        elif "text/csv" in content_type or url.lower().endswith(".csv"):
            csv_text = response.content.decode('utf-8-sig', errors='ignore')
            reader = csv.reader(io.StringIO(csv_text))
            for row in reader:
                row_str = " , ".join(row)
                if len(row_str.strip()) > 10:
                    extracted_items.append((f"CSV Row: {row_str}", url))

        # CASE D: XML Feeds / Official RSS Layouts
        elif "xml" in content_type or "rss" in content_type:
            soup = BeautifulSoup(response.content, "xml")
            items = soup.find_all(["item", "entry"])
            for item in items:
                title = item.find("title").get_text().strip() if item.find("title") else "Untitled Notification"
                link_tag = item.find("link")
                link = link_tag.get_text().strip() if link_tag else url
                if link_tag and link_tag.has_attr('href'):
                    link = link_tag['href']
                extracted_items.append((title, link))

        # CASE E: Traditional HTML Web Pages
        else:
            soup = BeautifulSoup(response.content, "html.parser")
            # Clear noise tags to find clean structural context
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Extract links with meaningful text strings
            for a_tag in soup.find_all("a", href=True):
                text = a_tag.get_text().strip()
                if len(text) > 12:
                    link = a_tag['href'] if a_tag['href'].startswith('http') else f"{url.rstrip('/')}/{a_tag['href'].lstrip('/')}"
                    extracted_items.append((text, link))

        # 3. Duplicate Prevention Verification Cycle
        new_updates_sent = 0
        for title_text, destination_url in extracted_items:
            # Create unique fingerprint matching both title text and target URL
            fingerprint = create_hash(f"{title_text}{destination_url}")
            
            if not is_duplicate(fingerprint):
                alert_body = f"🔔 <b>{label}</b>\n\n{title_text}\n\n🔗 <a href='{destination_url}'>Source Link</a>"
                send_telegram_alert(alert_body)
                new_updates_sent += 1
                time.sleep(1.5) # Anti-flood interval throttle
                
        if new_updates_sent == 0:
            print(f"ℹ️ Scanned {label}: No new updates found. Skipping duplicates.")

    except Exception as e:
        # Fallback alert: Sends the notification even if a private or broken link fails entirely
        error_fingerprint = create_hash(f"error-{url}-{str(e)[:30]}")
        if not is_duplicate(error_fingerprint):
            error_alert = f"⚠️ <b>PRIVATE/UNREADABLE AIRWAY SOURCE</b>\n\n<b>Source:</b> {label}\n<b>URL:</b> {url}\n<b>Error Details:</b> {str(e)}"
            send_telegram_alert(error_alert)

# 4. Monitored Portfolios Execution Engine
if __name__ == "__main__":
    print("🚀 Running complete multi-document airway monitor...")
    
    # Target configurations (Provide any site, XML feed, PDF link, CSV, or spreadsheet)
    targets = [
        {"url": "https://aviation-safety.net", "label": "Aviation Safety Net"},
        {"url": "https://faa.gov", "label": "FAA Newsroom Feed"},
        # You can append raw doc references at any time:
        # {"url": "https://example.com", "label": "CSV Metrics"},
        # {"url": "https://example.com", "label": "PDF Briefing"}
    ]
    
    for target in targets:
        parse_and_notify_document(target["url"], target["label"])
        
    print("✅ Run execution sequence finished.")

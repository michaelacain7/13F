import os
import re
import json
import time
import logging
import threading
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from flask import Flask, request, jsonify, Response
from apscheduler.schedulers.background import BackgroundScheduler

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Flask App ────────────────────────────────────────────────────────────────
app = Flask(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
DISCORD_WEBHOOKS = [
    "https://discordapp.com/api/webhooks/1470074345291382976/oF9qwznv_KyLGje4mwPL-brqsqPeJbIChKN081BiglQx2DXYmG8pXPSn7hakoqynuLqZ",
    "https://discordapp.com/api/webhooks/919672540237017138/Zga2QHBVwPUKXbCMNQ6hRXSsJaW8d136pOZNheRz1SK0YS5GIRnpjsGdN7trPul-zeXo",
]

SEC_HEADERS = {
    "User-Agent": "13FMonitor/1.0 (monitoring@example.com)",
    "Accept-Encoding": "gzip, deflate",
}

EST = ZoneInfo("America/New_York")
POLL_INTERVAL_SECONDS = 3  # Fast polling - uses single-request EDGAR feed

# ── In-memory state ──────────────────────────────────────────────────────────
monitored_filers = {}
alert_log = []
monitor_status = {
    "running": False,
    "last_check": None,
    "checks_today": 0,
    "errors": [],
}

# ── Persistence ──────────────────────────────────────────────────────────────
# Railway Volume should be mounted at /data. Falls back to local dir.
PERSIST_DIR = os.environ.get("PERSIST_DIR", "/data")
PERSIST_FILE = os.path.join(PERSIST_DIR, "13f_state.json")
_save_lock = threading.Lock()


def save_state():
    """Save monitored filers and alert log to disk."""
    try:
        os.makedirs(PERSIST_DIR, exist_ok=True)

        # Build serializable filer data (strip large holdings to keep file small)
        filers_save = {}
        for cik, data in monitored_filers.items():
            filers_save[cik] = {
                "name": data.get("name", ""),
                "cik": cik,
                "url": data.get("url", ""),
                "last_filing_accession": data.get("last_filing_accession"),
                "last_filing_date": data.get("last_filing_date"),
                "total_holdings": data.get("total_holdings", 0),
                "is_default": data.get("is_default", False),
                "user_added": data.get("user_added", False),
            }

        state = {
            "saved_at": datetime.now(EST).isoformat(),
            "filers": filers_save,
            "alert_log": alert_log[:100],
        }

        with _save_lock:
            tmp = PERSIST_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(state, f, indent=2)
            os.replace(tmp, PERSIST_FILE)

        log.info(f"State saved: {len(filers_save)} filers")
    except Exception as e:
        log.error(f"Failed to save state: {e}")


_state_loaded = False

def load_state():
    """Load saved state from disk. Returns True if state was loaded."""
    global alert_log, _state_loaded
    if _state_loaded:
        return False
    _state_loaded = True
    try:
        if not os.path.exists(PERSIST_FILE):
            log.info("No saved state found, starting fresh.")
            return False

        with open(PERSIST_FILE, "r") as f:
            state = json.load(f)

        saved_filers = state.get("filers", {})
        saved_alerts = state.get("alert_log", [])

        for cik, data in saved_filers.items():
            if cik not in monitored_filers:
                monitored_filers[cik] = {
                    "name": data.get("name", "Unknown"),
                    "cik": cik,
                    "url": data.get("url", ""),
                    "last_filing_accession": data.get("last_filing_accession"),
                    "last_filing_date": data.get("last_filing_date"),
                    "holdings_prev": {},
                    "holdings_current": {},
                    "total_holdings": data.get("total_holdings", 0),
                    "last_changes": None,
                    "is_default": data.get("is_default", False),
                    "user_added": data.get("user_added", False),
                    "_needs_init": True,
                }

        alert_log = saved_alerts[:100]

        log.info(f"State loaded: {len(saved_filers)} filers, {len(saved_alerts)} alerts (saved {state.get('saved_at', '?')})")
        return True
    except Exception as e:
        log.error(f"Failed to load state: {e}")
        return False

# ── Default Filers (Buzzy Hedge Funds & Notable 13F Filers) ──────────────────
DEFAULT_FILERS = [
    # Mega Hedge Funds
    ("1067983", "BERKSHIRE HATHAWAY INC"),            # Warren Buffett
    ("1649339", "Scion Asset Management, LLC"),        # Michael Burry
    ("1040273", "THIRD POINT LLC"),                    # Dan Loeb
    ("1336528", "Pershing Square Capital Management"), # Bill Ackman
    ("1350694", "BRIDGEWATER ASSOCIATES, LP"),         # Ray Dalio
    ("1423053", "Citadel Advisors LLC"),               # Ken Griffin
    ("1037389", "RENAISSANCE TECHNOLOGIES LLC"),       # Jim Simons
    ("1656456", "APPALOOSA LP"),                       # David Tepper
    ("1167483", "Tiger Global Management LLC"),        # Chase Coleman
    ("921669",  "ICAHN CARL C"),                       # Carl Icahn
    ("1079114", "GREENLIGHT CAPITAL INC"),             # David Einhorn
    ("1603466", "Point72 Asset Management, L.P."),     # Steve Cohen
    ("1536411", "DUQUESNE FAMILY OFFICE LLC"),         # Stanley Druckenmiller
    ("1535392", "Coatue Management LLC"),              # Philippe Laffont
    ("1061768", "BAUPOST GROUP LLC"),                  # Seth Klarman
    ("1048445", "ELLIOTT INVESTMENT MANAGEMENT L.P."), # Paul Singer
    ("1061165", "LONE PINE CAPITAL LLC"),              # Stephen Mandel
    ("1103804", "VIKING GLOBAL INVESTORS LP"),         # Andreas Halvorsen
    ("1273087", "MILLENNIUM MANAGEMENT LLC"),          # Israel Englander
    ("1159159", "MAVERICK CAPITAL LTD"),               # Lee Ainslie
    ("1541996", "D E SHAW & CO INC"),                  # D.E. Shaw
    ("1364742", "JANA PARTNERS LLC"),                  # Barry Rosenstein
    ("102909",  "VALUE ACT CAPITAL"),                  # Jeff Ubben / Mason Morfit
    ("1608702", "TWO SIGMA INVESTMENTS LP"),           # John Overdeck & David Siegel
    ("1279708", "STARBOARD VALUE LP"),                 # Jeff Smith
    ("1510387", "AQR CAPITAL MANAGEMENT LLC"),         # Cliff Asness
    ("1009207", "PAULSON & CO INC"),                   # John Paulson
    ("1484148", "ARK INVESTMENT MANAGEMENT LLC"),      # Cathie Wood
    ("1582202", "WHALE ROCK CAPITAL MANAGEMENT LLC"),  # Alex Sacerdote
    ("812295",  "SOROS FUND MANAGEMENT LLC"),          # George Soros
]

# ── SEC EDGAR Helpers ────────────────────────────────────────────────────────

def pad_cik(cik):
    return str(cik).zfill(10)


def extract_cik_from_url(url):
    patterns = [
        r'CIK=(\d+)',
        r'CIK=0*(\d+)',
        r'/cik(\d+)',
        r'/(\d{7,10})/',
    ]
    for p in patterns:
        m = re.search(p, url, re.IGNORECASE)
        if m:
            return m.group(1).lstrip('0') or '0'
    return None


# ── Cached company tickers data ──────────────────────────────────────────────
_tickers_cache = {"data": None, "loaded_at": None}
_tickers_lock = threading.Lock()


def get_company_tickers():
    """Fetch and cache SEC company_tickers.json (refreshed every 24h)."""
    now = time.time()
    with _tickers_lock:
        if _tickers_cache["data"] and _tickers_cache["loaded_at"] and (now - _tickers_cache["loaded_at"]) < 86400:
            return _tickers_cache["data"]
    try:
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=SEC_HEADERS, timeout=20
        )
        if r.status_code == 200:
            data = r.json()
            # Build lookup structures
            by_ticker = {}
            by_cik = {}
            entries = []
            for key, entry in data.items():
                title = entry.get("title", "")
                ticker = entry.get("ticker", "")
                cik = str(entry.get("cik_str", "")).lstrip("0") or "0"
                rec = {"cik": cik, "name": title, "ticker": ticker}
                entries.append(rec)
                if ticker:
                    by_ticker[ticker.upper()] = rec
                by_cik[cik] = rec

            cache_obj = {"entries": entries, "by_ticker": by_ticker, "by_cik": by_cik}
            with _tickers_lock:
                _tickers_cache["data"] = cache_obj
                _tickers_cache["loaded_at"] = now
            log.info(f"Loaded {len(entries)} companies from SEC tickers file")
            return cache_obj
    except Exception as e:
        log.warning(f"Failed to fetch company_tickers.json: {e}")

    return _tickers_cache["data"]  # Return stale cache if available


def search_sec_companies(query):
    """Search SEC EDGAR for companies/filers by name or ticker."""
    results = []
    seen_ciks = set()
    query_lower = query.lower().strip()
    query_upper = query.upper().strip()

    # ── Step 1: Search cached company tickers (fast, has every public company) ──
    tickers = get_company_tickers()
    if tickers:
        # 1a: Exact ticker match (highest priority)
        exact_ticker = tickers["by_ticker"].get(query_upper)
        if exact_ticker:
            cik = exact_ticker["cik"]
            seen_ciks.add(cik)
            results.append({
                "cik": cik,
                "name": f"{exact_ticker['name']} ({exact_ticker['ticker']})",
                "has_13f": None,
                "_score": 0,  # Best possible score
            })

        # 1b: Name and partial ticker matches
        for rec in tickers["entries"]:
            cik = rec["cik"]
            if cik in seen_ciks:
                continue
            name_lower = rec["name"].lower()
            ticker_lower = rec["ticker"].lower()

            score = None
            # Exact name match
            if query_lower == name_lower:
                score = 1
            # Name starts with query
            elif name_lower.startswith(query_lower):
                score = 2
            # Ticker starts with query (for partial ticker searches)
            elif ticker_lower.startswith(query_lower) and len(query_lower) >= 2:
                score = 3
            # Query is a significant part of the name (word boundary match)
            elif f" {query_lower}" in f" {name_lower}" or f" {query_lower}" in f" {name_lower}":
                score = 4
            # Name contains query anywhere
            elif query_lower in name_lower:
                score = 5

            if score is not None:
                seen_ciks.add(cik)
                display_name = f"{rec['name']} ({rec['ticker']})" if rec['ticker'] else rec['name']
                results.append({
                    "cik": cik,
                    "name": display_name,
                    "has_13f": None,
                    "_score": score,
                })
                if len(results) >= 30:
                    break

    # ── Step 2: EDGAR EFTS search for 13F filers (catches hedge funds not in tickers) ──
    try:
        efts_url = (
            f"https://efts.sec.gov/LATEST/search-index?"
            f"q=%22{requests.utils.quote(query)}%22&"
            f"forms=13F-HR,13F-HR%2FA&dateRange=custom&from=0&size=20"
        )
        r = requests.get(efts_url, headers=SEC_HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            for hit in data.get("hits", {}).get("hits", []):
                source = hit.get("_source", {})
                entity_name = source.get("entity_name", "")
                ciks = source.get("ciks", [])
                if ciks and entity_name:
                    cik = str(ciks[0]).lstrip("0") or "0"
                    if cik not in seen_ciks:
                        seen_ciks.add(cik)
                        name_lower = entity_name.lower()
                        score = 2 if name_lower.startswith(query_lower) else 4 if query_lower in name_lower else 6
                        results.append({"cik": cik, "name": entity_name, "has_13f": True, "_score": score})
    except Exception as e:
        log.warning(f"EFTS search failed: {e}")

    # ── Step 3: Browse-edgar Atom for 13F filers ──
    try:
        browse_url = (
            f"https://www.sec.gov/cgi-bin/browse-edgar?"
            f"company={requests.utils.quote(query)}&CIK=&type=13F-HR&dateb=&"
            f"owner=include&count=20&search_text=&action=getcompany&output=atom"
        )
        r = requests.get(browse_url, headers=SEC_HEADERS, timeout=15)
        if r.status_code == 200:
            content = re.sub(r'\sxmlns[^"]*"[^"]*"', '', r.text)
            try:
                root = ET.fromstring(content)
                for entry in root.iter('entry'):
                    cik_el = entry.find('.//cik')
                    cik = None
                    if cik_el is not None and cik_el.text:
                        cik = cik_el.text.strip().lstrip("0") or "0"
                    if not cik:
                        link_el = entry.find('.//link')
                        if link_el is not None:
                            m = re.search(r'CIK=0*(\d+)', link_el.get('href', ''))
                            if m:
                                cik = m.group(1).lstrip("0") or "0"
                    name = ""
                    for tag in ['company-name', 'title']:
                        name_el = entry.find(f'.//{tag}')
                        if name_el is not None and name_el.text:
                            name = re.sub(r'^\d+\s*-\s*', '', name_el.text.strip())
                            name = re.sub(r'\s*\(.*?\)\s*$', '', name)
                            if name:
                                break
                    if cik and name and cik not in seen_ciks:
                        seen_ciks.add(cik)
                        name_lower = name.lower()
                        score = 2 if name_lower.startswith(query_lower) else 4 if query_lower in name_lower else 6
                        results.append({"cik": cik, "name": name, "has_13f": True, "_score": score})
            except ET.ParseError:
                pass
    except Exception as e:
        log.warning(f"Browse-edgar Atom search failed: {e}")

    # ── Step 4: Verify 13F status for top ticker results (up to 5) ──
    unknown_results = [r for r in results if r.get("has_13f") is None]
    # Sort unknowns by score so we verify the best matches first
    unknown_results.sort(key=lambda x: x.get("_score", 99))
    for r_item in unknown_results[:5]:
        try:
            padded = pad_cik(r_item["cik"])
            url = f"https://data.sec.gov/submissions/CIK{padded}.json"
            resp = requests.get(url, headers=SEC_HEADERS, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                forms = data.get("filings", {}).get("recent", {}).get("form", [])
                r_item["has_13f"] = any(f in ("13F-HR", "13F-HR/A") for f in forms)
                official_name = data.get("name", "")
                if official_name and "(" in r_item["name"]:
                    ticker_part = r_item["name"].split("(")[-1].rstrip(")")
                    r_item["name"] = f"{official_name} ({ticker_part})"
            else:
                r_item["has_13f"] = False
        except:
            r_item["has_13f"] = False
        time.sleep(0.12)

    # Mark remaining unknowns
    for r_item in results:
        if r_item.get("has_13f") is None:
            r_item["has_13f"] = False  # Unknown, assume no

    # ── Sort: score first (best match), then 13F filers before non-filers ──
    def sort_key(r):
        score = r.get("_score", 99)
        has_13f = 0 if r["has_13f"] else 1
        # Prefer entries with tickers (real public companies)
        has_ticker = 0 if "(" in r["name"] else 1
        # Prefer shorter names (more likely to be the obvious match)
        name_len = len(r["name"])
        return (score, has_ticker, has_13f, name_len)

    results.sort(key=sort_key)

    # Clean up internal fields
    for r in results:
        r.pop("_score", None)

    return results[:15]


def get_company_info(cik):
    padded = pad_cik(cik)
    url = f"https://data.sec.gov/submissions/CIK{padded}.json"
    try:
        r = requests.get(url, headers=SEC_HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        return {
            "name": data.get("name", "Unknown"),
            "cik": cik,
            "cik_padded": padded,
            "filings": data.get("filings", {}).get("recent", {}),
        }
    except Exception as e:
        log.error(f"Error fetching company info for CIK {cik}: {e}")
        return None


def find_13f_filings(filings_data):
    forms = filings_data.get("form", [])
    accessions = filings_data.get("accessionNumber", [])
    dates = filings_data.get("filingDate", [])
    primary_docs = filings_data.get("primaryDocument", [])

    results = []
    for i, form in enumerate(forms):
        if form in ("13F-HR", "13F-HR/A"):
            results.append({
                "form": form,
                "accession": accessions[i],
                "date": dates[i],
                "primary_doc": primary_docs[i] if i < len(primary_docs) else "",
            })
    return results


def fetch_filing_index(cik, accession):
    padded = pad_cik(cik)
    acc_clean = accession.replace("-", "")
    base_url = f"https://www.sec.gov/Archives/edgar/data/{padded}/{acc_clean}/"

    # Try JSON index first (much more reliable than HTML scraping)
    try:
        json_url = f"https://www.sec.gov/Archives/edgar/data/{padded}/{acc_clean}/index.json"
        r = requests.get(json_url, headers=SEC_HEADERS, timeout=15)
        if r.status_code == 200:
            return r.text, base_url
    except Exception as e:
        log.warning(f"JSON index failed, trying HTML: {e}")

    # Fallback to HTML directory listing
    try:
        r = requests.get(base_url, headers=SEC_HEADERS, timeout=15)
        r.raise_for_status()
        return r.text, base_url
    except Exception as e:
        log.error(f"Error fetching filing index: {e}")
        return None, base_url


def find_infotable_url(index_content, base_url, cik, accession):
    padded = pad_cik(cik)
    acc_clean = accession.replace("-", "")
    archives_base = f"https://www.sec.gov/Archives/edgar/data/{padded}/{acc_clean}/"

    if not index_content:
        # Skip straight to brute force
        pass
    else:
        # Try parsing as JSON index first
        try:
            data = json.loads(index_content)
            items = data.get("directory", {}).get("item", [])
            for item in items:
                name = item.get("name", "")
                name_lower = name.lower()
                if name_lower.endswith(".xml") and "primary_doc" not in name_lower:
                    if any(kw in name_lower for kw in ["infotable", "information_table", "info_table"]):
                        url = archives_base + name
                        log.info(f"Found infotable via JSON index: {url}")
                        return url
            # If no infotable keyword match, grab the largest XML that isn't primary_doc
            xml_files = []
            for item in items:
                name = item.get("name", "")
                if name.lower().endswith(".xml") and "primary_doc" not in name.lower():
                    size = item.get("size", 0)
                    if isinstance(size, str):
                        try:
                            size = int(size)
                        except:
                            size = 0
                    xml_files.append((name, size))
            if xml_files:
                # Sort by size descending — infotable is typically the largest XML
                xml_files.sort(key=lambda x: x[1], reverse=True)
                url = archives_base + xml_files[0][0]
                log.info(f"Found infotable via largest XML: {url}")
                return url
        except (json.JSONDecodeError, KeyError):
            pass  # Not JSON, try HTML parsing

        # HTML regex fallback
        xml_patterns = [
            r'href="([^"]*infotable[^"]*\.xml)"',
            r'href="([^"]*information[_ ]?table[^"]*\.xml)"',
            r'href="([^"]*13[fF][^"]*\.xml)"',
        ]
        for pattern in xml_patterns:
            matches = re.findall(pattern, index_content, re.IGNORECASE)
            for match in matches:
                if 'primary_doc' in match.lower():
                    continue
                if match.startswith("http"):
                    return match
                return archives_base + match

        # Last HTML fallback: any XML that isn't primary_doc
        all_xml = re.findall(r'href="([^"]*\.xml)"', index_content, re.IGNORECASE)
        for match in all_xml:
            if 'primary_doc' not in match.lower():
                if match.startswith("http"):
                    return match
                return archives_base + match

    # Brute force: try common filenames directly
    common_names = [
        "infotable.xml", "InfoTable.xml", "INFOTABLE.XML",
        "information_table.xml", "InformationTable.xml",
    ]
    for name in common_names:
        test_url = archives_base + name
        try:
            r = requests.head(test_url, headers=SEC_HEADERS, timeout=10)
            if r.status_code == 200:
                log.info(f"Found infotable via brute force: {test_url}")
                return test_url
        except:
            pass

    log.warning(f"Could not find infotable for CIK {cik} accession {accession}")
    return None


def parse_13f_holdings(xml_url):
    try:
        r = requests.get(xml_url, headers=SEC_HEADERS, timeout=20)
        r.raise_for_status()
        raw_content = r.text

        # ── Check if filing uses new dollar format or old thousands format ──
        # New format (2025+) has "to the nearest dollar" or FIGI column
        is_dollar_format = False
        if "nearest dollar" in raw_content.lower() or "<figi>" in raw_content.lower():
            is_dollar_format = True

        # ── Strategy 1: Strip namespaces and parse XML tree ──
        content = raw_content
        # Strip all namespace declarations
        content = re.sub(r'\sxmlns(?::\w+)?="[^"]*"', '', content)
        # Strip all namespace prefixes from tags
        content = re.sub(r'<(/?)[\w]+:', r'<\1', content)

        holdings = {}

        try:
            root = ET.fromstring(content)

            # Find all infoTable entries — try multiple strategies
            entries = []

            # Strategy 1a: Look for elements with 'infotable' in tag (case-insensitive)
            for el in root.iter():
                tag_lower = el.tag.lower() if isinstance(el.tag, str) else ""
                if tag_lower == 'infotable':
                    entries.append(el)

            # Strategy 1b: If nothing found, look for elements that CONTAIN key 13F child tags
            if not entries:
                for el in root.iter():
                    child_tags = set()
                    for ch in el:
                        t = ch.tag.lower() if isinstance(ch.tag, str) else ""
                        child_tags.add(t)
                    # A 13F entry has cusip + nameofissuer + value
                    if 'cusip' in child_tags and 'nameofissuer' in child_tags:
                        entries.append(el)

            for entry in entries:
                cusip = None
                name_of_issuer = ""
                title = ""
                value = 0
                shares = 0
                put_call = ""

                # Walk ALL descendants (not just direct children)
                for child in entry.iter():
                    tag = child.tag.lower() if isinstance(child.tag, str) else ""
                    # Strip any remaining namespace from tag
                    if '}' in tag:
                        tag = tag.split('}')[-1]
                    text = (child.text or "").strip()

                    if tag == "cusip" and text:
                        cusip = text
                    elif tag == "nameofissuer" and text:
                        name_of_issuer = text
                    elif tag == "titleofclass" and text:
                        title = text
                    elif tag == "value" and text:
                        try:
                            value = int(text)
                        except:
                            value = 0
                    elif tag == "sshprnamt" and text:
                        try:
                            shares = int(text)
                        except:
                            shares = 0
                    elif tag == "putcall" and text:
                        put_call = text

                if cusip:
                    key = cusip
                    if put_call:
                        key = f"{cusip}_{put_call}"
                    holdings[key] = {
                        "cusip": cusip,
                        "name": name_of_issuer,
                        "title": title,
                        "value_raw": value,
                        "shares": shares,
                        "put_call": put_call,
                    }

        except ET.ParseError as xml_err:
            log.warning(f"XML parse failed, trying regex fallback: {xml_err}")

        # ── Strategy 2: Regex fallback on raw XML ──
        if not holdings:
            log.info(f"XML tree yielded 0 holdings, trying regex on raw content...")
            # Find all infoTable blocks
            blocks = re.findall(
                r'<(?:[\w:]*)?infoTable[^>]*>(.*?)</(?:[\w:]*)?infoTable>',
                raw_content, re.DOTALL | re.IGNORECASE
            )
            for block in blocks:
                cusip_m = re.search(r'<(?:[\w:]*)?cusip[^>]*>\s*([A-Z0-9]+)\s*</', block, re.IGNORECASE)
                name_m = re.search(r'<(?:[\w:]*)?nameOfIssuer[^>]*>\s*([^<]+)\s*</', block, re.IGNORECASE)
                title_m = re.search(r'<(?:[\w:]*)?titleOfClass[^>]*>\s*([^<]+)\s*</', block, re.IGNORECASE)
                value_m = re.search(r'<(?:[\w:]*)?value[^>]*>\s*(\d+)\s*</', block, re.IGNORECASE)
                shares_m = re.search(r'<(?:[\w:]*)?sshPrnamt[^>]*>\s*(\d+)\s*</', block, re.IGNORECASE)
                putcall_m = re.search(r'<(?:[\w:]*)?putCall[^>]*>\s*(\w+)\s*</', block, re.IGNORECASE)

                cusip = cusip_m.group(1) if cusip_m else None
                if cusip:
                    put_call = putcall_m.group(1) if putcall_m else ""
                    key = f"{cusip}_{put_call}" if put_call else cusip
                    holdings[key] = {
                        "cusip": cusip,
                        "name": name_m.group(1).strip() if name_m else "",
                        "title": title_m.group(1).strip() if title_m else "",
                        "value_raw": int(value_m.group(1)) if value_m else 0,
                        "shares": int(shares_m.group(1)) if shares_m else 0,
                        "put_call": put_call,
                    }

        # ── Detect value unit and normalize to dollars ──
        if not is_dollar_format and holdings:
            # Heuristic: compute median value/shares ratio
            # In dollars format: ratio = actual stock price ($1-$5000 typically)
            # In thousands format: ratio = price/1000 ($0.001-$5 typically)
            ratios = []
            for h in holdings.values():
                if h["shares"] > 0 and h["value_raw"] > 0:
                    ratios.append(h["value_raw"] / h["shares"])
            if ratios:
                ratios.sort()
                median_ratio = ratios[len(ratios) // 2]
                # If median price-per-share < $5, values are likely in thousands (old format)
                if median_ratio < 5:
                    is_dollar_format = False
                else:
                    is_dollar_format = True
            else:
                is_dollar_format = True  # Default to dollar format for newer filings

        # Normalize: store value_thousands for backward compatibility
        multiplier = 1 if is_dollar_format else 1000
        fmt_label = "dollars" if is_dollar_format else "thousands"
        for h in holdings.values():
            # value_thousands stores the value such that value_thousands * 1000 = wrong for new format
            # Instead, we store value so that the display is correct
            if is_dollar_format:
                h["value_thousands"] = h["value_raw"]  # Already in dollars
            else:
                h["value_thousands"] = h["value_raw"]  # In thousands (legacy)
            h["_is_dollar_format"] = is_dollar_format

        log.info(f"Parsed {len(holdings)} holdings from {xml_url} (values in {fmt_label})")
        return holdings
    except Exception as e:
        log.error(f"Error parsing 13F XML from {xml_url}: {e}")
        return {}


def cusip_to_ticker(cusip):
    try:
        figi_url = "https://api.openfigi.com/v3/mapping"
        payload = [{"idType": "ID_CUSIP", "idValue": cusip}]
        r = requests.post(figi_url, json=payload, headers={"Content-Type": "application/json"}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data and len(data) > 0 and "data" in data[0] and len(data[0]["data"]) > 0:
                ticker = data[0]["data"][0].get("ticker", cusip)
                return ticker
    except:
        pass
    return cusip


ticker_cache = {}


def get_ticker(cusip, name=""):
    """Resolve CUSIP to ticker. Tries SEC name match first, then OpenFIGI."""
    if cusip in ticker_cache:
        return ticker_cache[cusip]

    ticker = None

    # Strategy 1: Match issuer name against SEC company_tickers.json (fast, free)
    if name:
        tickers_data = get_company_tickers()
        if tickers_data:
            # Normalize: lowercase, strip punctuation, collapse whitespace
            def normalize(s):
                s = s.lower().replace(".", " ").replace(",", " ").replace("/", " ")
                s = re.sub(r'[^a-z0-9\s]', '', s)
                return " ".join(s.split())

            name_norm = normalize(name)
            name_words = set(name_norm.split())
            first_word = name_norm.split()[0] if name_norm else ""

            best_match = None
            best_score = 0

            for rec in tickers_data.get("entries", []):
                rec_ticker = rec.get("ticker", "")
                if not rec_ticker:
                    continue
                rec_norm = normalize(rec["name"])

                # Exact match
                if rec_norm == name_norm:
                    best_match = rec_ticker
                    break

                # One contains the other
                if name_norm in rec_norm or rec_norm in name_norm:
                    score = min(len(name_norm), len(rec_norm)) / max(len(name_norm), len(rec_norm), 1)
                    if score > best_score and score > 0.4:
                        best_score = score
                        best_match = rec_ticker

                # Word overlap with first-word anchor
                rec_words = set(rec_norm.split())
                rec_first = rec_norm.split()[0] if rec_norm else ""
                if first_word and first_word == rec_first and len(first_word) > 2:
                    overlap = name_words & rec_words
                    score = len(overlap) / max(len(name_words), len(rec_words), 1)
                    if score > best_score and score > 0.3:
                        best_score = score
                        best_match = rec_ticker

            if best_match:
                ticker = best_match

    # Strategy 2: OpenFIGI API (has rate limits, but resolves CUSIPs reliably)
    if not ticker:
        ticker = cusip_to_ticker(cusip)

    ticker_cache[cusip] = ticker
    return ticker


# ── Comparison Logic ─────────────────────────────────────────────────────────

def _holding_value_dollars(h):
    """Get the dollar value of a holding, handling both old (thousands) and new (dollar) formats."""
    raw = h.get("value_thousands", 0) or h.get("value_raw", 0)
    if h.get("_is_dollar_format", False):
        return raw  # Already in dollars
    else:
        return raw * 1000  # Old format: stored in thousands


def compare_holdings(prev_holdings, curr_holdings):
    prev_keys = set(prev_holdings.keys())
    curr_keys = set(curr_holdings.keys())

    new_positions = {}
    closed_positions = {}
    increased_positions = {}
    decreased_positions = {}

    for key in curr_keys - prev_keys:
        h = curr_holdings[key]
        ticker = get_ticker(h["cusip"], h["name"])
        new_positions[key] = {
            **h,
            "ticker": ticker,
            "value_dollars": _holding_value_dollars(h),
        }

    for key in prev_keys - curr_keys:
        h = prev_holdings[key]
        ticker = get_ticker(h["cusip"], h["name"])
        closed_positions[key] = {
            **h,
            "ticker": ticker,
            "value_dollars": _holding_value_dollars(h),
        }

    for key in prev_keys & curr_keys:
        prev = prev_holdings[key]
        curr = curr_holdings[key]
        ticker = get_ticker(curr["cusip"], curr["name"])

        share_diff = curr["shares"] - prev["shares"]
        curr_val = _holding_value_dollars(curr)
        prev_val = _holding_value_dollars(prev)
        value_diff = curr_val - prev_val

        if share_diff > 0:
            increased_positions[key] = {
                **curr,
                "ticker": ticker,
                "prev_shares": prev["shares"],
                "curr_shares": curr["shares"],
                "share_change": share_diff,
                "prev_value": prev_val,
                "curr_value": curr_val,
                "value_change": value_diff,
            }
        elif share_diff < 0:
            decreased_positions[key] = {
                **curr,
                "ticker": ticker,
                "prev_shares": prev["shares"],
                "curr_shares": curr["shares"],
                "share_change": share_diff,
                "prev_value": prev_val,
                "curr_value": curr_val,
                "value_change": value_diff,
            }

    return {
        "new": new_positions,
        "closed": closed_positions,
        "increased": increased_positions,
        "decreased": decreased_positions,
    }


# ── Discord Alert ────────────────────────────────────────────────────────────

def is_resolved_ticker(ticker):
    """Check if a ticker looks like a real ticker vs an unresolved CUSIP."""
    if not ticker:
        return False
    # CUSIPs are 9 chars (6 alpha + 2 digits + 1 check), tickers are 1-5
    if len(ticker) > 6:
        return False
    # CUSIPs typically end in digits
    if len(ticker) == 9 and ticker[-1].isdigit():
        return False
    return True


def format_dollar(amount):
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.2f}B"
    elif amount >= 1_000_000:
        return f"${amount / 1_000_000:.2f}M"
    elif amount >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:,.0f}"


def send_discord_alert(filer_name, cik, filing_date, changes, accession=""):
    new_pos = changes["new"]
    increased = changes["increased"]
    closed = changes["closed"]
    decreased = changes["decreased"]

    # Build filing link
    filing_link = ""
    if accession:
        acc_no_dashes = accession.replace("-", "")
        padded = pad_cik(cik)
        filing_link = f"https://www.sec.gov/Archives/edgar/data/{padded}/{acc_no_dashes}/{accession}-index.htm"

    desc = f"**{filer_name}** (CIK: {cik})\nFiling Date: {filing_date}"
    if filing_link:
        desc += f"\n[📄 View Filing on EDGAR]({filing_link})"

    main_embed = {
        "title": "New 13F Filing Detected",
        "description": desc,
        "color": 0x00D4AA,
        "timestamp": datetime.now(EST).isoformat(),
        "footer": {"text": "13F Monitor"},
    }

    fields = []

    summary = (
        f"New: **{len(new_pos)}** | Added: **{len(increased)}** | "
        f"Reduced: **{len(decreased)}** | Closed: **{len(closed)}**"
    )
    fields.append({"name": "Summary", "value": summary, "inline": False})

    if new_pos:
        lines = []
        for key, h in sorted(new_pos.items(), key=lambda x: x[1]["value_dollars"], reverse=True)[:15]:
            ticker = h["ticker"]
            name = h.get("name", "")
            val = format_dollar(h["value_dollars"])
            shares = f"{h['shares']:,}" if h['shares'] else "N/A"
            pc = f" ({h['put_call']})" if h.get('put_call') else ""
            # Show ticker + name, or just name if ticker is still a CUSIP
            is_real_ticker = is_resolved_ticker(ticker)
            if is_real_ticker:
                label = f"**{ticker}**{pc} ({name})" if name else f"**{ticker}**{pc}"
            else:
                label = f"**{name or ticker}**{pc}"
            lines.append(f"{label} - {val} ({shares} shares)")
        if len(new_pos) > 15:
            lines.append(f"*...and {len(new_pos) - 15} more*")
        fields.append({"name": f"New Positions ({len(new_pos)})", "value": "\n".join(lines), "inline": False})

    if increased:
        lines = []
        for key, h in sorted(increased.items(), key=lambda x: abs(x[1]["value_change"]), reverse=True)[:15]:
            ticker = h["ticker"]
            name = h.get("name", "")
            change = format_dollar(abs(h["value_change"]))
            share_pct = ((h["curr_shares"] - h["prev_shares"]) / h["prev_shares"] * 100) if h["prev_shares"] else 0
            is_real_ticker = is_resolved_ticker(ticker)
            if is_real_ticker:
                label = f"**{ticker}** ({name})" if name else f"**{ticker}**"
            else:
                label = f"**{name or ticker}**"
            lines.append(f"{label} - +{change} (+{share_pct:.1f}% shares)")
        if len(increased) > 15:
            lines.append(f"*...and {len(increased) - 15} more*")
        fields.append({"name": f"Added To ({len(increased)})", "value": "\n".join(lines), "inline": False})

    if closed:
        lines = []
        for key, h in sorted(closed.items(), key=lambda x: x[1]["value_dollars"], reverse=True)[:10]:
            ticker = h["ticker"]
            name = h.get("name", "")
            val = format_dollar(h["value_dollars"])
            is_real_ticker = is_resolved_ticker(ticker)
            if is_real_ticker:
                label = f"**{ticker}** ({name})" if name else f"**{ticker}**"
            else:
                label = f"**{name or ticker}**"
            lines.append(f"{label} - {val}")
        if len(closed) > 10:
            lines.append(f"*...and {len(closed) - 10} more*")
        fields.append({"name": f"Closed Positions ({len(closed)})", "value": "\n".join(lines), "inline": False})

    if decreased:
        lines = []
        for key, h in sorted(decreased.items(), key=lambda x: abs(x[1]["value_change"]), reverse=True)[:10]:
            ticker = h["ticker"]
            name = h.get("name", "")
            change = format_dollar(abs(h["value_change"]))
            share_pct = ((h["prev_shares"] - h["curr_shares"]) / h["prev_shares"] * 100) if h["prev_shares"] else 0
            is_real_ticker = is_resolved_ticker(ticker)
            if is_real_ticker:
                label = f"**{ticker}** ({name})" if name else f"**{ticker}**"
            else:
                label = f"**{name or ticker}**"
            lines.append(f"{label} - -{change} (-{share_pct:.1f}% shares)")
        if len(decreased) > 10:
            lines.append(f"*...and {len(decreased) - 10} more*")
        fields.append({"name": f"Reduced Positions ({len(decreased)})", "value": "\n".join(lines), "inline": False})

    if not fields:
        fields.append({"name": "Info", "value": "Holdings appear unchanged from previous quarter.", "inline": False})

    main_embed["fields"] = fields
    payload = {"username": "13F Monitor", "embeds": [main_embed]}

    for webhook_url in DISCORD_WEBHOOKS:
        try:
            r = requests.post(webhook_url, json=payload, timeout=10)
            if r.status_code in (200, 204):
                log.info("Discord alert sent")
            else:
                log.error(f"Discord webhook failed: {r.status_code} {r.text}")
        except Exception as e:
            log.error(f"Discord webhook error: {e}")
        time.sleep(0.5)


# ── 13D Filing Support ──────────────────────────────────────────────────────

def get_13d_subject_info(filer_cik, accession):
    """Extract the subject (target) company from a SC 13D filing header."""
    acc_no_dashes = accession.replace("-", "")
    padded = pad_cik(filer_cik)

    result = {"name": "Unknown Company", "cik": "", "ticker": "N/A", "cusip": ""}

    # Fetch the full submission text file header (first 10KB has SGML headers)
    txt_url = f"https://www.sec.gov/Archives/edgar/data/{padded}/{acc_no_dashes}/{accession}.txt"
    try:
        r = requests.get(txt_url, headers=SEC_HEADERS, timeout=15, stream=True)
        header_text = ""
        for chunk in r.iter_content(chunk_size=5000, decode_unicode=True):
            if chunk:
                header_text += chunk
            if len(header_text) > 15000 or "</SEC-HEADER>" in header_text:
                break
        r.close()

        # Parse SUBJECT COMPANY section
        subj_match = re.search(
            r'SUBJECT COMPANY:.*?COMPANY CONFORMED NAME:\s*(.+?)(?:\n|\r)',
            header_text, re.DOTALL
        )
        if subj_match:
            result["name"] = subj_match.group(1).strip()

        # Get CIK of subject
        cik_match = re.search(
            r'SUBJECT COMPANY:.*?CENTRAL INDEX KEY:\s*(\d+)',
            header_text, re.DOTALL
        )
        if cik_match:
            result["cik"] = cik_match.group(1).lstrip("0") or "0"

        # Try to get ticker from ticker cache using subject CIK
        if result["cik"]:
            tickers = get_company_tickers()
            if tickers and tickers.get("by_cik"):
                rec = tickers["by_cik"].get(result["cik"])
                if rec:
                    result["ticker"] = rec.get("ticker", "N/A")

        # If no ticker from CIK, try name match
        if result["ticker"] == "N/A" and result["name"] != "Unknown Company":
            tickers = get_company_tickers()
            if tickers:
                name_lower = result["name"].lower()
                for rec in tickers.get("entries", [])[:]:
                    if rec["name"].lower() == name_lower:
                        result["ticker"] = rec.get("ticker", "N/A")
                        break

    except Exception as e:
        log.warning(f"Failed to parse 13D subject from {txt_url}: {e}")

    # Fallback: try the index.json for document listing and parse primary doc
    if result["name"] == "Unknown Company":
        try:
            index_url = f"https://www.sec.gov/Archives/edgar/data/{padded}/{acc_no_dashes}/index.json"
            r = requests.get(index_url, headers=SEC_HEADERS, timeout=15)
            if r.status_code == 200:
                idx = r.json()
                items = idx.get("directory", {}).get("item", [])
                # Look for the primary document
                for item in items:
                    name = item.get("name", "")
                    if name.endswith(".htm") or name.endswith(".html"):
                        doc_url = f"https://www.sec.gov/Archives/edgar/data/{padded}/{acc_no_dashes}/{name}"
                        dr = requests.get(doc_url, headers=SEC_HEADERS, timeout=15)
                        # Look for CUSIP in the document
                        cusip_match = re.search(r'CUSIP\s*(?:No\.?|Number|#)?\s*[:\s]*([A-Z0-9]{6,9})', dr.text, re.IGNORECASE)
                        if cusip_match:
                            result["cusip"] = cusip_match.group(1).strip()
                        # Look for issuer name
                        issuer_match = re.search(r'(?:Name of Issuer|ISSUER)\s*[:\s]*([^\n<]{3,80})', dr.text, re.IGNORECASE)
                        if issuer_match and result["name"] == "Unknown Company":
                            result["name"] = issuer_match.group(1).strip()
                        break
        except Exception as e:
            log.warning(f"Failed to parse 13D from index.json: {e}")

    log.info(f"13D subject parsed: {result['name']} ({result['ticker']}) CIK:{result['cik']}")
    return result


def send_discord_13d_alert(filer_name, filer_cik, filing_date, subject_info, accession, is_amendment=False):
    """Send a Discord alert for a SC 13D filing."""
    form_type = "SC 13D/A (Amendment)" if is_amendment else "SC 13D"
    ticker_display = subject_info['ticker'] if subject_info['ticker'] != 'N/A' else ''
    ticker_bold = f"**${subject_info['ticker']}**" if ticker_display else ''

    filing_url = (
        f"https://www.sec.gov/cgi-bin/browse-edgar?"
        f"action=getcompany&CIK={filer_cik}&type=SC%2013D&dateb=&owner=include&count=10"
    )
    acc_no_dashes = accession.replace("-", "")
    direct_url = f"https://www.sec.gov/Archives/edgar/data/{pad_cik(filer_cik)}/{acc_no_dashes}/{accession}-index.htm"

    embed = {
        "title": f"{'🔄' if is_amendment else '🚨'} {form_type} Filed",
        "description": (
            f"**{filer_name}** (CIK: {filer_cik})\n"
            f"has {'amended their >5% stake in' if is_amendment else 'acquired >5% stake in'}:\n\n"
            f"**{subject_info['name']}** {ticker_bold}\n"
        ),
        "color": 0xff6b35 if not is_amendment else 0xf59e0b,
        "timestamp": datetime.now(EST).isoformat(),
        "footer": {"text": "13D Monitor"},
        "fields": [],
    }

    if subject_info.get("cusip"):
        embed["fields"].append({"name": "CUSIP", "value": subject_info["cusip"], "inline": True})
    if subject_info.get("cik"):
        embed["fields"].append({"name": "Subject CIK", "value": subject_info["cik"], "inline": True})
    embed["fields"].append({"name": "Filing Date", "value": filing_date, "inline": True})
    embed["fields"].append({"name": "Filing Link", "value": f"[View on EDGAR]({direct_url})", "inline": False})

    payload = {"username": "13D Monitor", "embeds": [embed]}

    for webhook_url in DISCORD_WEBHOOKS:
        try:
            r = requests.post(webhook_url, json=payload, timeout=10)
            if r.status_code in (200, 204):
                log.info(f"13D Discord alert sent: {filer_name} -> {subject_info['name']}")
            else:
                log.error(f"13D Discord webhook failed: {r.status_code} {r.text}")
        except Exception as e:
            log.error(f"13D Discord webhook error: {e}")
        time.sleep(0.5)


# ── Check Individual Filer ───────────────────────────────────────────────────

def check_filer(cik, filer_data):
    try:
        info = get_company_info(cik)
        if not info:
            return

        filings_13f = find_13f_filings(info["filings"])
        if not filings_13f:
            log.info(f"No 13F filings found for {filer_data['name']} (CIK: {cik})")
            return

        latest = filings_13f[0]
        latest_accession = latest["accession"]

        if latest_accession == filer_data.get("last_filing_accession"):
            return

        log.info(f"NEW 13F detected for {filer_data['name']}: {latest_accession} ({latest['date']})")

        index_html, base_url = fetch_filing_index(cik, latest_accession)
        infotable_url = find_infotable_url(index_html, base_url, cik, latest_accession)

        if not infotable_url:
            log.error(f"Could not find infotable XML for {latest_accession}")
            monitor_status["errors"].append({
                "time": datetime.now(EST).isoformat(),
                "msg": f"No infotable found for {filer_data['name']} filing {latest_accession}",
            })
            return

        curr_holdings = parse_13f_holdings(infotable_url)
        if not curr_holdings:
            log.error(f"No holdings parsed from {infotable_url}")
            return

        prev_holdings = filer_data.get("holdings_prev", {})

        if not prev_holdings and len(filings_13f) > 1:
            prev_filing = filings_13f[1]
            prev_index_html, prev_base_url = fetch_filing_index(cik, prev_filing["accession"])
            prev_infotable_url = find_infotable_url(prev_index_html, prev_base_url, cik, prev_filing["accession"])
            if prev_infotable_url:
                prev_holdings = parse_13f_holdings(prev_infotable_url)
                time.sleep(0.5)

        changes = compare_holdings(prev_holdings, curr_holdings)
        send_discord_alert(filer_data["name"], cik, latest["date"], changes, accession=latest_accession)

        filer_data["last_filing_accession"] = latest_accession
        filer_data["holdings_prev"] = curr_holdings
        filer_data["holdings_current"] = curr_holdings
        filer_data["last_filing_date"] = latest["date"]
        filer_data["last_changes"] = changes
        filer_data["total_holdings"] = len(curr_holdings)

        alert_entry = {
            "time": datetime.now(EST).isoformat(),
            "type": "13F",
            "filer": filer_data["name"],
            "cik": cik,
            "date": latest["date"],
            "accession": latest_accession,
            "new_positions": len(changes["new"]),
            "increased": len(changes["increased"]),
            "decreased": len(changes["decreased"]),
            "closed": len(changes["closed"]),
        }
        alert_log.insert(0, alert_entry)
        if len(alert_log) > 100:
            alert_log.pop()

        log.info(f"Processed 13F for {filer_data['name']}: "
                 f"{len(changes['new'])} new, {len(changes['increased'])} added, "
                 f"{len(changes['decreased'])} reduced, {len(changes['closed'])} closed")

        save_state()

    except Exception as e:
        log.error(f"Error checking filer {cik}: {e}")
        monitor_status["errors"].append({
            "time": datetime.now(EST).isoformat(),
            "msg": f"Error checking {filer_data.get('name', cik)}: {str(e)}",
        })
        if len(monitor_status["errors"]) > 50:
            monitor_status["errors"] = monitor_status["errors"][:50]


# ── Filing Window Logic ──────────────────────────────────────────────────────
# 13Fs due 45 calendar days after quarter end.
# We monitor from day 28 to day 52 (buffer for late + amended filings).
QUARTER_ENDS_MD = [(3, 31), (6, 30), (9, 30), (12, 31)]
FILING_WINDOW_START_DAYS = 28
FILING_WINDOW_END_DAYS = 52

def get_filing_window_info():
    """Determine if we're in a 13F filing window."""
    from datetime import date
    today = datetime.now(EST).date()

    # Build quarter-end candidates spanning previous year through current
    candidates = []
    for year in [today.year - 1, today.year]:
        for month, day in QUARTER_ENDS_MD:
            candidates.append(date(year, month, day))

    # Check if today falls in any window
    for qe in candidates:
        days_since = (today - qe).days
        if FILING_WINDOW_START_DAYS <= days_since <= FILING_WINDOW_END_DAYS:
            q_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
            deadline = qe + timedelta(days=45)
            window_end = qe + timedelta(days=FILING_WINDOW_END_DAYS)
            return True, {
                "active": True,
                "quarter": f"{qe.year} {q_map[qe.month]}",
                "quarter_end": qe.isoformat(),
                "deadline": deadline.isoformat(),
                "window_closes": window_end.isoformat(),
                "days_since_qe": days_since,
                "days_to_deadline": max(0, 45 - days_since),
            }

    # Find next upcoming window
    future = []
    for qe in candidates:
        window_start = qe + timedelta(days=FILING_WINDOW_START_DAYS)
        if window_start > today:
            future.append((window_start, qe))
    # Also check next year in case we're past all current-year windows
    for month, day in QUARTER_ENDS_MD:
        from datetime import date as _date
        qe = _date(today.year + 1, month, day)
        window_start = qe + timedelta(days=FILING_WINDOW_START_DAYS)
        future.append((window_start, qe))

    if future:
        next_start, next_qe = min(future)
        q_map = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
        return False, {
            "active": False,
            "next_window_opens": next_start.isoformat(),
            "next_quarter": f"{next_qe.year} {q_map[next_qe.month]}",
            "days_until_window": (next_start - today).days,
        }

    return False, {"active": False}


def should_poll():
    """Determine if we should actively poll right now."""
    now = datetime.now(EST)
    in_window, info = get_filing_window_info()
    monitor_status["filing_window"] = info

    if not in_window:
        return False, "dormant"

    # Monitor weekdays 5am-5pm ET (market hours with pre-market buffer)
    if now.weekday() >= 5:
        return False, "weekend"
    if now.hour < 5 or now.hour >= 17:
        return False, "after_hours"

    return True, "active"


# ── EDGAR Feed Check (single-request approach) ──────────────────────────────
seen_accessions = set()
seen_13d_accessions = set()
_filer_rotation_idx = 0


def check_edgar_feed():
    """Hit EDGAR full-text search for today's 13F filings — ONE request.
    Returns True if feed was successfully checked."""
    today = datetime.now(EST).strftime("%Y-%m-%d")
    url = (
        f"https://efts.sec.gov/LATEST/search-index?"
        f"forms=13F-HR,13F-HR%2FA&"
        f"dateRange=custom&startdt={today}&enddt={today}&"
        f"from=0&size=200"
    )
    try:
        r = requests.get(url, headers=SEC_HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            return True  # Feed works, just no filings yet today

        # Build set of monitored CIKs (both stripped and padded forms)
        monitored_cik_set = set()
        for cik in monitored_filers:
            monitored_cik_set.add(cik)
            monitored_cik_set.add(cik.zfill(10))
            monitored_cik_set.add(cik.lstrip("0") or "0")

        for hit in hits:
            source = hit.get("_source", {})
            accession = source.get("accession_no", "") or hit.get("_id", "")

            if not accession or accession in seen_accessions:
                continue

            # Extract CIK - try multiple possible field names
            filing_cik = None
            for field in ["entity_id", "cik", "ciks", "entity_cik"]:
                val = source.get(field)
                if val:
                    if isinstance(val, list):
                        val = val[0]
                    filing_cik = str(val).lstrip("0") or "0"
                    break

            if not filing_cik:
                continue

            if filing_cik in monitored_cik_set:
                # Normalize to our key format
                norm_cik = filing_cik.lstrip("0") or "0"
                if norm_cik not in monitored_filers:
                    # Try padded
                    for mc in monitored_filers:
                        if mc.lstrip("0") == norm_cik:
                            norm_cik = mc
                            break

                if norm_cik in monitored_filers:
                    seen_accessions.add(accession)
                    filer_data = monitored_filers[norm_cik]
                    filer_name = filer_data.get("name", norm_cik)
                    log.info(f"⚡ EDGAR feed: New 13F from {filer_name} (accession: {accession})")
                    check_filer(norm_cik, filer_data)
                    time.sleep(0.3)

        return True
    except Exception as e:
        log.warning(f"EDGAR feed unavailable: {e}")
        return False


def check_edgar_feed_13d():
    """Hit EDGAR full-text search for today's SC 13D filings — ONE request.
    Returns True if feed was successfully checked."""
    today = datetime.now(EST).strftime("%Y-%m-%d")
    url = (
        f"https://efts.sec.gov/LATEST/search-index?"
        f"forms=SC%2013D,SC%2013D%2FA&"
        f"dateRange=custom&startdt={today}&enddt={today}&"
        f"from=0&size=200"
    )
    try:
        r = requests.get(url, headers=SEC_HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            return True  # Feed works, just no 13D filings today

        # Build set of monitored CIKs (both stripped and padded forms)
        monitored_cik_set = set()
        for cik in monitored_filers:
            monitored_cik_set.add(cik)
            monitored_cik_set.add(cik.zfill(10))
            monitored_cik_set.add(cik.lstrip("0") or "0")

        for hit in hits:
            source = hit.get("_source", {})
            accession = source.get("accession_no", "") or hit.get("_id", "")

            if not accession or accession in seen_13d_accessions:
                continue

            # Extract CIK
            filing_cik = None
            for field in ["entity_id", "cik", "ciks", "entity_cik"]:
                val = source.get(field)
                if val:
                    if isinstance(val, list):
                        val = val[0]
                    filing_cik = str(val).lstrip("0") or "0"
                    break

            if not filing_cik:
                continue

            if filing_cik in monitored_cik_set:
                # Normalize CIK to our key format
                norm_cik = filing_cik.lstrip("0") or "0"
                if norm_cik not in monitored_filers:
                    for mc in monitored_filers:
                        if mc.lstrip("0") == norm_cik:
                            norm_cik = mc
                            break

                if norm_cik in monitored_filers:
                    seen_13d_accessions.add(accession)
                    filer_data = monitored_filers[norm_cik]
                    filer_name = filer_data.get("name", norm_cik)

                    # Determine if amendment
                    form_type = source.get("form_type", "SC 13D")
                    is_amendment = "/A" in form_type.upper() or "A" in form_type.upper().replace("SC 13D", "").strip()

                    filing_date = source.get("file_date", today)

                    log.info(f"⚡ EDGAR feed: New {form_type} from {filer_name} (accession: {accession})")

                    # Parse subject company info
                    subject_info = get_13d_subject_info(norm_cik, accession)

                    # Send Discord alert
                    send_discord_13d_alert(filer_name, norm_cik, filing_date, subject_info, accession, is_amendment)

                    # Add to alert log
                    alert_entry = {
                        "time": datetime.now(EST).isoformat(),
                        "type": "13D",
                        "filer": filer_name,
                        "cik": norm_cik,
                        "date": filing_date,
                        "accession": accession,
                        "form_type": form_type,
                        "subject_company": subject_info.get("name", "Unknown"),
                        "subject_ticker": subject_info.get("ticker", "N/A"),
                        "subject_cik": subject_info.get("cik", ""),
                        "is_amendment": is_amendment,
                    }
                    alert_log.insert(0, alert_entry)
                    if len(alert_log) > 100:
                        alert_log.pop()

                    save_state()
                    time.sleep(0.3)

        return True
    except Exception as e:
        log.warning(f"EDGAR 13D feed unavailable: {e}")
        return False


def check_next_filer_rotation():
    """Fallback: check ONE filer per tick via round-robin."""
    global _filer_rotation_idx
    ciks = list(monitored_filers.keys())
    if not ciks:
        return
    _filer_rotation_idx = _filer_rotation_idx % len(ciks)
    cik = ciks[_filer_rotation_idx]
    _filer_rotation_idx += 1
    filer = monitored_filers[cik]
    if not filer.get("_needs_init"):
        check_filer(cik, filer)


def monitor_loop():
    active, reason = should_poll()

    if not active:
        monitor_status["running"] = False
        monitor_status["poll_status"] = reason
        return

    monitor_status["running"] = True
    monitor_status["poll_status"] = "scanning"
    monitor_status["last_check"] = datetime.now(EST).isoformat()
    monitor_status["checks_today"] += 1

    # Primary: single EDGAR feed request catches all new 13F filings
    # Fallback: rotate through filers one-by-one if feed is down
    if not check_edgar_feed():
        check_next_filer_rotation()

    # Also check for SC 13D filings from monitored filers
    check_edgar_feed_13d()


# ── Initialize scheduler ─────────────────────────────────────────────────────
scheduler = BackgroundScheduler(timezone=EST)
scheduler.add_job(monitor_loop, 'interval', seconds=POLL_INTERVAL_SECONDS, id='monitor_job',
                  max_instances=1, coalesce=True)

_scheduler_started = False
_defaults_loaded = False


def load_default_filers():
    """Seed default filers immediately, then fetch filing data in background."""
    global _defaults_loaded
    if _defaults_loaded:
        return
    _defaults_loaded = True

    # Step 1: Instantly add all filers with placeholder data so they show in UI
    for cik, name in DEFAULT_FILERS:
        if cik not in monitored_filers:
            monitored_filers[cik] = {
                "name": name,
                "cik": cik,
                "url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={pad_cik(cik)}&type=13F-HR",
                "last_filing_accession": None,
                "last_filing_date": None,
                "holdings_prev": {},
                "holdings_current": {},
                "total_holdings": 0,
                "last_changes": None,
                "is_default": True,
                "_needs_init": True,
            }

    log.info(f"Seeded {len(DEFAULT_FILERS)} default filers. Starting background data load...")

    # Step 2: Background thread to fetch actual filing data from SEC
    def _bg_load():
        # Process defaults first, then any user-added filers restored from state
        all_to_init = [(cik, name) for cik, name in DEFAULT_FILERS]

        # Add any user-added filers that need init (restored from state)
        for cik, filer in list(monitored_filers.items()):
            if filer.get("_needs_init") and not filer.get("is_default"):
                all_to_init.append((cik, filer["name"]))

        for cik, name in all_to_init:
            if cik not in monitored_filers:
                continue
            filer = monitored_filers[cik]
            if not filer.get("_needs_init"):
                continue
            try:
                info = get_company_info(cik)
                if not info:
                    log.warning(f"Could not fetch info for {name} (CIK: {cik})")
                    filer.pop("_needs_init", None)  # Don't leave stuck
                    continue

                # Update name from SEC (more accurate)
                filer["name"] = info["name"]

                filings_13f = find_13f_filings(info["filings"])
                if filings_13f:
                    latest = filings_13f[0]
                    filer["last_filing_accession"] = latest["accession"]
                    filer["last_filing_date"] = latest["date"]

                    # Parse current holdings
                    index_html, base_url = fetch_filing_index(cik, latest["accession"])
                    infotable_url = find_infotable_url(index_html, base_url, cik, latest["accession"])
                    if infotable_url:
                        holdings = parse_13f_holdings(infotable_url)
                        filer["holdings_current"] = holdings
                        filer["holdings_prev"] = holdings
                        filer["total_holdings"] = len(holdings)

                    # Parse previous quarter for comparison
                    if len(filings_13f) > 1:
                        time.sleep(0.3)
                        prev = filings_13f[1]
                        prev_index_html, prev_base_url = fetch_filing_index(cik, prev["accession"])
                        prev_infotable_url = find_infotable_url(prev_index_html, prev_base_url, cik, prev["accession"])
                        if prev_infotable_url:
                            prev_holdings = parse_13f_holdings(prev_infotable_url)
                            filer["holdings_prev"] = prev_holdings
                else:
                    log.warning(f"No 13F filings found for {name} (CIK: {cik})")

                filer.pop("_needs_init", None)
                log.info(f"Loaded {info['name']} - {filer['total_holdings']} holdings, last filed {filer['last_filing_date']}")

            except Exception as e:
                log.error(f"Error loading filer {name}: {e}")
                filer.pop("_needs_init", None)  # Don't leave stuck on error

            # SEC rate limit: be polite
            time.sleep(0.5)

        log.info("Background loading of default filers complete.")
        save_state()

    thread = threading.Thread(target=_bg_load, daemon=True)
    thread.start()

def start_scheduler():
    global _scheduler_started
    if _scheduler_started:
        return
    try:
        scheduler.start()
        _scheduler_started = True
        log.info("13F Monitor scheduler started. Polling every %d seconds.", POLL_INTERVAL_SECONDS)
    except Exception as e:
        log.error(f"Scheduler start error: {e}")


@app.before_request
def ensure_scheduler():
    start_scheduler()
    load_state()
    load_default_filers()


# ── HTML Dashboard (embedded - no templates folder needed) ───────────────────

INDEX_HTML = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>13F/13D Filing Monitor</title>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0e17;
            --bg-secondary: #111827;
            --bg-card: #151d2e;
            --bg-card-hover: #1a2438;
            --border: #1e293b;
            --border-active: #334155;
            --text-primary: #e2e8f0;
            --text-secondary: #94a3b8;
            --text-muted: #64748b;
            --accent: #00d4aa;
            --accent-dim: #00d4aa22;
            --accent-glow: #00d4aa44;
            --red: #ef4444;
            --red-dim: #ef444422;
            --orange: #f59e0b;
            --orange-dim: #f59e0b22;
            --blue: #3b82f6;
            --blue-dim: #3b82f622;
            --green: #22c55e;
            --green-dim: #22c55e22;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'DM Sans', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
        }
        .container { max-width: 1200px; margin: 0 auto; padding: 24px; }
        .header {
            display: flex; align-items: center; justify-content: space-between;
            padding: 20px 0 32px; border-bottom: 1px solid var(--border); margin-bottom: 32px;
        }
        .header-left { display: flex; align-items: center; gap: 16px; }
        .logo {
            width: 48px; height: 48px;
            background: linear-gradient(135deg, var(--accent), #00a888);
            border-radius: 12px; display: flex; align-items: center; justify-content: center;
            font-family: 'JetBrains Mono', monospace; font-weight: 700; font-size: 18px;
            color: var(--bg-primary); box-shadow: 0 0 24px var(--accent-glow);
        }
        .header-title h1 { font-size: 22px; font-weight: 700; }
        .header-title p { font-size: 13px; color: var(--text-muted); font-family: 'JetBrains Mono', monospace; }
        .status-badge {
            display: flex; align-items: center; gap: 8px; padding: 8px 16px;
            border-radius: 8px; font-family: 'JetBrains Mono', monospace; font-size: 12px; font-weight: 500;
        }
        .status-badge.active { background: var(--accent-dim); color: var(--accent); border: 1px solid var(--accent); }
        .status-badge.inactive { background: var(--orange-dim); color: var(--orange); border: 1px solid var(--orange); }
        .status-dot { width: 8px; height: 8px; border-radius: 50%; }
        .status-badge.active .status-dot { background: var(--accent); animation: pulse 2s ease-in-out infinite; }
        .status-badge.inactive .status-dot { background: var(--orange); }
        @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }
        .stats-row { display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 32px; }
        .stat-card { background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }
        .stat-label { font-size: 11px; text-transform: uppercase; letter-spacing: 1px; color: var(--text-muted); font-family: 'JetBrains Mono', monospace; margin-bottom: 8px; }
        .stat-value { font-size: 28px; font-weight: 700; font-family: 'JetBrains Mono', monospace; }
        .add-section { background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 24px; margin-bottom: 32px; }
        .add-section h2 { font-size: 16px; font-weight: 600; margin-bottom: 4px; }
        .add-section p { font-size: 13px; color: var(--text-muted); margin-bottom: 16px; }
        .input-row { display: flex; gap: 12px; }
        .input-field { flex: 1; padding: 12px 16px; background: var(--bg-primary); border: 1px solid var(--border); border-radius: 8px; color: var(--text-primary); font-family: 'JetBrains Mono', monospace; font-size: 13px; outline: none; }
        .input-field:focus { border-color: var(--accent); }
        .input-field::placeholder { color: var(--text-muted); }
        .btn { padding: 12px 24px; border: none; border-radius: 8px; font-family: 'DM Sans', sans-serif; font-size: 14px; font-weight: 600; cursor: pointer; transition: all 0.2s; display: inline-flex; align-items: center; gap: 8px; white-space: nowrap; }
        .btn-primary { background: var(--accent); color: var(--bg-primary); }
        .btn-primary:hover { background: #00e8bb; box-shadow: 0 0 20px var(--accent-glow); }
        .btn-primary:disabled { opacity: 0.5; cursor: not-allowed; }
        .btn-secondary { background: var(--bg-secondary); color: var(--text-secondary); border: 1px solid var(--border); }
        .btn-secondary:hover { border-color: var(--border-active); color: var(--text-primary); }
        .btn-danger { background: var(--red-dim); color: var(--red); border: 1px solid transparent; }
        .btn-danger:hover { border-color: var(--red); }
        .btn-sm { padding: 6px 12px; font-size: 12px; }
        .section-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 16px; }
        .section-header h2 { font-size: 16px; font-weight: 600; }
        .filers-table { width: 100%; border-collapse: collapse; background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; margin-bottom: 32px; }
        .filers-table thead th { padding: 14px 20px; text-align: left; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; color: var(--text-muted); font-family: 'JetBrains Mono', monospace; background: var(--bg-secondary); border-bottom: 1px solid var(--border); }
        .filers-table tbody td { padding: 16px 20px; font-size: 14px; border-bottom: 1px solid var(--border); }
        .filers-table tbody tr:last-child td { border-bottom: none; }
        .filers-table tbody tr:hover { background: var(--bg-card-hover); }
        .filer-name { font-weight: 600; }
        .tag { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 11px; font-family: 'JetBrains Mono', monospace; font-weight: 500; }
        .tag-blue { background: var(--blue-dim); color: var(--blue); }
        .actions-cell { display: flex; gap: 8px; }
        .changes-grid { display: grid; grid-template-columns: 1fr 1fr; }
        .changes-section { padding: 20px 24px; border-right: 1px solid var(--border); border-bottom: 1px solid var(--border); }
        .changes-section:nth-child(2n) { border-right: none; }
        .changes-section:nth-last-child(-n+2) { border-bottom: none; }
        .holding-item { display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid var(--border); font-size: 13px; }
        .holding-item:last-child { border-bottom: none; }
        .holding-ticker { font-family: 'JetBrains Mono', monospace; font-weight: 600; }
        .holding-value { font-family: 'JetBrains Mono', monospace; font-size: 12px; color: var(--text-secondary); }
        .holding-name { font-size: 11px; color: var(--text-muted); margin-left: 6px; }
        .alert-log { background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; overflow: hidden; margin-bottom: 32px; }
        .alert-log-header { padding: 20px 24px; background: var(--bg-secondary); border-bottom: 1px solid var(--border); }
        .alert-log-header h2 { font-size: 16px; font-weight: 600; }
        .alert-item { padding: 14px 24px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 16px; font-size: 13px; }
        .alert-item:last-child { border-bottom: none; }
        .alert-time { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--text-muted); min-width: 140px; }
        .alert-filer { font-weight: 600; min-width: 200px; }
        .alert-stats { display: flex; gap: 12px; font-family: 'JetBrains Mono', monospace; font-size: 11px; }
        .alert-stats span { padding: 2px 6px; border-radius: 3px; }
        .empty-state { text-align: center; padding: 60px 24px; color: var(--text-muted); }
        .empty-state .icon { font-size: 48px; margin-bottom: 16px; }
        .empty-state p { font-size: 14px; max-width: 400px; margin: 0 auto; }
        .spinner { display: inline-block; width: 16px; height: 16px; border: 2px solid var(--bg-primary); border-top-color: transparent; border-radius: 50%; animation: spin 0.8s linear infinite; }
        @keyframes spin { to { transform: rotate(360deg); } }
        .toast-container { position: fixed; top: 24px; right: 24px; z-index: 10000; display: flex; flex-direction: column; gap: 8px; }
        .toast { padding: 14px 20px; border-radius: 8px; font-size: 13px; font-weight: 500; animation: slideIn 0.3s ease-out; max-width: 400px; box-shadow: 0 8px 32px rgba(0,0,0,0.4); }
        .toast-success { background: var(--accent-dim); border: 1px solid var(--accent); color: var(--accent); }
        .toast-error { background: var(--red-dim); border: 1px solid var(--red); color: var(--red); }
        @keyframes slideIn { from{transform:translateX(100%);opacity:0} to{transform:translateX(0);opacity:1} }
        .modal-overlay { position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.7); backdrop-filter: blur(4px); z-index: 5000; display: none; align-items: center; justify-content: center; }
        .modal-overlay.active { display: flex; }
        .modal { background: var(--bg-card); border: 1px solid var(--border); border-radius: 16px; padding: 32px; max-width: 700px; width: 90%; max-height: 80vh; overflow-y: auto; }
        .modal h3 { font-size: 18px; margin-bottom: 20px; }
        @media (max-width: 768px) { .stats-row{grid-template-columns:repeat(2,1fr)} .changes-grid{grid-template-columns:1fr} .input-row{flex-direction:column} .header{flex-direction:column;gap:16px} }
        .search-result-item:hover { background: var(--bg-card-hover) !important; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="header-left">
                <div class="logo">13F</div>
                <div class="header-title"><h1>13F/13D Filing Monitor</h1><p id="clock">Loading...</p></div>
            </div>
            <div id="statusBadge" class="status-badge inactive"><span class="status-dot"></span><span id="statusText">INITIALIZING</span></div>
        </div>
        <div class="stats-row">
            <div class="stat-card"><div class="stat-label">Monitored Filers</div><div class="stat-value" id="statFilers">0</div></div>
            <div class="stat-card"><div class="stat-label">Filing Window</div><div class="stat-value" id="statWindow" style="font-size:14px">&#8212;</div></div>
            <div class="stat-card"><div class="stat-label">Last Check</div><div class="stat-value" id="statLastCheck" style="font-size:16px">&#8212;</div></div>
            <div class="stat-card"><div class="stat-label">Checks Today</div><div class="stat-value" id="statChecks">0</div></div>
        </div>
        <div class="add-section">
            <h2>Add 13F Filer to Monitor</h2>
            <p>Search by company or fund name, CIK number, or SEC EDGAR URL.</p>
            <div class="input-row">
                <input type="text" class="input-field" id="filerInput" placeholder="e.g. Berkshire Hathaway, Scion, 1067983, or EDGAR URL">
                <button class="btn btn-primary" id="addBtn" onclick="addFiler()"><span id="addBtnText">Add Filer</span></button>
                <button class="btn btn-secondary" onclick="testAlert()">Test 13F</button>
                <button class="btn btn-secondary" style="background:#ff6b3522;color:#ff6b35;border-color:#ff6b3544" onclick="test13DAlert()">Test 13D</button>
            </div>
            <div id="searchResults" style="display:none;margin-top:12px;background:var(--bg-primary);border:1px solid var(--accent);border-radius:8px;max-height:350px;overflow-y:auto"></div>
        </div>
        <div class="section-header"><h2>Monitored Filers</h2></div>
        <div id="filersContainer"><div class="empty-state"><div class="icon">&#128203;</div><p>No filers being monitored yet. Add a CIK number above to start.</p></div></div>
        <div class="alert-log" id="alertLogSection" style="display:none;">
            <div class="alert-log-header"><h2>Alert History</h2></div>
            <div id="alertLogBody"></div>
        </div>
    </div>
    <div class="toast-container" id="toastContainer"></div>
    <div class="modal-overlay" id="changesModal">
        <div class="modal">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:20px;">
                <h3 id="modalTitle">Holdings Changes</h3>
                <button class="btn btn-secondary btn-sm" onclick="closeModal()">Close</button>
            </div>
            <div id="modalBody"></div>
        </div>
    </div>
<script>
let filers=[],alerts=[];
function showToast(m,t){t=t||'success';const c=document.getElementById('toastContainer'),e=document.createElement('div');e.className='toast toast-'+t;e.textContent=m;c.appendChild(e);setTimeout(()=>e.remove(),4000)}
function esc(s){if(!s)return'';const d=document.createElement('div');d.textContent=s;return d.innerHTML}
function fmtD(v){if(v>=1e9)return'$'+(v/1e9).toFixed(2)+'B';if(v>=1e6)return'$'+(v/1e6).toFixed(2)+'M';if(v>=1e3)return'$'+(v/1e3).toFixed(1)+'K';return'$'+v.toLocaleString()}
async function fetchStatus(){try{const r=await fetch('/api/status'),d=await r.json();document.getElementById('clock').textContent=d.current_time_est+' | '+d.current_day+' | Poll: '+d.poll_interval;document.getElementById('statFilers').textContent=d.filer_count;document.getElementById('statChecks').textContent=d.checks_today;if(d.last_check)document.getElementById('statLastCheck').textContent=new Date(d.last_check).toLocaleTimeString();const b=document.getElementById('statusBadge'),s=document.getElementById('statusText');const fw=d.filing_window||{};if(d.poll_status==='active'){b.className='status-badge active';s.textContent='SCANNING '+((fw.quarter||'')+' filings').trim()}else if(d.filing_window_active&&d.poll_status==='weekend'){b.className='status-badge inactive';s.textContent='FILING WINDOW (weekend)'}else if(d.filing_window_active&&d.poll_status==='after_hours'){b.className='status-badge inactive';s.textContent='FILING WINDOW (after hours)'}else{b.className='status-badge inactive';s.textContent='DORMANT'+(fw.days_until_window?' • Next window in '+fw.days_until_window+'d':'')}const wi=document.getElementById('statWindow');if(wi){if(d.filing_window_active){const dl=fw.days_to_deadline;wi.textContent=fw.quarter+(dl>0?' • Deadline in '+dl+'d':' • Past deadline')}else{wi.textContent=fw.next_quarter?(fw.days_until_window+'d until '+fw.next_quarter):'—'}}}catch(e){}}
async function fetchFilers(){try{const r=await fetch('/api/filers');filers=await r.json();renderFilers()}catch(e){}}
async function fetchAlerts(){try{const r=await fetch('/api/alerts');alerts=await r.json();renderAlerts()}catch(e){}}
async function addFiler(forceCik){const i=forceCik||document.getElementById('filerInput').value.trim();if(!i)return;const b=document.getElementById('addBtn');b.disabled=true;document.getElementById('addBtnText').innerHTML='<span class="spinner"></span> Searching...';hideSearchResults();try{const r=await fetch('/api/filers',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({input:i})}),d=await r.json();if(d.success){showToast('Added '+d.name+' ('+d.total_holdings+' holdings)');document.getElementById('filerInput').value='';hideSearchResults();fetchFilers()}else if(d.search_results){showSearchResults(d.search_results)}else showToast(d.error||'Failed','error')}catch(e){showToast('Network error','error')}b.disabled=false;document.getElementById('addBtnText').textContent='Add Filer'}
function showSearchResults(results){const c=document.getElementById('searchResults');if(!results||!results.length){c.style.display='none';return}let h='<div style="font-size:12px;color:var(--text-muted);padding:8px 12px;border-bottom:1px solid var(--border)">'+results.length+' matches found — click to add:</div>';results.forEach(r=>{const badge=r.has_13f?'<span style="background:var(--accent-dim);color:var(--accent);padding:2px 6px;border-radius:3px;font-size:10px;margin-left:8px">13F FILER</span>':'<span style="background:var(--orange-dim);color:var(--orange);padding:2px 6px;border-radius:3px;font-size:10px;margin-left:8px">NO 13F</span>';h+='<div class="search-result-item" onclick="pickSearchResult(\\x27'+r.cik+'\\x27)" style="padding:10px 16px;cursor:pointer;border-bottom:1px solid var(--border);transition:background 0.15s"><div style="display:flex;align-items:center;justify-content:space-between"><div><span style="font-weight:600;font-size:14px">'+esc(r.name)+'</span>'+badge+'</div><span style="font-family:JetBrains Mono,monospace;font-size:11px;color:var(--text-muted)">CIK '+r.cik+'</span></div></div>'});c.innerHTML=h;c.style.display='block'}
function hideSearchResults(){const c=document.getElementById('searchResults');if(c)c.style.display='none'}
function pickSearchResult(cik){hideSearchResults();document.getElementById('filerInput').value=cik;addFiler(cik)}
async function removeFiler(c){if(!confirm('Remove this filer?'))return;try{const r=await fetch('/api/filers/'+c,{method:'DELETE'}),d=await r.json();if(d.success){showToast(d.message);fetchFilers()}}catch(e){showToast('Error','error')}}
async function forceCheck(c){showToast('Running force check...');try{const r=await fetch('/api/filers/'+c+'/force-check',{method:'POST'}),d=await r.json();if(d.success){showToast(d.message);fetchFilers();fetchAlerts()}}catch(e){showToast('Error','error')}}
async function viewChanges(c,n){try{const r=await fetch('/api/filers/'+c+'/changes'),d=await r.json();document.getElementById('modalTitle').textContent=n+' - Holdings Changes';const b=document.getElementById('modalBody');if(!d.new&&!d.increased&&!d.decreased&&!d.closed){b.innerHTML='<div class="empty-state" style="padding:20px"><p>No changes detected yet.</p></div>'}else{b.innerHTML=renderChanges(d)}document.getElementById('changesModal').classList.add('active')}catch(e){showToast('Error loading changes','error')}}
async function testAlert(){try{const r=await fetch('/api/test-alert',{method:'POST'}),d=await r.json();if(d.success)showToast('Test 13F alert sent to Discord')}catch(e){showToast('Error','error')}}
async function test13DAlert(){try{const r=await fetch('/api/test-13d-alert',{method:'POST'}),d=await r.json();if(d.success)showToast('Test 13D alert sent to Discord')}catch(e){showToast('Error','error')}}
function closeModal(){document.getElementById('changesModal').classList.remove('active')}
function renderFilers(){const c=document.getElementById('filersContainer');if(!filers.length){c.innerHTML='<div class="empty-state"><div class="icon">&#128203;</div><p>No filers being monitored yet.</p></div>';return}let h='<table class="filers-table"><thead><tr><th>Filer</th><th>CIK</th><th>Last Filing</th><th>Holdings</th><th>Actions</th></tr></thead><tbody>';filers.forEach(f=>{const loading=f.loading;h+='<tr><td><div class="filer-name">'+esc(f.name)+'</div></td><td>'+f.cik+'</td><td>'+(loading?'<span style="color:var(--orange)"><span class="spinner" style="border-color:var(--orange);border-top-color:transparent;width:12px;height:12px"></span> Loading...</span>':(f.last_filing_date||'&#8212;'))+'</td><td>'+(loading?'<span class="tag" style="background:var(--orange-dim);color:var(--orange)">loading...</span>':'<span class="tag tag-blue">'+(f.total_holdings||0)+' positions</span>')+'</td><td><div class="actions-cell"><button class="btn btn-secondary btn-sm" onclick="viewChanges(\\x27'+f.cik+'\\x27,\\x27'+esc(f.name).replace(/'/g,'')+'\\x27)">Changes</button><button class="btn btn-secondary btn-sm" onclick="forceCheck(\\x27'+f.cik+'\\x27)">Force Check</button><button class="btn btn-danger btn-sm" onclick="removeFiler(\\x27'+f.cik+'\\x27)">Remove</button></div></td></tr>'});h+='</tbody></table>';c.innerHTML=h}
function renderChanges(d){let h='<div class="changes-grid">';h+='<div class="changes-section"><div style="font-size:12px;font-family:JetBrains Mono,monospace;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:12px;color:var(--accent)">NEW POSITIONS ('+(d.new||[]).length+')</div>';if(d.new&&d.new.length)d.new.slice(0,20).forEach(x=>{const v=x.value_dollars||0;h+='<div class="holding-item"><div><span class="holding-ticker" style="color:var(--accent)">'+esc(x.ticker||x.cusip)+'</span><span class="holding-name">'+esc(x.name)+'</span></div><span class="holding-value">'+fmtD(v)+'</span></div>'});else h+='<div style="color:var(--text-muted);font-size:13px">None</div>';h+='</div>';h+='<div class="changes-section"><div style="font-size:12px;font-family:JetBrains Mono,monospace;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:12px;color:var(--green)">ADDED TO ('+(d.increased||[]).length+')</div>';if(d.increased&&d.increased.length)d.increased.slice(0,20).forEach(x=>{const p=x.prev_shares?((x.curr_shares-x.prev_shares)/x.prev_shares*100).toFixed(1):'?';h+='<div class="holding-item"><div><span class="holding-ticker" style="color:var(--green)">'+esc(x.ticker||x.cusip)+'</span><span class="holding-name">+'+p+'%</span></div><span class="holding-value">'+fmtD(Math.abs(x.value_change||0))+'</span></div>'});else h+='<div style="color:var(--text-muted);font-size:13px">None</div>';h+='</div>';h+='<div class="changes-section"><div style="font-size:12px;font-family:JetBrains Mono,monospace;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:12px;color:var(--orange)">REDUCED ('+(d.decreased||[]).length+')</div>';if(d.decreased&&d.decreased.length)d.decreased.slice(0,20).forEach(x=>{const p=x.prev_shares?((x.prev_shares-x.curr_shares)/x.prev_shares*100).toFixed(1):'?';h+='<div class="holding-item"><div><span class="holding-ticker" style="color:var(--orange)">'+esc(x.ticker||x.cusip)+'</span><span class="holding-name">-'+p+'%</span></div><span class="holding-value">-'+fmtD(Math.abs(x.value_change||0))+'</span></div>'});else h+='<div style="color:var(--text-muted);font-size:13px">None</div>';h+='</div>';h+='<div class="changes-section"><div style="font-size:12px;font-family:JetBrains Mono,monospace;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:12px;color:var(--red)">CLOSED ('+(d.closed||[]).length+')</div>';if(d.closed&&d.closed.length)d.closed.slice(0,20).forEach(x=>{const v=x.value_dollars||0;h+='<div class="holding-item"><div><span class="holding-ticker" style="color:var(--red)">'+esc(x.ticker||x.cusip)+'</span><span class="holding-name">'+esc(x.name)+'</span></div><span class="holding-value">'+fmtD(v)+'</span></div>'});else h+='<div style="color:var(--text-muted);font-size:13px">None</div>';h+='</div></div>';return h}
function renderAlerts(){const s=document.getElementById('alertLogSection'),b=document.getElementById('alertLogBody');if(!alerts.length){s.style.display='none';return}s.style.display='block';let h='';alerts.forEach(a=>{if(a.type==='13D'){h+='<div class="alert-item" style="border-left:3px solid #ff6b35"><span class="alert-time">'+(a.time||'')+'</span><span style="background:#ff6b3522;color:#ff6b35;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;margin-right:8px">'+(a.form_type||'SC 13D')+'</span><span class="alert-filer">'+esc(a.filer)+'</span><div class="alert-stats"><span style="background:#ff6b3522;color:#ff6b35">\u2192 '+esc(a.subject_company||'Unknown')+'</span>'+(a.subject_ticker&&a.subject_ticker!=='N/A'?'<span style="background:var(--accent-dim);color:var(--accent);font-weight:700">$'+esc(a.subject_ticker)+'</span>':'')+'</div></div>'}else{h+='<div class="alert-item"><span class="alert-time">'+(a.time||'')+'</span><span style="background:var(--accent-dim);color:var(--accent);padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;margin-right:8px">13F</span><span class="alert-filer">'+esc(a.filer)+'</span><div class="alert-stats"><span style="background:var(--accent-dim);color:var(--accent)">'+(a.new_positions||0)+' new</span><span style="background:var(--green-dim);color:var(--green)">'+(a.increased||0)+' added</span><span style="background:var(--orange-dim);color:var(--orange)">'+(a.decreased||0)+' reduced</span><span style="background:var(--red-dim);color:var(--red)">'+(a.closed||0)+' closed</span></div></div>'}});b.innerHTML=h}
document.getElementById('filerInput').addEventListener('keydown',e=>{if(e.key==='Enter')addFiler();hideSearchResults()});
document.getElementById('changesModal').addEventListener('click',e=>{if(e.target===document.getElementById('changesModal'))closeModal()});
async function poll(){await Promise.all([fetchStatus(),fetchFilers(),fetchAlerts()])}
poll();setInterval(poll,10000);
</script>
</body>
</html>'''


# ── Flask Routes ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return Response(INDEX_HTML, content_type="text/html")


@app.route("/api/status")
def api_status():
    now = datetime.now(EST)
    in_window, window_info = get_filing_window_info()
    active, poll_reason = should_poll()
    return jsonify({
        "monitoring": monitor_status["running"],
        "filing_window_active": in_window,
        "filing_window": window_info,
        "poll_status": poll_reason,
        "current_time_est": now.strftime("%I:%M:%S %p EST"),
        "current_day": now.strftime("%A"),
        "last_check": monitor_status["last_check"],
        "checks_today": monitor_status["checks_today"],
        "filer_count": len(monitored_filers),
        "poll_interval": f"{POLL_INTERVAL_SECONDS}s",
        "recent_errors": monitor_status["errors"][:5],
    })


@app.route("/api/filers", methods=["GET"])
def api_get_filers():
    result = []
    for cik, data in monitored_filers.items():
        result.append({
            "cik": cik,
            "name": data.get("name", "Unknown"),
            "last_filing_date": data.get("last_filing_date"),
            "last_filing_accession": data.get("last_filing_accession"),
            "total_holdings": data.get("total_holdings", 0),
            "url": data.get("url", ""),
            "loading": data.get("_needs_init", False),
        })
    return jsonify(result)


@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q or len(q) < 2:
        return jsonify({"results": []})
    results = search_sec_companies(q)
    return jsonify({"results": results})


@app.route("/api/filers", methods=["POST"])
def api_add_filer():
    body = request.json or {}
    input_val = body.get("input", "").strip()

    if not input_val:
        return jsonify({"error": "No input provided"}), 400

    cik = None
    if input_val.startswith("http"):
        cik = extract_cik_from_url(input_val)
        if not cik:
            return jsonify({"error": "Could not extract CIK from URL"}), 400
    elif input_val.isdigit():
        cik = input_val.lstrip('0') or '0'
    else:
        # Text input — search SEC by company name
        results = search_sec_companies(input_val)
        if not results:
            return jsonify({"error": f"No companies found matching '{input_val}'"}), 404
        if len(results) == 1:
            # Exact single match — auto-add
            cik = results[0]["cik"]
        else:
            # Multiple matches — return search results for user to pick
            return jsonify({"search_results": results})

    if cik in monitored_filers:
        return jsonify({"error": f"CIK {cik} is already being monitored"}), 400

    info = get_company_info(cik)
    if not info:
        return jsonify({"error": f"Could not find company with CIK {cik}"}), 404

    filings_13f = find_13f_filings(info["filings"])

    filer_entry = {
        "name": info["name"],
        "cik": cik,
        "url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={pad_cik(cik)}&type=13F-HR",
        "last_filing_accession": None,
        "last_filing_date": None,
        "holdings_prev": {},
        "holdings_current": {},
        "total_holdings": 0,
        "last_changes": None,
        "user_added": True,
    }

    if filings_13f:
        latest = filings_13f[0]
        filer_entry["last_filing_accession"] = latest["accession"]
        filer_entry["last_filing_date"] = latest["date"]

        index_html, base_url = fetch_filing_index(cik, latest["accession"])
        infotable_url = find_infotable_url(index_html, base_url, cik, latest["accession"])
        if infotable_url:
            holdings = parse_13f_holdings(infotable_url)
            filer_entry["holdings_current"] = holdings
            filer_entry["holdings_prev"] = holdings
            filer_entry["total_holdings"] = len(holdings)

        if len(filings_13f) > 1:
            prev = filings_13f[1]
            time.sleep(0.5)
            prev_index_html, prev_base_url = fetch_filing_index(cik, prev["accession"])
            prev_infotable_url = find_infotable_url(prev_index_html, prev_base_url, cik, prev["accession"])
            if prev_infotable_url:
                prev_holdings = parse_13f_holdings(prev_infotable_url)
                filer_entry["holdings_prev"] = prev_holdings

    monitored_filers[cik] = filer_entry
    save_state()

    return jsonify({
        "success": True,
        "name": info["name"],
        "cik": cik,
        "existing_filings": len(filings_13f),
        "last_filing_date": filer_entry["last_filing_date"],
        "total_holdings": filer_entry["total_holdings"],
    })


@app.route("/api/filers/<cik>", methods=["DELETE"])
def api_remove_filer(cik):
    if cik in monitored_filers:
        name = monitored_filers[cik]["name"]
        del monitored_filers[cik]
        save_state()
        return jsonify({"success": True, "message": f"Removed {name}"})
    return jsonify({"error": "Filer not found"}), 404


@app.route("/api/filers/<cik>/changes")
def api_filer_changes(cik):
    if cik not in monitored_filers:
        return jsonify({"error": "Filer not found"}), 404

    filer = monitored_filers[cik]
    changes = filer.get("last_changes")

    if not changes:
        prev = filer.get("holdings_prev", {})
        curr = filer.get("holdings_current", {})
        if prev and curr and prev != curr:
            changes = compare_holdings(prev, curr)
        else:
            return jsonify({"message": "No changes detected yet", "changes": None})

    def serialize_holdings(holdings_dict):
        result = []
        for key, h in sorted(holdings_dict.items(), key=lambda x: x[1].get("value_dollars", _holding_value_dollars(x[1])), reverse=True):
            result.append({k: v for k, v in h.items()})
        return result

    return jsonify({
        "filer": filer["name"],
        "cik": cik,
        "new": serialize_holdings(changes["new"]),
        "increased": serialize_holdings(changes["increased"]),
        "decreased": serialize_holdings(changes["decreased"]),
        "closed": serialize_holdings(changes["closed"]),
    })


@app.route("/api/filers/<cik>/force-check", methods=["POST"])
def api_force_check(cik):
    if cik not in monitored_filers:
        return jsonify({"error": "Filer not found"}), 404

    filer = monitored_filers[cik]
    filer["last_filing_accession"] = None
    check_filer(cik, filer)

    return jsonify({"success": True, "message": f"Force check completed for {filer['name']}"})


@app.route("/api/alerts")
def api_alerts():
    return jsonify(alert_log[:50])


@app.route("/api/test-alert", methods=["POST"])
def api_test_alert():
    test_changes = {
        "new": {
            "TEST123": {
                "cusip": "TEST123", "name": "Test Corp", "ticker": "TEST",
                "value_thousands": 50000, "value_dollars": 50000000,
                "shares": 1000000, "put_call": "",
            }
        },
        "increased": {
            "AAPL123": {
                "cusip": "AAPL123", "name": "Apple Inc", "ticker": "AAPL",
                "value_thousands": 100000, "value_dollars": 100000000,
                "shares": 500000, "prev_shares": 400000, "curr_shares": 500000,
                "share_change": 100000, "prev_value": 80000000, "curr_value": 100000000,
                "value_change": 20000000, "put_call": "",
            }
        },
        "decreased": {},
        "closed": {},
    }
    send_discord_alert("Test Filer", "0000000", datetime.now(EST).strftime("%Y-%m-%d"), test_changes)
    return jsonify({"success": True, "message": "Test 13F alert sent"})


@app.route("/api/test-13d-alert", methods=["POST"])
def api_test_13d_alert():
    test_subject = {
        "name": "Test Target Corp",
        "cik": "1234567",
        "ticker": "TGTC",
        "cusip": "88160R101",
    }
    send_discord_13d_alert(
        "Test Activist Fund", "0000000",
        datetime.now(EST).strftime("%Y-%m-%d"),
        test_subject, "0000000-25-000001", is_amendment=False
    )
    return jsonify({"success": True, "message": "Test 13D alert sent"})


if __name__ == "__main__":
    start_scheduler()
    load_state()
    load_default_filers()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

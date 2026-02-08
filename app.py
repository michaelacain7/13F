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
from flask import Flask, render_template, request, jsonify, redirect, url_for
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
POLL_INTERVAL_SECONDS = 60  # Check every 60 seconds

# ── In-memory state ──────────────────────────────────────────────────────────
monitored_filers = {}       # cik -> {name, cik, url, last_filing_accession, holdings_prev, holdings_current}
alert_log = []              # List of alert dicts
monitor_status = {
    "running": False,
    "last_check": None,
    "checks_today": 0,
    "errors": [],
}

# ── SEC EDGAR Helpers ────────────────────────────────────────────────────────

def pad_cik(cik):
    """Pad CIK to 10 digits."""
    return str(cik).zfill(10)


def extract_cik_from_url(url):
    """Extract CIK from an EDGAR URL."""
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


def get_company_info(cik):
    """Fetch company name and recent filings from EDGAR."""
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
    """Extract 13F-HR filings from the filings data."""
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
    """Fetch the filing index page to find the infotable XML."""
    padded = pad_cik(cik)
    acc_clean = accession.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{padded}/{acc_clean}/"
    try:
        r = requests.get(url, headers=SEC_HEADERS, timeout=15)
        r.raise_for_status()
        return r.text, url
    except Exception as e:
        log.error(f"Error fetching filing index: {e}")
        return None, url


def find_infotable_url(index_html, base_url, cik, accession):
    """Find the information table XML file URL from the filing index."""
    padded = pad_cik(cik)
    acc_clean = accession.replace("-", "")

    # Try the standard EDGAR XML path first
    # Pattern: look for links to XML files that contain "infotable" or similar
    xml_patterns = [
        r'href="([^"]*infotable[^"]*\.xml)"',
        r'href="([^"]*information[_ ]?table[^"]*\.xml)"',
        r'href="([^"]*13[fF][^"]*\.xml)"',
        r'href="([^"]*\.xml)"',
    ]

    if index_html:
        for pattern in xml_patterns:
            matches = re.findall(pattern, index_html, re.IGNORECASE)
            for match in matches:
                # Skip the primary document XML (usually the form itself)
                if 'primary_doc' in match.lower():
                    continue
                if match.startswith("http"):
                    return match
                return f"https://www.sec.gov/Archives/edgar/data/{padded}/{acc_clean}/{match}"

    # Fallback: try common naming conventions
    common_names = [
        "infotable.xml",
        "InfoTable.xml",
        "INFOTABLE.XML",
        "information_table.xml",
    ]
    for name in common_names:
        test_url = f"https://www.sec.gov/Archives/edgar/data/{padded}/{acc_clean}/{name}"
        try:
            r = requests.head(test_url, headers=SEC_HEADERS, timeout=10)
            if r.status_code == 200:
                return test_url
        except:
            pass

    return None


def parse_13f_holdings(xml_url):
    """Parse a 13F infotable XML and return holdings dict keyed by CUSIP."""
    try:
        r = requests.get(xml_url, headers=SEC_HEADERS, timeout=20)
        r.raise_for_status()
        content = r.text

        # Remove namespace prefixes for easier parsing
        content = re.sub(r'xmlns[^"]*"[^"]*"', '', content)
        content = re.sub(r'<ns\d+:', '<', content)
        content = re.sub(r'</ns\d+:', '</', content)
        content = re.sub(r'<informationTable:', '<', content)
        content = re.sub(r'</informationTable:', '</', content)

        root = ET.fromstring(content)

        holdings = {}
        # Find all infoTable entries
        for entry in root.iter():
            if 'infotable' in entry.tag.lower() or 'infoTable' in entry.tag:
                cusip = None
                name_of_issuer = ""
                title = ""
                value = 0  # in thousands
                shares = 0
                put_call = ""

                for child in entry:
                    tag = child.tag.split('}')[-1].lower() if '}' in child.tag else child.tag.lower()
                    text = (child.text or "").strip()

                    if tag == "cusip":
                        cusip = text
                    elif tag == "nameofissuer":
                        name_of_issuer = text
                    elif tag == "titleofclass":
                        title = text
                    elif tag == "value":
                        try:
                            value = int(text)
                        except:
                            value = 0
                    elif tag == "sshprnamt" or tag == "shrsorprnamt":
                        # This is a container element
                        for sub in child:
                            sub_tag = sub.tag.split('}')[-1].lower() if '}' in sub.tag else sub.tag.lower()
                            if sub_tag in ("sshprnamt", "sshprnamttype"):
                                if sub_tag == "sshprnamt":
                                    try:
                                        shares = int(sub.text.strip())
                                    except:
                                        shares = 0
                    elif tag == "putcall":
                        put_call = text

                if cusip:
                    key = cusip
                    if put_call:
                        key = f"{cusip}_{put_call}"
                    holdings[key] = {
                        "cusip": cusip,
                        "name": name_of_issuer,
                        "title": title,
                        "value_thousands": value,
                        "shares": shares,
                        "put_call": put_call,
                    }

        log.info(f"Parsed {len(holdings)} holdings from {xml_url}")
        return holdings
    except Exception as e:
        log.error(f"Error parsing 13F XML from {xml_url}: {e}")
        return {}


def cusip_to_ticker(cusip):
    """Attempt to resolve CUSIP to ticker symbol. Returns CUSIP if lookup fails."""
    # Use SEC EDGAR company search or a mapping
    try:
        url = f"https://efts.sec.gov/LATEST/search-index?q=%22{cusip}%22&forms=13F-HR"
        # This is a best-effort approach - we'll try OpenFIGI
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


# ── Ticker cache to avoid repeated lookups ───────────────────────────────────
ticker_cache = {}


def get_ticker(cusip, name=""):
    """Get ticker for a CUSIP, with caching."""
    if cusip in ticker_cache:
        return ticker_cache[cusip]
    ticker = cusip_to_ticker(cusip)
    ticker_cache[cusip] = ticker
    return ticker


# ── Comparison Logic ─────────────────────────────────────────────────────────

def compare_holdings(prev_holdings, curr_holdings):
    """Compare two quarters of 13F holdings and return changes."""
    prev_keys = set(prev_holdings.keys())
    curr_keys = set(curr_holdings.keys())

    new_positions = {}       # In current but not in previous
    closed_positions = {}    # In previous but not in current
    increased_positions = {} # In both, but shares increased
    decreased_positions = {} # In both, but shares decreased

    # New positions
    for key in curr_keys - prev_keys:
        h = curr_holdings[key]
        ticker = get_ticker(h["cusip"], h["name"])
        new_positions[key] = {
            **h,
            "ticker": ticker,
            "value_dollars": h["value_thousands"] * 1000,
        }

    # Closed positions
    for key in prev_keys - curr_keys:
        h = prev_holdings[key]
        ticker = get_ticker(h["cusip"], h["name"])
        closed_positions[key] = {
            **h,
            "ticker": ticker,
            "value_dollars": h["value_thousands"] * 1000,
        }

    # Changed positions
    for key in prev_keys & curr_keys:
        prev = prev_holdings[key]
        curr = curr_holdings[key]
        ticker = get_ticker(curr["cusip"], curr["name"])

        share_diff = curr["shares"] - prev["shares"]
        value_diff = (curr["value_thousands"] - prev["value_thousands"]) * 1000

        if share_diff > 0:
            increased_positions[key] = {
                **curr,
                "ticker": ticker,
                "prev_shares": prev["shares"],
                "curr_shares": curr["shares"],
                "share_change": share_diff,
                "prev_value": prev["value_thousands"] * 1000,
                "curr_value": curr["value_thousands"] * 1000,
                "value_change": value_diff,
            }
        elif share_diff < 0:
            decreased_positions[key] = {
                **curr,
                "ticker": ticker,
                "prev_shares": prev["shares"],
                "curr_shares": curr["shares"],
                "share_change": share_diff,
                "prev_value": prev["value_thousands"] * 1000,
                "curr_value": curr["value_thousands"] * 1000,
                "value_change": value_diff,
            }

    return {
        "new": new_positions,
        "closed": closed_positions,
        "increased": increased_positions,
        "decreased": decreased_positions,
    }


# ── Discord Alert ────────────────────────────────────────────────────────────

def format_dollar(amount):
    """Format dollar amount with commas."""
    if amount >= 1_000_000_000:
        return f"${amount / 1_000_000_000:.2f}B"
    elif amount >= 1_000_000:
        return f"${amount / 1_000_000:.2f}M"
    elif amount >= 1_000:
        return f"${amount / 1_000:.1f}K"
    return f"${amount:,.0f}"


def send_discord_alert(filer_name, cik, filing_date, changes):
    """Send a Discord webhook alert with 13F changes."""
    new_pos = changes["new"]
    increased = changes["increased"]
    closed = changes["closed"]
    decreased = changes["decreased"]

    # Build embed fields
    embeds = []

    # Main embed
    main_embed = {
        "title": f"📊 New 13F Filing Detected",
        "description": f"**{filer_name}** (CIK: {cik})\nFiling Date: {filing_date}",
        "color": 0x00D4AA,  # Teal
        "timestamp": datetime.now(EST).isoformat(),
        "footer": {"text": "13F Monitor • SEC EDGAR"},
    }

    fields = []

    # New Positions
    if new_pos:
        lines = []
        for key, h in sorted(new_pos.items(), key=lambda x: x[1]["value_dollars"], reverse=True)[:15]:
            ticker = h["ticker"]
            val = format_dollar(h["value_dollars"])
            shares = f"{h['shares']:,}" if h['shares'] else "N/A"
            pc = f" ({h['put_call']})" if h.get('put_call') else ""
            lines.append(f"**{ticker}**{pc} — {val} ({shares} shares)")
        if len(new_pos) > 15:
            lines.append(f"*...and {len(new_pos) - 15} more*")
        fields.append({
            "name": f"🆕 New Positions ({len(new_pos)})",
            "value": "\n".join(lines) or "None",
            "inline": False,
        })

    # Increased Positions
    if increased:
        lines = []
        for key, h in sorted(increased.items(), key=lambda x: abs(x[1]["value_change"]), reverse=True)[:15]:
            ticker = h["ticker"]
            change = format_dollar(abs(h["value_change"]))
            share_pct = ((h["curr_shares"] - h["prev_shares"]) / h["prev_shares"] * 100) if h["prev_shares"] else 0
            lines.append(f"**{ticker}** — +{change} (+{share_pct:.1f}% shares)")
        if len(increased) > 15:
            lines.append(f"*...and {len(increased) - 15} more*")
        fields.append({
            "name": f"📈 Added To ({len(increased)})",
            "value": "\n".join(lines) or "None",
            "inline": False,
        })

    # Closed Positions
    if closed:
        lines = []
        for key, h in sorted(closed.items(), key=lambda x: x[1]["value_dollars"], reverse=True)[:10]:
            ticker = h["ticker"]
            val = format_dollar(h["value_dollars"])
            lines.append(f"**{ticker}** — {val}")
        if len(closed) > 10:
            lines.append(f"*...and {len(closed) - 10} more*")
        fields.append({
            "name": f"🚫 Closed Positions ({len(closed)})",
            "value": "\n".join(lines) or "None",
            "inline": False,
        })

    # Decreased Positions
    if decreased:
        lines = []
        for key, h in sorted(decreased.items(), key=lambda x: abs(x[1]["value_change"]), reverse=True)[:10]:
            ticker = h["ticker"]
            change = format_dollar(abs(h["value_change"]))
            share_pct = ((h["prev_shares"] - h["curr_shares"]) / h["prev_shares"] * 100) if h["prev_shares"] else 0
            lines.append(f"**{ticker}** — -{change} (-{share_pct:.1f}% shares)")
        if len(decreased) > 10:
            lines.append(f"*...and {len(decreased) - 10} more*")
        fields.append({
            "name": f"📉 Reduced Positions ({len(decreased)})",
            "value": "\n".join(lines) or "None",
            "inline": False,
        })

    if not fields:
        fields.append({
            "name": "ℹ️ No Changes Detected",
            "value": "Holdings appear unchanged from previous quarter.",
            "inline": False,
        })

    # Summary field
    summary = (
        f"New: **{len(new_pos)}** | Added: **{len(increased)}** | "
        f"Reduced: **{len(decreased)}** | Closed: **{len(closed)}**"
    )
    fields.insert(0, {
        "name": "📋 Summary",
        "value": summary,
        "inline": False,
    })

    main_embed["fields"] = fields
    embeds.append(main_embed)

    payload = {
        "username": "13F Monitor",
        "avatar_url": "https://www.sec.gov/files/sec-logo.png",
        "embeds": embeds,
    }

    for webhook_url in DISCORD_WEBHOOKS:
        try:
            r = requests.post(webhook_url, json=payload, timeout=10)
            if r.status_code in (200, 204):
                log.info(f"Discord alert sent to webhook")
            else:
                log.error(f"Discord webhook failed: {r.status_code} {r.text}")
        except Exception as e:
            log.error(f"Discord webhook error: {e}")
        time.sleep(0.5)  # Rate limit buffer


# ── Monitor Logic ────────────────────────────────────────────────────────────

def is_market_hours():
    """Check if current time is 6am-6pm EST on a weekday."""
    now = datetime.now(EST)
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    if now.hour < 6 or now.hour >= 18:
        return False
    return True


def check_filer(cik, filer_data):
    """Check a single filer for new 13F filings."""
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

        # Check if this is a new filing we haven't seen
        if latest_accession == filer_data.get("last_filing_accession"):
            return  # Already processed

        log.info(f"🔔 NEW 13F detected for {filer_data['name']}: {latest_accession} ({latest['date']})")

        # Fetch and parse the new filing
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

        # Get previous quarter's filing for comparison
        prev_holdings = filer_data.get("holdings_prev", {})

        # If we don't have previous holdings yet, try to get the second most recent filing
        if not prev_holdings and len(filings_13f) > 1:
            prev_filing = filings_13f[1]
            prev_index_html, prev_base_url = fetch_filing_index(cik, prev_filing["accession"])
            prev_infotable_url = find_infotable_url(prev_index_html, prev_base_url, cik, prev_filing["accession"])
            if prev_infotable_url:
                prev_holdings = parse_13f_holdings(prev_infotable_url)
                time.sleep(0.5)  # SEC rate limit

        # Compare holdings
        changes = compare_holdings(prev_holdings, curr_holdings)

        # Send Discord alert
        send_discord_alert(filer_data["name"], cik, latest["date"], changes)

        # Update state
        filer_data["last_filing_accession"] = latest_accession
        filer_data["holdings_prev"] = curr_holdings  # Current becomes previous for next time
        filer_data["holdings_current"] = curr_holdings
        filer_data["last_filing_date"] = latest["date"]
        filer_data["last_changes"] = changes
        filer_data["total_holdings"] = len(curr_holdings)

        # Log the alert
        alert_entry = {
            "time": datetime.now(EST).isoformat(),
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

        log.info(f"✅ Processed 13F for {filer_data['name']}: "
                 f"{len(changes['new'])} new, {len(changes['increased'])} added, "
                 f"{len(changes['decreased'])} reduced, {len(changes['closed'])} closed")

    except Exception as e:
        log.error(f"Error checking filer {cik}: {e}")
        monitor_status["errors"].append({
            "time": datetime.now(EST).isoformat(),
            "msg": f"Error checking {filer_data.get('name', cik)}: {str(e)}",
        })
        if len(monitor_status["errors"]) > 50:
            monitor_status["errors"] = monitor_status["errors"][:50]


def monitor_loop():
    """Main monitoring loop, runs on schedule."""
    if not is_market_hours():
        now = datetime.now(EST)
        monitor_status["running"] = False
        log.debug(f"Outside monitoring hours ({now.strftime('%H:%M %Z')} {now.strftime('%A')})")
        return

    monitor_status["running"] = True
    monitor_status["last_check"] = datetime.now(EST).isoformat()
    monitor_status["checks_today"] += 1

    for cik, filer_data in list(monitored_filers.items()):
        check_filer(cik, filer_data)
        time.sleep(1)  # SEC rate limiting: 10 requests per second max


# ── Initialize scheduler ─────────────────────────────────────────────────────
scheduler = BackgroundScheduler(timezone=EST)
scheduler.add_job(monitor_loop, 'interval', seconds=POLL_INTERVAL_SECONDS, id='monitor_job',
                  max_instances=1, coalesce=True)


# ── Flask Routes ─────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    now = datetime.now(EST)
    return jsonify({
        "monitoring": monitor_status["running"],
        "market_hours": is_market_hours(),
        "current_time_est": now.strftime("%I:%M:%S %p EST"),
        "current_day": now.strftime("%A"),
        "last_check": monitor_status["last_check"],
        "checks_today": monitor_status["checks_today"],
        "filer_count": len(monitored_filers),
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
        })
    return jsonify(result)


@app.route("/api/filers", methods=["POST"])
def api_add_filer():
    body = request.json or {}
    input_val = body.get("input", "").strip()

    if not input_val:
        return jsonify({"error": "No input provided"}), 400

    # Determine if it's a URL or a CIK
    cik = None
    if input_val.startswith("http"):
        cik = extract_cik_from_url(input_val)
        if not cik:
            return jsonify({"error": "Could not extract CIK from URL"}), 400
    elif input_val.isdigit():
        cik = input_val.lstrip('0') or '0'
    else:
        # Try searching by company name
        try:
            search_url = f"https://efts.sec.gov/LATEST/search-index?q=%22{requests.utils.quote(input_val)}%22&forms=13F-HR"
            r = requests.get(search_url, headers=SEC_HEADERS, timeout=10)
            if r.status_code == 200:
                # Fallback: treat as CIK
                return jsonify({"error": "Please provide a CIK number or EDGAR URL"}), 400
        except:
            return jsonify({"error": "Please provide a CIK number or EDGAR URL"}), 400

    if cik in monitored_filers:
        return jsonify({"error": f"CIK {cik} is already being monitored"}), 400

    # Fetch company info
    info = get_company_info(cik)
    if not info:
        return jsonify({"error": f"Could not find company with CIK {cik}"}), 404

    # Get existing 13F filings to establish baseline
    filings_13f = find_13f_filings(info["filings"])

    filer_entry = {
        "name": info["name"],
        "cik": cik,
        "url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={pad_cik(cik)}&type=13F-HR&dateb=&owner=include&count=40",
        "last_filing_accession": None,
        "last_filing_date": None,
        "holdings_prev": {},
        "holdings_current": {},
        "total_holdings": 0,
        "last_changes": None,
    }

    # Load the most recent filing as baseline
    if filings_13f:
        latest = filings_13f[0]
        filer_entry["last_filing_accession"] = latest["accession"]
        filer_entry["last_filing_date"] = latest["date"]

        # Parse current holdings
        index_html, base_url = fetch_filing_index(cik, latest["accession"])
        infotable_url = find_infotable_url(index_html, base_url, cik, latest["accession"])
        if infotable_url:
            holdings = parse_13f_holdings(infotable_url)
            filer_entry["holdings_current"] = holdings
            filer_entry["holdings_prev"] = holdings  # Use as baseline
            filer_entry["total_holdings"] = len(holdings)

        # Try to load previous quarter for comparison data
        if len(filings_13f) > 1:
            prev = filings_13f[1]
            time.sleep(0.5)
            prev_index_html, prev_base_url = fetch_filing_index(cik, prev["accession"])
            prev_infotable_url = find_infotable_url(prev_index_html, prev_base_url, cik, prev["accession"])
            if prev_infotable_url:
                prev_holdings = parse_13f_holdings(prev_infotable_url)
                filer_entry["holdings_prev"] = prev_holdings

    monitored_filers[cik] = filer_entry

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
        return jsonify({"success": True, "message": f"Removed {name}"})
    return jsonify({"error": "Filer not found"}), 404


@app.route("/api/filers/<cik>/changes")
def api_filer_changes(cik):
    if cik not in monitored_filers:
        return jsonify({"error": "Filer not found"}), 404

    filer = monitored_filers[cik]
    changes = filer.get("last_changes")

    if not changes:
        # Generate comparison from stored holdings
        prev = filer.get("holdings_prev", {})
        curr = filer.get("holdings_current", {})
        if prev and curr and prev != curr:
            changes = compare_holdings(prev, curr)
        else:
            return jsonify({"message": "No changes detected yet", "changes": None})

    # Serialize changes for JSON
    def serialize_holdings(holdings_dict):
        result = []
        for key, h in sorted(holdings_dict.items(), key=lambda x: x[1].get("value_dollars", x[1].get("value_thousands", 0) * 1000), reverse=True):
            entry = {k: v for k, v in h.items()}
            result.append(entry)
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
    """Force an immediate check for a specific filer (ignores market hours)."""
    if cik not in monitored_filers:
        return jsonify({"error": "Filer not found"}), 404

    # Temporarily clear the last accession to force re-check
    filer = monitored_filers[cik]
    old_accession = filer.get("last_filing_accession")
    filer["last_filing_accession"] = None

    check_filer(cik, filer)

    return jsonify({"success": True, "message": f"Force check completed for {filer['name']}"})


@app.route("/api/alerts")
def api_alerts():
    return jsonify(alert_log[:50])


@app.route("/api/test-alert", methods=["POST"])
def api_test_alert():
    """Send a test Discord alert."""
    test_changes = {
        "new": {
            "TEST123": {
                "cusip": "TEST123",
                "name": "Test Corp",
                "ticker": "TEST",
                "value_thousands": 50000,
                "value_dollars": 50000000,
                "shares": 1000000,
                "put_call": "",
            }
        },
        "increased": {
            "AAPL123": {
                "cusip": "AAPL123",
                "name": "Apple Inc",
                "ticker": "AAPL",
                "value_thousands": 100000,
                "value_dollars": 100000000,
                "shares": 500000,
                "prev_shares": 400000,
                "curr_shares": 500000,
                "share_change": 100000,
                "prev_value": 80000000,
                "curr_value": 100000000,
                "value_change": 20000000,
                "put_call": "",
            }
        },
        "decreased": {},
        "closed": {},
    }
    send_discord_alert("Test Filer", "0000000", datetime.now(EST).strftime("%Y-%m-%d"), test_changes)
    return jsonify({"success": True, "message": "Test alert sent"})


# ── Start ────────────────────────────────────────────────────────────────────

# Start the scheduler when the module loads (works with gunicorn)
if not scheduler.running:
    scheduler.start()
    log.info("🚀 13F Monitor scheduler started. Polling every %d seconds.", POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)

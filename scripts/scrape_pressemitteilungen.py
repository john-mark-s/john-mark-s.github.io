"""
Scrapes all pages of Pressemitteilungen from Bezirksamt Steglitz-Zehlendorf
and merges them into pressemitteilungen/data.json.

New entries are added; existing entries (matched by URL) are never duplicated.
"""

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL   = "https://www.berlin.de"
START_URL  = "/ba-steglitz-zehlendorf/aktuelles/pressemitteilungen/"
EINHEIT    = "Steglitz-Zehlendorf"
OUT_FILE   = Path("pressemitteilungen/data.json")
HEADERS    = {"User-Agent": "Mozilla/5.0 (compatible; PM-Scraper/1.0)"}


def parse_date(raw: str) -> str | None:
    """Turn '06.03.2026 09:46 Uhr' into '2026-03-06'."""
    m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def scrape_page(path: str) -> tuple[list[dict], str | None]:
    """Returns (entries_on_page, next_page_path_or_None)."""
    url = BASE_URL + path
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    entries = []

    # Press releases are inside <ul> lists where each <li> contains a date
    # text node and an <a> link. We look for the section heading "Aktuelle
    # Pressemitteilungen" and grab everything below it.
    for li in soup.select("ul li"):
        a = li.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        # Only keep links that look like actual press-release articles
        if "pressemitteilung" not in href:
            continue
        title = a.get_text(strip=True)
        if not title:
            continue
        # The date lives as a text node before the <a> tag
        raw_text = li.get_text(" ", strip=True)
        date = parse_date(raw_text)
        full_url = BASE_URL + href if href.startswith("/") else href
        entries.append({
            "date":   date or "",
            "title":  title,
            "url":    full_url,
            "einheit": EINHEIT,
        })

    # Find "next page" link – the pager uses ?page_at_1_0=N
    next_path = None
    for a in soup.select("a[href]"):
        href = a["href"]
        if "page_at_1_0=" in href and ("›" in a.get_text() or ">" in a.get_text()):
            next_path = href if href.startswith("/") else START_URL + href
            break

    return entries, next_path


def load_existing() -> list[dict]:
    if OUT_FILE.exists():
        with open(OUT_FILE, encoding="utf-8") as f:
            return json.load(f)
    return []


def save(data: list[dict]) -> None:
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main() -> None:
    existing = load_existing()
    known_urls = {row["url"] for row in existing}

    new_entries: list[dict] = []
    path = START_URL
    page = 1

    while path:
        print(f"  Scraping page {page}: {path}")
        try:
            entries, next_path = scrape_page(path)
        except Exception as e:
            print(f"  Error on page {page}: {e}")
            break

        added_this_page = 0
        for entry in entries:
            if entry["url"] not in known_urls:
                new_entries.append(entry)
                known_urls.add(entry["url"])
                added_this_page += 1

        print(f"  → {len(entries)} found, {added_this_page} new")

        # Once a whole page has zero new entries, stop paginating —
        # everything after this point is already in our archive.
        if added_this_page == 0:
            print("  No new entries on this page – stopping.")
            break

        path = next_path
        page += 1
        time.sleep(1)  # be polite to berlin.de

    if new_entries:
        combined = new_entries + existing  # newest first
        save(combined)
        print(f"\nDone. Added {len(new_entries)} new entries. Total: {len(combined)}")
    else:
        print("\nNo new entries found. data.json unchanged.")


if __name__ == "__main__":
    main()

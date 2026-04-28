"""
Winery Vacation – Weekly Availability Scraper
voyager/booking-scraper → Supabase Edge Function

Läuft wöchentlich via GitHub Actions (montags 03:00 UTC).
"""

import os
import sys
import time
import logging
from datetime import date, timedelta, datetime

import httpx
from apify_client import ApifyClient

# ── Config ───────────────────────────────────────────────────────────────────

APIFY_TOKEN       = os.environ["APIFY_TOKEN"]
MAKE_API_KEY      = os.environ["MAKE_API_KEY"]
SUPABASE_ENDPOINT = (
    "https://pfvupcmrxrnkjyqlopyz.supabase.co"
    "/functions/v1/bulk-import-availabilities"
)

# Nächste 12 Wochen, wöchentliche Check-in-Slots
CHECK_IN_DATES = [
    (date.today() + timedelta(weeks=w)).isoformat()
    for w in range(1, 13)
]

APIFY_BATCH_SIZE    = 20   # Hotels pro Apify-Run
SUPABASE_BATCH_SIZE = 500  # Hard Limit des Endpoints
MAX_RETRIES         = 3
RETRY_DELAY         = 10   # Sekunden (wird multipliziert pro Versuch)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Hotels laden ──────────────────────────────────────────────────────────────

from hotels import HOTEL_URLS  # Liste der 120 Booking.com URLs (siehe hotels.py)

# ── Apify ─────────────────────────────────────────────────────────────────────

def run_apify(hotel_urls: list[str], check_in: str) -> list[dict]:
    """Einen Apify-Run für eine URL-Batch + Check-in-Datum anstoßen."""
    check_out = (date.fromisoformat(check_in) + timedelta(days=1)).isoformat()
    client    = ApifyClient(APIFY_TOKEN)

    run_input = {
        "startUrls": [{"url": url} for url in hotel_urls],
        "checkIn":   check_in,
        "checkOut":  check_out,
        "currency":  "EUR",
        "language":  "de",
        "rooms":     1,
        "adults":    2,
        # Zimmer-Details & Ausstattung aktivieren (kostenpflichtiges Add-on):
        # "scrapeRoomOfferings": True,
    }

    log.info("Apify | %d URLs | check_in=%s", len(hotel_urls), check_in)
    run   = client.actor("voyager/booking-scraper").call(run_input=run_input)
    items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
    log.info("  → %d Items", len(items))
    return items


# ── Mapping: Apify-Output → Supabase-Records ─────────────────────────────────

def extract_hotel_id(url: str) -> str:
    """https://www.booking.com/hotel/de/SLUG.html → SLUG"""
    try:
        return url.split("/hotel/")[1].split("/")[1].split(".html")[0].split("?")[0]
    except Exception:
        return url


def is_valid_date(date_str: str) -> bool:
    """Prüft ob ein String ein valides ISO-Datum (YYYY-MM-DD) ist."""
    if not date_str or not isinstance(date_str, str):
        return False
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


def map_item(item: dict) -> list[dict]:
    """
    Ein Apify-Item → eine oder mehrere Supabase-Records.
    Liste, weil ein Hotel mehrere Zimmertypen haben kann.
    """
    records  = []
    url      = item.get("url", "")
    hotel_id = item.get("hotelId") or extract_hotel_id(url)
    check_in = item.get("checkIn") or item.get("checkin")

    # Validierung: check_in muss ein valides ISO-Datum sein
    if not is_valid_date(check_in):
        log.debug("Ungültiges Datum '%s' für %s – übersprungen", check_in, hotel_id)
        return []

    # ── Mit Room-Offerings Add-on (detaillierte Zimmer-Ebene) ────────────────
    room_offerings = item.get("roomOfferings") or []
    if room_offerings:
        for room in room_offerings:
            price = room.get("price") or room.get("pricePerNight")
            if price is None:
                continue

            room_id      = str(room.get("id") or room.get("roomId") or "default")
            room_name    = room.get("name") or room.get("roomName") or "Zimmer"
            mealplan     = (room.get("mealplan") or room.get("boardType") or "RO").upper()
            persons      = int(room.get("maxPersons") or room.get("persons") or 2)
            cancellation = "free" if room.get("freeCancellation") else "non-refundable"
            rooms_left   = int(room.get("roomsLeft") or 0)
            # Ausstattung als kommaseparierter String
            facilities   = room.get("facilities") or room.get("amenities") or []
            facilities_str = ", ".join(facilities) if isinstance(facilities, list) else str(facilities)

            avail_key = f"{hotel_id}_{check_in}_{room_id}_{mealplan}_{persons}_{cancellation}"

            records.append({
                "availability_key": avail_key,
                "hotel_id":    hotel_id,
                "room_id":     room_id,
                "date":        check_in,
                "price_eur":   float(price),
                "rooms_left":  rooms_left,
                "available":   rooms_left > 0,
                "room_name":   room_name,
                "mealplan":    mealplan,
                "persons":     persons,
                "cancellation": cancellation,
                "source":      "apify-weekly",
            })

    # ── Ohne Add-on (Hotel-Ebene, Standard-Output) ───────────────────────────
    else:
        price = item.get("price") or item.get("priceFrom") or item.get("minPrice")
        if price is None:
            return []

        mealplan     = (item.get("mealplan") or item.get("boardType") or "RO").upper()
        persons      = int(item.get("adults") or item.get("persons") or 2)
        cancellation = "free" if item.get("freeCancellation") else "non-refundable"
        rooms_left   = int(item.get("roomsLeft") or item.get("availability") or 1)

        avail_key = f"{hotel_id}_{check_in}_default_{mealplan}_{persons}_{cancellation}"

        records.append({
            "availability_key": avail_key,
            "hotel_id":    hotel_id,
            "room_id":     "default",
            "date":        check_in,
            "price_eur":   float(price),
            "rooms_left":  rooms_left,
            "available":   rooms_left > 0,
            "room_name":   item.get("roomType") or "Standardzimmer",
            "mealplan":    mealplan,
            "persons":     persons,
            "cancellation": cancellation,
            "source":      "apify-weekly",
        })

    return records


# ── Supabase Upsert ───────────────────────────────────────────────────────────

def upsert(records: list[dict]) -> None:
    """Records in 500er-Batches an den Supabase Endpoint schicken."""
    total = len(records)
    sent  = 0

    for i in range(0, total, SUPABASE_BATCH_SIZE):
        batch = records[i : i + SUPABASE_BATCH_SIZE]

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = httpx.post(
                    SUPABASE_ENDPOINT,
                    headers={
                        "Content-Type": "application/json",
                        "make-api-key": MAKE_API_KEY,
                    },
                    json={"records": batch},
                    timeout=60,
                )
                resp.raise_for_status()
                sent += len(batch)
                log.info("  Upsert %d/%d ✓", sent, total)
                break

            except httpx.HTTPStatusError as e:
                log.warning(
                    "  HTTP %s | Versuch %d/%d | %s",
                    e.response.status_code, attempt, MAX_RETRIES,
                    e.response.text[:300],
                )
            except httpx.RequestError as e:
                log.warning("  Netzwerk-Fehler | Versuch %d/%d | %s", attempt, MAX_RETRIES, e)

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
            else:
                log.error("  Batch %d–%d dauerhaft fehlgeschlagen – Abbruch", i + 1, i + len(batch))
                sys.exit(1)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=== Winery Vacation Scraper gestartet ===")
    log.info("%d Hotels | %d Check-in-Daten", len(HOTEL_URLS), len(CHECK_IN_DATES))

    all_records: list[dict] = []
    skipped = 0

    for check_in in CHECK_IN_DATES:
        for i in range(0, len(HOTEL_URLS), APIFY_BATCH_SIZE):
            batch = HOTEL_URLS[i : i + APIFY_BATCH_SIZE]
            try:
                items = run_apify(batch, check_in)
            except Exception as e:
                log.error("Apify-Fehler | check_in=%s | Batch %d | %s", check_in, i, e)
                continue

            for item in items:
                mapped = map_item(item)
                if mapped:
                    all_records.extend(mapped)
                else:
                    skipped += 1

        time.sleep(3)  # Rate Limiting zwischen Dates

    log.info(
        "Scraping abgeschlossen: %d Records | %d übersprungen",
        len(all_records), skipped
    )

    if not all_records:
        log.warning("Keine Records – Job beendet.")
        return

    log.info("Starte Upsert nach Supabase …")
    upsert(all_records)
    log.info("=== Fertig. %d Records in Supabase. ===", len(all_records))


if __name__ == "__main__":
    main()

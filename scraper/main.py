"""
Winery Vacation – Hotel Availability Scraper
voyager/booking-scraper → Supabase availabilities

Standard-Modus: scrapet die nächsten 90 Tage (für wöchentliche Runs)
Initial-Modus:  scrapet die nächsten 180 Tage (einmalig, beim ersten Setup)

Steuerung über Umgebungsvariable DAYS_AHEAD (default 90).
"""

import os
import sys
import time
import logging
from datetime import date, datetime, timedelta

import httpx
from apify_client import ApifyClient

# ── Config ───────────────────────────────────────────────────────────────────

APIFY_TOKEN  = os.environ["APIFY_TOKEN"]
MAKE_API_KEY = os.environ["MAKE_API_KEY"]
SUPABASE_ENDPOINT = (
    "https://pfvupcmrxrnkjyqlopyz.supabase.co"
    "/functions/v1/bulk-import-availabilities"
)

# Wie viele Tage in die Zukunft scrapen?
DAYS_AHEAD = int(os.environ.get("DAYS_AHEAD", "90"))

# Welche Daten genau? Jeder Tag der nächsten N Tage (eine Nacht pro Slot)
START_DATE = date.today() + timedelta(days=1)  # ab morgen
CHECK_IN_DATES = [
    (START_DATE + timedelta(days=d)).isoformat()
    for d in range(DAYS_AHEAD)
]

APIFY_BATCH_SIZE    = 20    # Hotels pro Apify-Run
SUPABASE_BATCH_SIZE = 500   # Hard Limit des Endpoints
MAX_RETRIES         = 3
RETRY_DELAY         = 10    # Sekunden (wird multipliziert pro Versuch)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Hotels laden ──────────────────────────────────────────────────────────────

from hotels import HOTEL_URLS

# ── Mealplan-Erkennung ────────────────────────────────────────────────────────

def detect_mealplan(your_choices: list) -> str:
    """
    Aus dem yourChoices-Feld die Verpflegung erkennen.
    Mapping auf Supabase-Werte: room_only, breakfast, half_board, full_board, all_inclusive
    """
    text = " ".join(str(c).lower() for c in (your_choices or []))

    if "all inclusive" in text or "all-inclusive" in text:
        return "all_inclusive"
    if "vollpension" in text or "full board" in text:
        return "full_board"
    if "halbpension" in text or "half board" in text or "abendessen" in text:
        return "half_board"
    if "frühstück" in text or "breakfast" in text:
        return "breakfast"
    return "room_only"


# ── Cancellation-Erkennung ────────────────────────────────────────────────────

def detect_cancellation(option: dict) -> str:
    """
    Aus optionsArray.cancellationType bzw. freeCancellation Flag.
    """
    if option.get("freeCancellation") is True:
        return "free_cancellation"
    cancel_type = option.get("cancellationType")
    if cancel_type:
        return str(cancel_type)
    return "non_refundable"


# ── Apify ─────────────────────────────────────────────────────────────────────

def run_apify(hotel_urls: list[str], check_in: str) -> list[dict]:
    """Einen Apify-Run für eine URL-Batch + Check-in-Datum anstoßen."""
    check_out = (date.fromisoformat(check_in) + timedelta(days=1)).isoformat()
    client    = ApifyClient(APIFY_TOKEN)

    run_input = {
        "startUrls": [{"url": url, "method": "GET"} for url in hotel_urls],
        "checkIn":   check_in,
        "checkOut":  check_out,
        "currency":  "EUR",
        "language":  "de",
        "rooms":     1,
        "adults":    2,
        "flexWindow": "1",
        "maxItems":  len(hotel_urls) * 2,  # Puffer
        "propertyType": "none",
        "sortBy": "bayesian_review_score",
        "starsCountFilter": "any",
    }

    log.info("Apify | %d URLs | check_in=%s", len(hotel_urls), check_in)

    try:
        run   = client.actor("voyager/booking-scraper").call(run_input=run_input)
        items = list(client.dataset(run["defaultDatasetId"]).iterate_items())
        log.info("  → %d Hotels", len(items))
        return items
    except Exception as e:
        log.error("  Apify-Fehler: %s", e)
        return []


# ── Mapping: Apify → Supabase ────────────────────────────────────────────────

def map_hotel(hotel_item: dict) -> list[dict]:
    """
    Ein Hotel-Item aus Apify → Liste von Supabase-Records.
    Pro Zimmer × pro Option = 1 Record.
    """
    records = []

    # Hotel-Stammdaten
    hotel_id   = str(hotel_item.get("hotelId") or "").strip()
    hotel_name = hotel_item.get("name") or ""
    check_in   = hotel_item.get("checkInDate")  # ISO-Datum vom Apify-Actor

    # Validierung: hotel_id und Datum müssen vorhanden sein
    if not hotel_id:
        log.debug("Hotel ohne hotelId – übersprungen: %s", hotel_name)
        return []

    if not check_in or not _is_valid_date(check_in):
        log.debug("Hotel %s ohne valides checkInDate (%s) – übersprungen", hotel_id, check_in)
        return []

    rooms = hotel_item.get("rooms") or []

    # Fall 1: Hotel hat Zimmer mit Optionen → ein Record pro Zimmer × Option
    if rooms:
        for room in rooms:
            room_id    = str(room.get("id") or "").strip()
            room_name  = room.get("roomType") or "Zimmer"
            room_avail = bool(room.get("available", False))
            rooms_left = int(room.get("roomsLeft") or 0)
            max_persons = int(room.get("persons") or 2)

            if not room_id or room_id == "0":
                continue  # ungültige Room-ID überspringen

            options = room.get("options") or []

            # Wenn keine Optionen, aber Zimmer existiert: einen Record für "nicht verfügbar"
            if not options:
                avail_key = f"{hotel_id}_{check_in}_{room_id}_room_only_{max_persons}_unknown"
                records.append({
                    "availability_key": avail_key,
                    "hotel_id":    hotel_id,
                    "hotel_name":  hotel_name,
                    "room_id":     room_id,
                    "room_name":   room_name,
                    "date":        check_in,
                    "price_eur":   0,
                    "rooms_left":  0,
                    "available":   False,
                    "mealplan":    "room_only",
                    "max_persons": max_persons,
                    "persons":     max_persons,
                    "cancellation": "unknown",
                    "source":      "github-actions",
                })
                continue

            # Pro Option ein Record
            for opt in options:
                price = opt.get("price")
                if price is None:
                    continue
                try:
                    price_eur = float(price)
                except (TypeError, ValueError):
                    continue
                if not (0 < price_eur < 100000):
                    continue  # außerhalb des Supabase-Range

                opt_persons = int(opt.get("persons") or max_persons)
                mealplan    = detect_mealplan(opt.get("yourChoices"))
                cancellation = detect_cancellation(opt)

                avail_key = (
                    f"{hotel_id}_{check_in}_{room_id}_"
                    f"{mealplan}_{opt_persons}_{cancellation}"
                )

                records.append({
                    "availability_key": avail_key,
                    "hotel_id":    hotel_id,
                    "hotel_name":  hotel_name,
                    "room_id":     room_id,
                    "room_name":   room_name,
                    "date":        check_in,
                    "price_eur":   round(price_eur, 2),
                    "rooms_left":  rooms_left,
                    "available":   room_avail,
                    "mealplan":    mealplan,
                    "max_persons": max_persons,
                    "persons":     opt_persons,
                    "cancellation": cancellation,
                    "source":      "github-actions",
                })

    # Fall 2: Hotel hat KEINE Zimmer-Daten → ein Record als "nicht verfügbar"
    else:
        avail_key = f"{hotel_id}_{check_in}_no_rooms_room_only_2_unknown"
        records.append({
            "availability_key": avail_key,
            "hotel_id":    hotel_id,
            "hotel_name":  hotel_name,
            "room_id":     "no_rooms",
            "room_name":   "Keine Verfügbarkeit",
            "date":        check_in,
            "price_eur":   0,
            "rooms_left":  0,
            "available":   False,
            "mealplan":    "room_only",
            "max_persons": 2,
            "persons":     2,
            "cancellation": "unknown",
            "source":      "github-actions",
        })

    return records


def _is_valid_date(date_str) -> bool:
    """Prüft ob ein String ein valides ISO-Datum (YYYY-MM-DD) ist."""
    if not date_str or not isinstance(date_str, str):
        return False
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except (ValueError, TypeError):
        return False


# ── Supabase Upsert ───────────────────────────────────────────────────────────

def upsert(records: list[dict]) -> int:
    """Records in 500er-Batches an den Supabase Endpoint schicken. Gibt Anzahl gesendeter Records zurück."""
    total = len(records)
    sent  = 0

    for i in range(0, total, SUPABASE_BATCH_SIZE):
        batch = records[i : i + SUPABASE_BATCH_SIZE]
        success = False

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
                success = True
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

        if not success:
            log.error("  Batch %d–%d dauerhaft fehlgeschlagen – fahre mit nächstem Batch fort", i + 1, i + len(batch))

    return sent


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("=== Winery Vacation Scraper gestartet ===")
    log.info("Modus: %d Tage voraus | %d Hotels", DAYS_AHEAD, len(HOTEL_URLS))
    log.info("Zeitraum: %s bis %s", CHECK_IN_DATES[0], CHECK_IN_DATES[-1])
    log.info("Geschätzte Apify-Calls: %d", len(HOTEL_URLS) * len(CHECK_IN_DATES))

    grand_total_sent = 0
    grand_total_skipped = 0

    # Wir verarbeiten Tag für Tag und upserten direkt nach jedem Tag
    # Das vermeidet Speicher-Probleme bei großen Datenmengen
    for date_idx, check_in in enumerate(CHECK_IN_DATES, start=1):
        log.info("[%d/%d] Datum: %s", date_idx, len(CHECK_IN_DATES), check_in)

        day_records = []

        for batch_idx in range(0, len(HOTEL_URLS), APIFY_BATCH_SIZE):
            batch = HOTEL_URLS[batch_idx : batch_idx + APIFY_BATCH_SIZE]
            items = run_apify(batch, check_in)

            for item in items:
                mapped = map_hotel(item)
                if mapped:
                    day_records.extend(mapped)
                else:
                    grand_total_skipped += 1

        if day_records:
            log.info("Upsert für %s: %d Records", check_in, len(day_records))
            sent = upsert(day_records)
            grand_total_sent += sent
        else:
            log.warning("Keine Records für %s", check_in)

        # Kurze Pause zwischen Tagen
        time.sleep(2)

    log.info("=== FERTIG ===")
    log.info("Gesendet: %d Records | Übersprungen: %d", grand_total_sent, grand_total_skipped)


if __name__ == "__main__":
    main()

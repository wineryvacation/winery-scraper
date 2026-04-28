"""
Deine 120 Booking.com Hotel-URLs.

Format: Vollständige URL der Hotel-Detailseite.
Check-in/out-Parameter werden vom Script gesetzt – die hier stehende URL
braucht keine Datumsparameter.

Beispiel-URL:
  https://www.booking.com/hotel/de/weingut-am-nil.html
  https://www.booking.com/hotel/at/loisium-wine-spa-resort-langenlois.html
  https://www.booking.com/hotel/it/villa-sparina-resort.html
"""

HOTEL_URLS: list[str] = [
    # ── Deutschland ──────────────────────────────────────────────────────────
    # Mosel
    "https://www.booking.com/hotel/de/beispiel-weingut-mosel.html",
    # Rheingau
    "https://www.booking.com/hotel/de/beispiel-weinhotel-rheingau.html",
    # Pfalz
    # ...

    # ── Österreich ────────────────────────────────────────────────────────────
    # Wachau
    "https://www.booking.com/hotel/at/beispiel-wachau.html",
    # ...

    # ── Italien ───────────────────────────────────────────────────────────────
    # Toskana
    "https://www.booking.com/hotel/it/beispiel-toskana.html",
    # ...

    # HIER DEINE 120 URLS EINTRAGEN
    # Tipp: Du kannst sie aus deiner bestehenden Make.com-Konfiguration
    # oder aus Supabase exportieren: SELECT DISTINCT hotel_url FROM hotels;
]

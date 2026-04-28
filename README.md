# Winery Vacation – Availability Scraper

Automatischer wöchentlicher Scraper: Booking.com → Supabase.
Stack: Python · Apify (voyager/booking-scraper) · GitHub Actions

---

## Setup (einmalig, ~15 Minuten)

### 1. Hotel-URLs eintragen

`scraper/hotels.py` öffnen und deine 120 Booking.com URLs eintragen.

Schnellster Weg: URLs direkt aus Supabase holen, falls du sie dort bereits hast:
```sql
SELECT hotel_url FROM hotels ORDER BY name;
```

### 2. GitHub Repository

Falls noch kein Repo existiert:
```bash
git init
git remote add origin https://github.com/DEIN_USER/winery-vacation-scraper.git
```

Dateien committen:
```bash
git add scraper/ .github/
git commit -m "feat: add weekly availability scraper"
git push -u origin main
```

### 3. GitHub Secrets setzen

Repository → Settings → Secrets and variables → Actions → New repository secret

| Secret | Wo finden |
|--------|-----------|
| `APIFY_TOKEN` | apify.com → Settings → Integrations → API tokens |
| `MAKE_API_KEY` | Du kennst ihn aus deinem Make.com Setup |

### 4. Ersten Test-Run starten

GitHub → Actions → "Weekly Hotel Availability Scrape" → Run workflow

Logs live in der Actions-Ansicht. Wenn alles grün: Make.com-Workflow deaktivieren.

---

## Betrieb

- Läuft automatisch jeden Montag 03:00 UTC (05:00 CEST)
- Logs: GitHub Actions → letzter Run
- Fehler: GitHub schickt automatisch eine E-Mail bei fehlgeschlagenem Run

## Zimmer-Details aktivieren (optional)

Wenn du Zimmertypen, Belegung und Ausstattung pro Zimmer brauchst:

1. In Apify das "Room Offerings"-Add-on für voyager/booking-scraper aktivieren
2. In `scraper/main.py` diese Zeile einkommentieren:
   ```python
   # "scrapeRoomOfferings": True,
   ```

Das Add-on kostet extra pro Hotel – erst testen, dann auf alle 120 ausrollen.

## Kosten (Richtwerte)

| Komponente | Kosten |
|------------|--------|
| GitHub Actions | Kostenlos (öffentliche Repos, 2.000 Min/Monat für private) |
| Apify voyager/booking-scraper | Pay-per-result, ~$3 pro 1.000 Ergebnisse |
| Supabase | Bestehender Plan |

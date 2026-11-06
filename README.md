# Deal Finder MVP

A single-file automated deal-finding pipeline: pulls deals from a built-in
**14-source registry** (RSS feeds + lightweight HTML/JSON/Playwright
scrapes), normalizes them into one `Deal` shape, stores everything in
SQLite, scores the deals, deduplicates across sources (rapidfuzz), and
renders the good ones to **static HTML pages** (the primary output) —
regular deals on `deals.html`, coupon-code deals on `coupons.html`.
Telegram is an optional side-channel for the best-of-the-best deals.

## Pipeline

```
14-source registry (RSS + scrapes) ─┐
Extra RSS_FEEDS (optional)         ─┤
Affiliate feed (fallback only)     ─┘
        │
        ▼
normalize_deal() ─> enrich_deal() ─> store_deal() ─> score_deal()
   (Deal)            (URL resolve +   (SQLite:        (all-time low
                      real/mock         deals +         + discount)
                      prices)           price_history)        │
                                                            ▼
                                  deduplicate_deals() (rapidfuzz >= 85 + price within 5%)
                                                            │
                                     ┌──────────────────────┴──────────────────────┐
                                     ▼                                             ▼
                          no promo code -> deals.html                  promo code -> coupons.html
                                     └─────────> optional: send_telegram() for score >= 40
```

## Sources (built-in registry)

| Source | Kind | Notes |
|---|---|---|
| Slickdeals | RSS | frontpage Amazon search feed |
| eDealInfo | RSS | behind Cloudflare — often 403s; kept as an honest attempt |
| MusiciansFriend | Playwright | Stupid Deal of the Day (Akamai may block headless) |
| BensBargains | RSS | |
| DealNews | RSS | `?rss=1&sort=hotness` |
| DealsOfAmerica | RSS | small feed (a few entries) |
| GottaDeal | HTML scrape | `/deal/` anchors |
| DealCatcher | RSS | |
| DealsDigger | HTML scrape | legacy Amazon `/exec/obidos/ASIN/` links |
| 1Sale | JSON scrape | Next.js flight-data payload; carries promo codes |
| iHotOffers | Playwright | `/offer/` cards |
| ShopSale | Playwright | `/products/` cards |
| TechBargains | RSS | |
| MyVipon | Playwright | deals behind login — often 0 items |

Each source gets a 15 s budget (`SOURCE_TIMEOUT`); a dead or slow source
logs a WARNING and the run continues. Add extra RSS feeds via
`RSS_FEEDS` in `.env` (registry URLs are ignored there to avoid
double-fetching). A source failing is normal — sites move feeds and add
bot walls; the WARNINGs in the log are your diagnostic.

**Scoring rules**

- A deal *qualifies* if: discount >= 20% **OR** current price is the
  **all-time low** (local `price_history` + backfilled 52-week history).
- `score = discount_percent + (10 if all-time low else 0)`
- The HTML pages always render **all** currently-qualifying deals
  (score >= 15) — they are your browsable catalog and are not filtered by
  `last_alert_price`. That column only gates the optional Telegram
  side-channel (and any future "NEW since last run" logic).
- After scoring, cross-source duplicates collapse: normalized-title
  match >= 85 (rapidfuzz `token_set_ratio`) **and** prices within 5%
  keep one deal per cluster — preferring Amazon-linked rows (deepest
  price history), then the highest score.

## Project layout

```
deal-finder/
├── deal_finder.py             # the entire pipeline (MVP: one file)
├── deploy.py                  # publish index.html + coupons.html to GitHub Pages (gh-pages branch)
├── templates/deals.html       # Jinja2 template: the deal catalog page
├── templates/coupons.html     # Jinja2 template: the coupon-codes page
├── .github/workflows/update-deals.yml  # GitHub Actions: cloud updates every 30 min
├── Push-To-GitHub.bat          # one-click source push to GitHub (main branch)
├── Create-Shortcut.ps1         # generates the desktop shortcut for the bat file
├── affiliate_feed_sample.json # mock affiliate-network product feed
├── requirements.txt
├── .env.example               # copy to .env and fill in
└── README.md
```

Generated at runtime: `deals.db` (SQLite), `deals.html` /
`deals-preview.html` (catalog), `coupons.html` / `coupons-preview.html`
(coupon codes).

## Setup

```bash
cd deal-finder
python -m venv .venv

# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### Playwright browser (only needed for scraping)

```bash
playwright install chromium
```

Skip this if you only use RSS + the affiliate feed — the script detects
whether Playwright is importable and otherwise skips the scraper.

### Telegram (optional)

1. Message [@BotFather](https://t.me/BotFather) → `/newbot` → copy the
   **bot token** (looks like `123456789:AAF...`).
2. Create a channel and add your bot as an **admin**.
3. For public channels the chat ID is the `@username`; for private ones,
   forward a channel message to [@userinfobot](https://t.me/userinfobot)
   and use the numeric ID prefixed with `-100`.
4. Uncomment `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `.env`.

Telegram is **optional** — without it you'll see a single INFO line that
it was skipped, and the HTML page remains the full-fidelity output. When
enabled, it only receives deals with **score >= 40** (best-of-the-best),
deduplicated by `last_alert_price`.

### Configure

```bash
copy .env.example .env   # macOS/Linux: cp .env.example .env
```

Edit `.env`: set your feed URLs etc. `SCRAPE_SAMPLE_URL` is optional —
leave it blank to disable scraping.

## Running

```bash
# 1) Preview: writes deals-preview.html, never touches last_alert_price
python deal_finder.py --dry-run

# 2) Preview and pop it open in your default browser
python deal_finder.py --dry-run --once --open

# 3) Single live pass (cron-friendly): writes deals.html + updates the DB
python deal_finder.py --once

# 4) Daemon mode: loop forever, default every 30 min (--interval to override)
python deal_finder.py

# 5) Real Amazon prices: scrape live product pages with Playwright
#    (up to MAX_SCRAPE deals per run, default 10; falls back to mock
#    prices with a WARNING when Amazon blocks or a price is missing)
python deal_finder.py --dry-run --once --scrape
```

**Note on `--scrape` and URL resolution:** deal-site links (Slickdeals
feeds) are interstitial pages, so `enrich_deal()` resolves them first: a
raw Amazon product href in the landing page HTML rewrites `deal.url`
directly (always on — even without `--scrape`); otherwise, with
`--scrape`, the deal's `slickdeals.net/click?...` hop is followed in a
real browser until it lands on the Amazon product page, and the real
current/list prices are scraped in the same visit. Non-Amazon targets
(e.g. Woot) are detected and left untouched. Resolved CTAs get the
affiliate tag automatically at render time. Real PA-API access, once
approved, replaces the scraper without changing the call site
(`enrich_deal()`).

## Viewing the output

The HTML page is a fully self-contained static file — just open it:

- **Directly:** double-click `deals.html` (or `deals-preview.html`), or
  `python deal_finder.py --dry-run --once --open`.
- **Local server (nicer):** `python -m http.server 8000` from the project
  directory, then visit `http://localhost:8000/deals.html`. The page
  auto-reloads itself every 30 minutes via `<meta http-equiv="refresh">`.

## Deploying to GitHub Pages

`deploy.py` publishes the latest pages to a free live URL you can
open on a phone or share. It copies `deals.html` to `index.html`
(`coupons.html` rides along under its own name), commits both to a
`gh-pages` branch, and force-pushes that branch to GitHub
(one-time setup below; after that it's a single command). The deployed
site gets a small **Home / Coupon Codes** nav between the two pages.

1. Create a new **public** repository on GitHub:
   go to <https://github.com/new> (no README / .gitignore / license needed).
2. Connect this folder to it:
   ```bash
   git remote add origin https://github.com/<your-username>/<your-repo>.git
   ```
3. Deploy:
   ```bash
   python deploy.py            # add --fetch to refresh the deals first
   ```
4. In the GitHub repo: **Settings -> Pages**, set the branch to
   **`gh-pages`** and save.

The site is then live at `https://<your-username>.github.io/<your-repo>/`
(the first build takes a minute or two). The page already contains a
`<meta http-equiv="refresh" content="1800">` tag, so an open phone browser
picks up each new deploy within 30 minutes.

`deploy.py` flags: `--fetch` runs `python deal_finder.py --once` before
publishing; `--no-push` does everything except the push (local test).
If the push fails (e.g. no remote configured), the script prints a
step-by-step setup guide.

### Other hosting options

- **Netlify Drop** — drag-and-drop `deals.html` onto
  <https://app.netlify.com/drop> for an instant hosted URL.
- **Flask later** — wrap `render_html()` in a tiny Flask route to render
  on demand from the DB instead of writing a file (the Jinja2 template is
  reused as-is).

## Automated Cloud Updates (GitHub Actions)

The live site is hosted at: **https://jonybgoode.github.io/my-deal-site/**

`.github/workflows/update-deals.yml` turns the site into a zero-touch
pipeline: pushing the workflow file to the `main` branch automatically
updates the site every 30 minutes — completely independent of your local
computer. The workflow checks out the repo on GitHub's runners, installs
Python 3.10 + Playwright Chromium, runs `python deal_finder.py --once
--scrape`, and publishes the rendered `index.html` + `coupons.html` to the
`gh-pages` branch via `peaceiris/actions-gh-pages` (only the final HTML is
pushed — the Jinja templates are fully rendered into those files). A cached
`deals.db` carries NEW badges and price history across runs.

You can manually trigger a run by going to the "Actions" tab in the GitHub
repository, selecting "Auto-Update Deals", and clicking "Run workflow".

GitHub Pages settings: "Ensure your repository Settings -> Pages -> Build
and deployment -> Source is set to 'Deploy from a branch', and the Branch
is set to `gh-pages` / `/ (root)`."

Notes:

- GitHub's cron scheduler can delay or occasionally skip a run during
  periods of high load; the `workflow_dispatch` trigger is your friend.
- Amazon scraping may hit more captchas from GitHub's datacenter IPs —
  blocked scrapes log a WARNING and fall back to feed prices (by design).
- With the workflow active, local `deploy.py` / cron runs are optional
  (the Actions concurrency guard prevents overlapping publishes).

## Desktop Shortcut

`Push-To-GitHub.bat` is a one-click way to push source updates to GitHub
(`main` branch): it adds the `origin` remote if missing, creates/switches
to the `main` branch, stages + commits the source files, and force-pushes.
Safe to re-run — it only pushes what changed.

`Create-Shortcut.ps1` generates a **Deal Finder Push** desktop icon that
runs the batch file (pin it to the taskbar from there). Create it with:

```powershell
powershell -ExecutionPolicy Bypass -File .\Create-Shortcut.ps1
```

With the GitHub Actions workflow active, this shortcut is only needed for
source-code changes — content updates happen in the cloud automatically.

## Cron / Task Scheduler

Use `--once` so each invocation is a single pass. To refresh the deals
**and** republish every 30 minutes on Linux/Mac, add this crontab entry
(`crontab -e`):

```cron
*/30 * * * * cd /path/to/deal-finder && python deal_finder.py --once && python deploy.py
```

A variant with logging and an explicit venv python:

```cron
*/30 * * * * cd /path/to/deal-finder && .venv/bin/python deal_finder.py --once && .venv/bin/python deploy.py >> deal_finder.log 2>&1
```

Windows Task Scheduler: program `...\.venv\Scripts\python.exe`,
arguments `deal_finder.py --once`, start in `...\deal-finder`, trigger
every 30 minutes (add a second task for `deploy.py`, or wrap both in a
`deploy.bat`).

## Scraping notes

The scraper (`scrape_sample` in `deal_finder.py`) demonstrates the full
pattern with Playwright's **async** API: navigate, read the price from the
DOM (with a regex fallback over the page body), click a **"Reveal Code"**
button like a real user, then grab the promo code from the DOM or sniff it
from JSON network responses.

To target a real site, tune these constants at the top of `deal_finder.py`:
`PRICE_SELECTORS`, `REVEAL_BUTTON_SELECTORS`, `CODE_SELECTORS`.

Some sites run bot detection (Cloudflare, PerimeterX, Akamai). Production
scraping usually requires stealth plugins, residential proxies, realistic
fingerprints, and human-like delays — and you must respect each site's
Terms of Service and robots.txt.

## Extending later

- **More sources:** add a `SourceSpec("name", "rss" | "html" | "json" |
  "playwright", url, href_pattern=...)` row to the `SOURCES` registry at
  the top of `deal_finder.py` — raw dicts need `title`, `url`,
  `merchant`, `price`, `original_price`, `promo_code` (+ optional
  `image_url` / `entry`).
- **Real affiliate APIs:** replace the body of `fetch_affiliate()` — see
  the docstring there for Awin/CJ/Impact pointers — and implement
  `generate_affiliate_link()`.
- **Migrating from SQLite:** schema is plain `CREATE TABLE IF NOT EXISTS`
  statements in `init_db()`; easy to port to Postgres later.

## Troubleshooting

- **"RSS feed ... returned 0 entries"** — the feed URL is dead, redirects
  to an HTML page, or serves malformed XML. Open the URL in a browser and
  look for an XML icon/feed link; sites often move their feed.
- **No qualifying deals:** remember a deal's first sighting is trivially
  its own 30-day low, and `score >= 15` filters those out until a real
  price drop appears in `price_history`.
- **Telegram never sends:** it's opt-in now (see above) and only carries
  score >= 40 deals; also confirm the bot is a channel admin.
- **Playwright missing:** install it (`pip install playwright`) or leave
  `SCRAPE_SAMPLE_URL` blank — the rest of the pipeline works without it.
- **Garbled emoji in console output:** run `chcp 65001` or use Windows
  Terminal; the HTML page itself is always written as UTF-8.

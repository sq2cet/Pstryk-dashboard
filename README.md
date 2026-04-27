# Pstryk Dashboard

A self-hosted energy dashboard for households on the Polish dynamic-pricing
**[Pstryk](https://pstryk.pl)** tariff with a **[BleBox](https://blebox.eu)**
energy meter on the LAN. It correlates live consumption with the hourly
spot price, shows today / this-month / this-year totals against Pstryk's
authoritative meter records, and surfaces the cheapest upcoming hours
from Pstryk's price forecast.

![status](https://github.com/sq2cet/Pstryk-dashboard/actions/workflows/ci.yml/badge.svg)

## What you get

- **Live tile** — current power (W), current price (PLN/kWh), today /
  this month / this year usage and cost.
- **Live power chart** — last 60 minutes, 5-second resolution, total +
  per-phase L1/L2/L3 (auto-scrolls left).
- **Cheapest / most-expensive hours** — top of today + tomorrow as
  soon as Pstryk publishes the next-day prices.
- **Range explorer** — last 24 h, today, last 7 days, this/last month,
  this/last year, or custom dates × hour / day / month / year resolution.
  Three views per range: price + consumption combo chart, cost +
  consumption chart, per-bucket aggregates table.
- **Per-phase diagnostics** — voltage, current, frequency, power factor,
  apparent / reactive power, plus all four energy counters (forward /
  reverse active in kWh, forward / reverse reactive in varh, apparent
  in VAh) — Total + L1 + L2 + L3 columns.

## How it works

```
        ┌─────────────────────┐    every 60 min     ┌──────────────────┐
        │ api.pstryk.pl       │ ──────────────────▶ │ scheduler        │
        │ unified-metrics     │  (last 7 d + next   │  pstryk_poll     │
        │ (price + meter +    │   24 h, wide window │                  │
        │  cost)              │   so late-arriving  │  + on first run, │
        └─────────────────────┘   data lands here)  │  full backfill   │
                                                    │  walks 7-day     │
                                                    │  chunks back up  │
                                                    │  to 5 years.     │
                                                    └────────┬─────────┘
                                                             ▼
        ┌─────────────────────┐    every 5 s        ┌──────────────────┐
        │ BleBox MultiSensor  │ ◀─────────────────  │ FastAPI app      │
        │ (LAN, no auth)      │                     │                  │
        │ /state              │                     │  SQLite DB       │
        └─────────────────────┘                     │  Jinja + HTMX    │
                                                    │  Chart.js        │
                                                    └──────────────────┘
```

The first time the app runs against your Pstryk key, the backfill job
downloads every hour Pstryk has on file (in 7-day chunks, idempotent)
and stores it locally so you never query Pstryk on page render.

## Requirements

- Python **3.12+**, or any host with Docker
- A **Pstryk API key** — generate one in the Pstryk mobile app under
  *Account → Devices & integrations*
- A **BleBox** energy meter reachable on your local network (the app
  identifies the device automatically)

## Run locally

```bash
git clone https://github.com/sq2cet/Pstryk-dashboard.git
cd Pstryk-dashboard

# 1. Create .env from the template and put a fresh Fernet key in it
cp .env.example .env
python3 -c "from cryptography.fernet import Fernet; print('FERNET_KEY=' + Fernet.generate_key().decode())"
$EDITOR .env   # paste the line above as FERNET_KEY=...

# 2. Install + run (uv path)
uv sync
uv run uvicorn app.main:app --reload

# or pip path:
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/uvicorn app.main:app --reload
```

Open <http://127.0.0.1:8000>. On first launch the app redirects you to
`/settings`. Paste your Pstryk API key and your BleBox host (or IP),
save, and the dashboard takes over. Backfill kicks off ~10 seconds
later; you'll see prices and meter values populate over the next minute
or two as 7-day chunks land.

## Run with Docker

The published image is multi-arch (`linux/amd64` + `linux/arm64`) so
the same tag works on a laptop, an x86 server, or an ARM SBC like a
Raspberry Pi 5 / Orange Pi 5.

```bash
mkdir -p ~/apps/pstryk-dashboard && cd ~/apps/pstryk-dashboard

# Grab the compose file and env template at the version you want
TAG=v1.0.0
curl -O https://raw.githubusercontent.com/sq2cet/Pstryk-dashboard/${TAG}/docker-compose.yml
curl -O https://raw.githubusercontent.com/sq2cet/Pstryk-dashboard/${TAG}/.env.example
mv .env.example .env
$EDITOR .env   # set FERNET_KEY (and HOST_PORT if 8765 is taken)

docker compose --project-name pstryk-dashboard pull
docker compose --project-name pstryk-dashboard up -d
```

Open `http://<host-ip>:8765`. Same first-run flow — the redirect to
`/settings` walks you through the API key and BleBox host.

The compose project name (`pstryk-dashboard`) and dedicated network
mean it stays cleanly scoped on a host that already runs other Docker
apps. The data directory is bind-mounted at `./data` next to your
`.env` so SQLite survives upgrades and restarts.

## Settings

Two layers:

- **`.env`** holds host-level config: `FERNET_KEY` (required), DB path,
  timezone, host port. Never commit this file.
- **`/settings` page** holds runtime config: Pstryk API key, BleBox
  host/port, polling intervals, timezone. The Pstryk key is encrypted
  at rest with the Fernet key from `.env`; it is never written to a
  log or rendered back into the page.

## Updating

```bash
docker compose --project-name pstryk-dashboard pull
docker compose --project-name pstryk-dashboard up -d
```

Schema migrations are applied automatically at startup (`init_db()`
runs SQLite ALTER TABLE for any added columns).

## Troubleshooting

- **`SSL: CERTIFICATE_VERIFY_FAILED` calling Pstryk** — something on
  your network intercepts TLS. Add its root CA to the venv's `certifi`
  store, or set `SSL_CERT_FILE=/path/to/ca-bundle.pem` in `.env` and
  restart.
- **BleBox unreachable** — the meter and the dashboard host must be on
  the same LAN. Disconnect from any VPN that doesn't route to
  `192.168.x.x` / `10.x.x.x` ranges, then verify with
  `curl http://<blebox-ip>/api/device/state`.
- **`429 Too Many Requests` from Pstryk** — the per-endpoint rate
  limit is roughly 3 requests/hour per key. The client backs off
  automatically; if you keep seeing these, lower
  `pstryk_poll_minutes` in `/settings` or wait an hour.
- **Empty consumption bars for a day in the recent past** — Pstryk
  occasionally publishes meter values with a delay. The hourly poll
  rolls a 7-day window so late arrivals fill in within an hour. To
  force an older refresh, restart the app and the startup backfill
  will re-fetch any chunk that has missing kWh data.

## License

MIT — see [LICENSE](LICENSE).

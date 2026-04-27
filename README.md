# Pstryk Dashboard

Personal energy-monitoring dashboard combining two data sources:

- **[Pstryk](https://pstryk.pl)** — dynamic-pricing electricity supplier (PLN/kWh per hour).
- **[BleBox](https://blebox.eu) energy meter** — real-time power and cumulative kWh from a device on the LAN.

It correlates live consumption with the hourly tariff to show real-time cost, today/this-month spending, historical aggregates by hour/day/month/year, and the cheapest upcoming hours from the Pstryk forecast.

## Status

Scaffolding stage. Plan: see `~/.claude/plans/name-will-be-pstryk-federated-peacock.md` (local).

## Stack

- FastAPI + HTMX + Chart.js
- SQLite (via SQLModel)
- APScheduler for polling
- `cryptography` (Fernet) for at-rest encryption of the Pstryk API key
- Packaged with `uv` / `pyproject.toml`

## Run the POC (Mac)

Requires Python 3.12+ and either `uv` or `pip`.

```bash
# generate a Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# create .env from the template and paste the key as FERNET_KEY
cp .env.example .env
$EDITOR .env

# install + run
uv sync                                     # or: python -m venv .venv && pip install -e ".[dev]"
uv run uvicorn app.main:app --reload        # or: .venv/bin/uvicorn app.main:app --reload
```

Open `http://127.0.0.1:8000/`. The first time you'll be redirected to `/settings` to enter your Pstryk API key and BleBox IP.

## Run via Docker

The released image is multi-arch (`linux/amd64` + `linux/arm64`). On RPi5, OrangePi5, or any Docker host:

```bash
mkdir -p ~/apps/pstryk-dashboard && cd ~/apps/pstryk-dashboard
curl -O https://raw.githubusercontent.com/sq2cet/Pstryk-dashboard/v1.0.0/docker-compose.yml
curl -O https://raw.githubusercontent.com/sq2cet/Pstryk-dashboard/v1.0.0/.env.example
mv .env.example .env  # edit FERNET_KEY etc.
docker compose --project-name pstryk-dashboard pull
docker compose --project-name pstryk-dashboard up -d
```

Then open `http://<host>:${HOST_PORT}/`.

## Settings

All runtime configuration lives in two places:

- `.env` — Fernet key, DB path, timezone, ports. **Never commit.**
- `/settings` page — Pstryk API key, BleBox IP, polling intervals. The Pstryk key is encrypted at rest with the Fernet key from `.env`.

## Troubleshooting

- **`SSL: CERTIFICATE_VERIFY_FAILED` calling Pstryk** — corporate proxy / TLS inspection. Set `SSL_CERT_FILE=/path/to/corporate-ca.pem` in `.env` and restart.
- **BleBox unreachable** — make sure the host is on the same LAN as the meter; corporate VPNs typically can't route to `192.168.x.x`.
- **`429 Too Many Requests` from Pstryk** — the per-endpoint limit is 3 req/hour. Lower the polling cadence or wait an hour.

## License

MIT — see [LICENSE](LICENSE).

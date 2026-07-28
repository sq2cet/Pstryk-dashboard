# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.1.0] — 2026-07-28

### Added

- **Fullscreen live-power chart** — click ⤢ on the dashboard chart to
  open a full-screen view with up to 24 hours of history. Scroll the
  mouse wheel to zoom, drag to pan. The Chart.js built-in legend controls
  series visibility (Total / L1 / L2 / L3). Data refreshes every 5 seconds;
  new points appear at the right edge without resetting the user's position.
- **Per-phase columns in the database** — `MeterReading` now stores
  `l1_power_w`, `l2_power_w`, `l3_power_w`. Migration runs automatically
  at startup via `ALTER TABLE` (idempotent, safe on existing installs).
- **24-hour live-power endpoint** — `/api/charts/live-power` accepts
  `minutes` up to 1440 and blends the 1-minute-resolution SQLite data
  with the in-memory 5-second deque for the most-recent 60 minutes.
- **Error banner retry countdown** — when Pstryk or BleBox polling
  fails, the banner shows a live countdown to the next scheduled retry
  (sourced from APScheduler's `next_run_time`). The banner clears on
  its own once the error resolves.

### Changed

- `chartjs-adapter-date-fns` and `chartjs-plugin-zoom` loaded from CDN
  to support time-scale navigation in the fullscreen chart.

## [1.0.2] — 2026-04-27

### Fixed

- Backfill walk produced sparse coverage that the skip-logic mistook for
  fully hydrated chunks, leaving gaps in historical data.

## [1.0.1] — 2026-04-27

### Fixed

- Daily backfill schedule not firing correctly.
- Custom date range inputs kept stale values when switching back to a
  preset range.

## [1.0.0] — 2026-04-27

### Added

- Yesterday range preset in the explorer filter.
- Error banner on the dashboard for Pstryk / BleBox connectivity issues.
- API-key rotation wipes the stored settings rather than leaving a stale
  encrypted value.

## [0.9.0] — initial release candidates

Early development iterations (rc1–rc3). Core dashboard: live tile,
live power chart, range explorer, price forecast, per-phase diagnostics,
Docker multi-arch image.

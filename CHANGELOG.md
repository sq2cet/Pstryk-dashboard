# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.2.10] — 2026-07-30

### Fixed

- **Sticky Bucket column in per-bucket aggregates table** — the "Bucket"
  column now stays visible while scrolling horizontally, matching the
  behaviour of the "Phase" column in the Diagnostic per-phase table.

## [1.2.0] — 2026-07-30

### Added

- **Pinch-zoom in fullscreen chart** — two-finger pinch gesture zooms the
  time axis on iOS/Android. Manual touch-distance tracking replaces the
  chartjs-plugin-zoom pinch (which used pointer events, unreliable inside
  `<dialog>` on iOS Safari). Minimum visible range is 1 minute.
- **Unit labels above y-axis columns** — "W", "PLN/kWh", "PLN", "kWh" now
  appear as column headers directly above their respective y-axis tick values
  on all three charts. Rendered as a Chart.js `afterDraw` plugin so they
  always appear on top of the legend.

### Changed

- **Live power chart height** — doubled from 220 px to 440 px on desktop
  (320 px on mobile) so more waveform detail is visible without opening
  fullscreen.
- **Fullscreen button** — ⤢ button right-aligned in the chart header via
  `margin-left: auto` on the flex container.
- **Price & consumption legend** — two-line layout on mobile (one dataset per
  line) via `fullSize: false`, matching the visual style of the Cost &
  consumption chart.

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

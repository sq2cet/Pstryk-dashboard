// Three charts on the dashboard:
//   1. Live power (per-phase, last 60 min, 5 s cadence) — auto-refresh
//   2. Combo chart (avg price line + consumption bars) — driven by filter
//   3. Cumulative chart (running cost + running usage, dual axis) —
//      driven by filter
//
// Filter changes refetch /api/charts/range and redraw 2 + 3 plus the
// totals strip and the aggregates table. The live-power chart polls
// /api/charts/live-power on its own 5 s timer.

(() => {
  const form = document.getElementById("filter-form");
  if (!form) return;

  const fRange = form.querySelector("#f-range");
  const fFrom = form.querySelector("#f-from");
  const fTo = form.querySelector("#f-to");
  const fResolution = form.querySelector("#f-resolution");
  const customLabels = form.querySelectorAll(".custom-only");

  let comboChart = null;
  let costChart = null;
  let liveChart = null;

  const NUM = (v, digits = 2) =>
    v == null ? "—" : Number(v).toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: digits });

  function showCustomInputs(show) {
    customLabels.forEach((el) => (el.hidden = !show));
  }

  function bucketLabel(iso, resolution) {
    const d = new Date(iso);
    const opts =
      resolution === "hour"
        ? { hour: "2-digit", minute: "2-digit" }
        : resolution === "day"
          ? { month: "short", day: "2-digit" }
          : resolution === "month"
            ? { year: "numeric", month: "short" }
            : { year: "numeric" };
    return d.toLocaleString([], opts);
  }

  // --- Filter-driven charts ---------------------------------------------
  async function refresh() {
    const params = new URLSearchParams();
    params.set("range", fRange.value);
    if (fRange.value === "custom") {
      if (!fFrom.value || !fTo.value) return;
      params.set("from", fFrom.value);
      params.set("to", fTo.value);
    }
    if (fResolution.value) params.set("resolution", fResolution.value);

    const res = await fetch(`/api/charts/range?${params.toString()}`);
    if (!res.ok) {
      document.getElementById("chart-subtitle").textContent =
        `Could not load (HTTP ${res.status})`;
      return;
    }
    render(await res.json());
  }

  function render(data) {
    const labels = data.buckets.map((b) => bucketLabel(b.bucket_local_iso, data.resolution));
    const prices = data.buckets.map((b) => b.avg_price_pln_per_kwh);
    const kwh = data.buckets.map((b) => b.kwh);

    document.getElementById("chart-title").textContent =
      `Price & consumption · ${data.range} · ${data.resolution}`;
    document.getElementById("chart-subtitle").textContent =
      `${data.totals.bucket_count} bucket(s) · tz ${data.tz}`;
    document.getElementById("table-subtitle").textContent =
      `${data.buckets.length} row(s)`;

    document.getElementById("t-kwh").textContent = `${NUM(data.totals.kwh, 2)} kWh`;
    document.getElementById("t-cost").textContent = `${NUM(data.totals.cost_pln, 2)} PLN`;
    document.getElementById("t-avg").textContent = NUM(data.totals.avg_price_pln_per_kwh, 4);
    document.getElementById("t-min").textContent = NUM(data.totals.min_price_pln_per_kwh, 4);
    document.getElementById("t-max").textContent = NUM(data.totals.max_price_pln_per_kwh, 4);

    const cost = data.buckets.map((b) => b.cost_pln);
    renderCombo(labels, prices, kwh);
    renderCostConsumption(labels, kwh, cost);
    renderTable(data.buckets, data.resolution);
  }

  function renderCombo(labels, prices, kwh) {
    const canvas = document.getElementById("chart-combo");
    if (comboChart) comboChart.destroy();
    comboChart = new Chart(canvas, {
      data: {
        labels,
        datasets: [
          {
            type: "bar",
            label: "Consumption (kWh)",
            data: kwh,
            backgroundColor: "rgba(245, 197, 24, 0.5)",
            borderColor: "rgba(245, 197, 24, 0.9)",
            borderWidth: 1,
            yAxisID: "y_kwh",
          },
          {
            type: "line",
            label: "Avg price (PLN/kWh)",
            data: prices,
            borderColor: "#7ec8ff",
            backgroundColor: "#7ec8ff",
            tension: 0.2,
            pointRadius: 0,
            spanGaps: true,
            yAxisID: "y_price",
          },
        ],
      },
      options: chartCommon({
        scales: {
          y_price: {
            type: "linear",
            position: "left",
            title: { display: true, text: "PLN/kWh" },
            grid: { color: "rgba(255,255,255,0.05)" },
            ticks: { color: "#8a8f98" },
          },
          y_kwh: {
            type: "linear",
            position: "right",
            title: { display: true, text: "kWh" },
            grid: { display: false },
            ticks: { color: "#8a8f98" },
          },
        },
      }),
    });
  }

  function renderCostConsumption(labels, kwh, costPln) {
    const canvas = document.getElementById("chart-cost-consumption");
    if (costChart) costChart.destroy();
    costChart = new Chart(canvas, {
      data: {
        labels,
        datasets: [
          {
            type: "bar",
            label: "Consumption (kWh)",
            data: kwh,
            backgroundColor: "rgba(245, 197, 24, 0.5)",
            borderColor: "rgba(245, 197, 24, 0.9)",
            borderWidth: 1,
            yAxisID: "y_kwh",
          },
          {
            type: "line",
            label: "Cost (PLN)",
            data: costPln,
            borderColor: "#9affb6",
            backgroundColor: "rgba(154, 255, 182, 0.15)",
            fill: true,
            tension: 0.2,
            pointRadius: 0,
            spanGaps: true,
            yAxisID: "y_cost",
          },
        ],
      },
      options: chartCommon({
        scales: {
          y_cost: {
            type: "linear",
            position: "left",
            title: { display: true, text: "PLN" },
            grid: { color: "rgba(255,255,255,0.05)" },
            ticks: { color: "#8a8f98" },
          },
          y_kwh: {
            type: "linear",
            position: "right",
            title: { display: true, text: "kWh" },
            grid: { display: false },
            ticks: { color: "#8a8f98" },
          },
        },
      }),
    });
  }

  function chartCommon({ scales }) {
    return {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      scales: {
        ...scales,
        x: {
          ticks: { color: "#8a8f98", maxTicksLimit: 16 },
          grid: { color: "rgba(255,255,255,0.05)" },
        },
      },
      plugins: { legend: { labels: { color: "#e6e8eb" } } },
    };
  }

  function renderTable(buckets, resolution) {
    const tbody = document.querySelector("#agg-table tbody");
    tbody.innerHTML = "";
    buckets.forEach((b) => {
      const tr = document.createElement("tr");
      if (b.is_now) tr.classList.add("row-now");
      tr.innerHTML = `
        <td>${bucketLabel(b.bucket_local_iso, resolution)}${b.is_now ? ' <span class="pill pill-ok">now</span>' : ""}</td>
        <td class="num">${NUM(b.kwh, 3)}</td>
        <td class="num">${NUM(b.avg_price_pln_per_kwh, 4)}</td>
        <td class="num">${NUM(b.min_price_pln_per_kwh, 4)}</td>
        <td class="num">${NUM(b.max_price_pln_per_kwh, 4)}</td>
        <td class="num">${NUM(b.cost_pln, 2)}</td>
      `;
      tbody.appendChild(tr);
    });
  }

  fRange.addEventListener("change", () => {
    showCustomInputs(fRange.value === "custom");
    if (fRange.value !== "custom") refresh();
  });
  fResolution.addEventListener("change", refresh);
  fFrom.addEventListener("change", refresh);
  fTo.addEventListener("change", refresh);

  // --- Live-power chart -------------------------------------------------
  // Built once, then updated in place every 5 s. Replacing the data and
  // calling update("none") makes the chart appear to slide left as new
  // points arrive at the right and old points drop off the left, instead
  // of animating from zero on each refresh.
  async function refreshLivePower() {
    const res = await fetch("/api/charts/live-power?minutes=60");
    if (!res.ok) return;
    const data = await res.json();

    const labels = data.ts.map((iso) =>
      new Date(iso).toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }),
    );

    if (!liveChart) {
      const canvas = document.getElementById("chart-live-power");
      if (!canvas) return;
      const datasets = [
        { label: "Total", data: data.total_w, borderColor: "#e6e8eb" },
        { label: "L1", data: data.l1_w, borderColor: "#7ec8ff" },
        { label: "L2", data: data.l2_w, borderColor: "#9affb6" },
        { label: "L3", data: data.l3_w, borderColor: "#f5c518" },
      ].map((d) => ({
        ...d,
        type: "line",
        borderWidth: 1.5,
        tension: 0.15,
        pointRadius: 0,
        spanGaps: true,
        backgroundColor: d.borderColor,
      }));
      liveChart = new Chart(canvas, {
        data: { labels, datasets },
        options: {
          ...chartCommon({
            scales: {
              y: {
                type: "linear",
                position: "left",
                title: { display: true, text: "W" },
                grid: { color: "rgba(255,255,255,0.05)" },
                ticks: { color: "#8a8f98" },
              },
            },
          }),
          animation: false,
        },
      });
      return;
    }

    liveChart.data.labels = labels;
    liveChart.data.datasets[0].data = data.total_w;
    liveChart.data.datasets[1].data = data.l1_w;
    liveChart.data.datasets[2].data = data.l2_w;
    liveChart.data.datasets[3].data = data.l3_w;
    liveChart.update("none");
  }

  refresh();
  refreshLivePower();
  setInterval(refreshLivePower, 5000);
})();

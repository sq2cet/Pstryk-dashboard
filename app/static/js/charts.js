// Render the dashboard's combo chart, cumulative-cost chart, totals
// strip, and aggregates table from /api/charts/range. The filter form
// drives all of it: any change refetches and redraws.

(() => {
  const form = document.getElementById("filter-form");
  if (!form) return;

  const fRange = form.querySelector("#f-range");
  const fFrom = form.querySelector("#f-from");
  const fTo = form.querySelector("#f-to");
  const fResolution = form.querySelector("#f-resolution");
  const customLabels = form.querySelectorAll(".custom-only");

  let comboChart = null;
  let cumChart = null;

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
    const data = await res.json();
    render(data);
  }

  function render(data) {
    const labels = data.buckets.map((b) => bucketLabel(b.bucket_local_iso, data.resolution));
    const prices = data.buckets.map((b) => b.avg_price_pln_per_kwh);
    const kwh = data.buckets.map((b) => b.kwh);
    const cumulative = data.cumulative_cost_pln;
    const nowIdx = data.buckets.findIndex((b) => b.is_now);

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
    document.getElementById("t-count").textContent = data.totals.bucket_count;

    renderCombo(labels, prices, kwh, nowIdx);
    renderCumulative(labels, cumulative);
    renderTable(data.buckets, data.resolution);
  }

  function renderCombo(labels, prices, kwh, nowIdx) {
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
        nowIdx,
      }),
    });
  }

  function renderCumulative(labels, cumulative) {
    const canvas = document.getElementById("chart-cumcost");
    if (cumChart) cumChart.destroy();
    cumChart = new Chart(canvas, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Cumulative cost (PLN)",
            data: cumulative,
            borderColor: "#9affb6",
            backgroundColor: "rgba(154, 255, 182, 0.15)",
            fill: true,
            tension: 0.2,
            pointRadius: 0,
            spanGaps: true,
          },
        ],
      },
      options: chartCommon({
        scales: {
          y: {
            type: "linear",
            position: "left",
            title: { display: true, text: "PLN" },
            grid: { color: "rgba(255,255,255,0.05)" },
            ticks: { color: "#8a8f98" },
          },
        },
      }),
    });
  }

  function chartCommon({ scales, nowIdx }) {
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
      plugins: {
        legend: { labels: { color: "#e6e8eb" } },
      },
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

  // Initial render
  refresh();
})();

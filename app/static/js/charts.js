// Hourly combo chart: price line + consumption bars over the last 24 h
// (history) plus the next 24 h (forecast — bars will be empty there).

(async function () {
  const canvas = document.getElementById("chart-hourly");
  if (!canvas || typeof Chart === "undefined") return;

  const res = await fetch("/api/charts/hourly?hours=24&forecast_hours=24");
  if (!res.ok) {
    canvas.replaceWith(
      Object.assign(document.createElement("p"), {
        className: "muted",
        textContent: "Could not load chart data.",
      }),
    );
    return;
  }
  const data = await res.json();
  const labels = data.series.map((p) =>
    new Date(p.bucket_local).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }),
  );
  const prices = data.series.map((p) => p.price_pln_per_kwh);
  const kwh = data.series.map((p) => p.kwh);
  const nowIdx = data.series.findIndex((p) => p.is_now);

  new Chart(canvas, {
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
          label: "Price (PLN/kWh)",
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
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
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
        x: {
          ticks: { color: "#8a8f98", maxTicksLimit: 12 },
          grid: { color: "rgba(255,255,255,0.05)" },
        },
      },
      plugins: {
        legend: { labels: { color: "#e6e8eb" } },
        tooltip: {
          callbacks: {
            title: (items) => {
              const i = items[0]?.dataIndex ?? 0;
              const d = new Date(data.series[i].bucket_local);
              return d.toLocaleString();
            },
          },
        },
        annotation: nowIdx >= 0 ? {} : undefined, // placeholder; "now" line later
      },
    },
  });
})();

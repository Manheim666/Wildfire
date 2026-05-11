/* ═══════════════════════════════════════════════════════════
   MANHEIM Wildfire & Weather Intelligence — app.js  v2
   ═══════════════════════════════════════════════════════════ */

const RISK = {
  Low:      { color: "#22a66e", bg: "rgba(34,166,110,.08)",  gradient: "linear-gradient(135deg,#22a66e,#2ecc71)" },
  Moderate: { color: "#daa520", bg: "rgba(218,165,32,.08)",  gradient: "linear-gradient(135deg,#daa520,#f1c40f)" },
  High:     { color: "#e06730", bg: "rgba(224,103,48,.08)",  gradient: "linear-gradient(135deg,#e06730,#e67e22)" },
  Extreme:  { color: "#c0392b", bg: "rgba(192,57,43,.08)",   gradient: "linear-gradient(135deg,#c0392b,#e74c3c)" },
};
const riskColor = (lvl) => (RISK[lvl] || RISK.Low).color;
const riskBg    = (lvl) => (RISK[lvl] || RISK.Low).bg;

let forecast = [], hourlyForecast = [], metrics = {};
let selectedRegion = "Baku", selectedDate = "", selectedHour = "";
let forecastMode = "daily";
let map, markers = [];

const fmt  = (v) => `${Math.round((v || 0) * 100)}%`;
const f1   = (v) => Number(v || 0).toFixed(1);
const CIRC = 2 * Math.PI * 52;

/* ─── DATA LOADING ────────────────────────────────────── */
async function loadData() {
  try {
    const v = Date.now();
    const [fRes, mRes, hRes] = await Promise.all([
      fetch(`./data/forecast_30_days.json?v=${v}`),
      fetch(`./data/metrics.json?v=${v}`),
      fetch(`./data/hourly_forecast_168h.json?v=${v}`),
    ]);
    if (!fRes.ok) throw new Error(`Daily forecast: ${fRes.status}`);
    if (!mRes.ok) throw new Error(`Metrics: ${mRes.status}`);
    forecast       = await fRes.json();
    metrics        = await mRes.json();
    hourlyForecast = hRes.ok ? await hRes.json() : [];
  } catch (err) {
    document.querySelector("main").innerHTML =
      `<div style="padding:60px 28px;text-align:center"><h2>Could not load data</h2><p>${err.message}</p></div>`;
    document.body.classList.add("loaded");
    return;
  }
  // Filter out rows with null/undefined probability
  forecast = forecast.filter(d => d.probability != null && d.region);
  hourlyForecast = hourlyForecast.filter(d => d.region);
  selectedDate   = forecast[0]?.date || "";
  selectedRegion = forecast[0]?.region || "Baku";
  if (hourlyForecast.length) selectedHour = hourlyForecast[0].timestamp;
  initControls();
  initMap();
  renderAll();
  document.body.classList.add("loaded");
}

/* ─── HELPERS: get current data row depending on mode ── */
function currentRow() {
  if (forecastMode === "daily") {
    return forecast.find(d => d.region === selectedRegion && d.date === selectedDate)
        || forecast.find(d => d.region === selectedRegion)
        || forecast[0] || {};
  }
  // hourly — pick matching hour or first available
  const hr = hourlyForecast.filter(d => d.region === selectedRegion);
  if (selectedHour) {
    const match = hr.find(d => d.timestamp === selectedHour);
    if (match) return match;
  }
  return hr[0] || {};
}

function dayRows() {
  if (forecastMode === "daily") return forecast.filter(d => d.date === selectedDate);
  // hourly — pick latest hour per region for map snapshot
  const byRegion = {};
  hourlyForecast.forEach(r => {
    if (!byRegion[r.region]) byRegion[r.region] = r;
  });
  return Object.values(byRegion);
}

function hourlyRowsForRegion() {
  return hourlyForecast.filter(d => d.region === selectedRegion);
}

/* ─── CONTROLS ────────────────────────────────────────── */
function syncAllRegionSelects() {
  ["heroRegionSelect","regionSelect","panelRegionSelect"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.value = selectedRegion;
  });
}

function initControls() {
  const dates   = [...new Set(forecast.map(d => d.date))];
  const regions = [...new Set(forecast.map(d => d.region))].sort();
  const regOpts = regions.map(r => `<option value="${r}">${r}</option>`).join("");

  ["heroRegionSelect","regionSelect","panelRegionSelect"].forEach(id => {
    const el = document.getElementById(id);
    if (el) { el.innerHTML = regOpts; el.value = selectedRegion; }
  });

  const dp = document.getElementById("datePicker");
  dp.innerHTML = dates.map(d => {
    const lbl = new Date(d+"T00:00").toLocaleDateString("en-US",{month:"short",day:"numeric"});
    return `<option value="${d}">${lbl}</option>`;
  }).join("");
  dp.value = selectedDate;

  const tdf = document.getElementById("tableDateFilter");
  tdf.innerHTML = '<option value="">All dates</option>' + dates.map(d => {
    const lbl = new Date(d+"T00:00").toLocaleDateString("en-US",{month:"short",day:"numeric"});
    return `<option value="${d}">${lbl}</option>`;
  }).join("");

  dp.onchange = e => { selectedDate = e.target.value; renderAll(); };
  ["heroRegionSelect","regionSelect","panelRegionSelect"].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.onchange = e => { selectedRegion = e.target.value; syncAllRegionSelects(); renderAll(); };
  });

  document.getElementById("toggleDaily").onclick  = () => setMode("daily");
  document.getElementById("toggleHourly").onclick = () => setMode("hourly");
  document.getElementById("riskFilter").onchange  = renderTable;
  tdf.onchange = renderTable;

  const modelName = metrics.selected_model || "Model";
  const days = metrics.prediction_horizon_days || 30;
  document.getElementById("modelStatus").textContent = `${modelName} · ${days}-day`;
}

/* ─── MAP ─────────────────────────────────────────────── */
function initMap() {
  map = L.map("map", { zoomControl: true, scrollWheelZoom: true }).setView([40.35, 47.8], 7);
  L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
    maxZoom: 18, attribution: "CartoDB"
  }).addTo(map);
}

function renderMap() {
  markers.forEach(m => m.remove()); markers = [];
  if (!map) return;
  const rows = dayRows();
  rows.forEach(row => {
    if (!row.Latitude || !row.Longitude) return;
    const sel = row.region === selectedRegion;
    const prob = row.probability || 0;
    const lvl  = row.risk_level || "Low";
    const col  = riskColor(lvl);
    const c = L.circle([row.Latitude, row.Longitude], {
      radius: 22000, color: col,
      weight: sel ? 3 : 1, fillColor: col,
      fillOpacity: sel ? 0.25 : 0.10,
    }).addTo(map);
    const m = L.marker([row.Latitude, row.Longitude], {
      icon: L.divIcon({ className: "", iconSize: [22,22], iconAnchor: [11,11],
        html: `<div style="width:${sel?24:16}px;height:${sel?24:16}px;border-radius:50%;background:${col};border:3px solid #fff;box-shadow:0 2px 10px rgba(0,0,0,.3);transition:all .2s"></div>` })
    }).addTo(map);
    m.bindTooltip(`<b>${row.region}</b><br>${lvl} · ${fmt(prob)}`, {direction:"top",offset:[0,-10]});
    const pick = () => { selectedRegion = row.region; syncAllRegionSelects(); renderAll(); };
    m.on("click", pick); c.on("click", pick);
    markers.push(c, m);
  });
}

/* ─── HERO ────────────────────────────────────────────── */
function renderHero() {
  const row = currentRow();
  if (!row || !row.region) return;

  document.getElementById("heroCity").textContent = row.region;
  document.getElementById("heroTemp").textContent = forecastMode === "daily"
    ? `${f1(row.temp_min ?? row.temperature ?? 0)}–${f1(row.temp_max ?? row.temperature ?? 0)}°C`
    : `${f1(row.temperature || 0)}°C`;
  document.getElementById("heroWind").textContent     = `${f1(row.wind || 0)} km/h`;
  document.getElementById("heroHumidity").textContent = `${f1(row.humidity || 0)}%`;

  const rainEl = document.getElementById("heroRain");
  if (forecastMode === "daily") {
    rainEl.textContent = `${Number(row.rain || 0).toFixed(2)} mm`;
    rainEl.closest(".cond-card").querySelector("small").textContent = "Rain";
    document.getElementById("heroSummary").textContent = row.climate_summary || "";
    document.getElementById("heroWarning").textContent = row.warning || "";
    document.getElementById("heroSubtitle").textContent = `Azerbaijan · ${row.date || ""}`;
  } else {
    rainEl.textContent = `${f1(row.solar || 0)} W/m²`;
    rainEl.closest(".cond-card").querySelector("small").textContent = "Solar";
    const ts = row.timestamp ? new Date(row.timestamp) : null;
    document.getElementById("heroSummary").textContent =
      ts ? `Hourly forecast from ${ts.toLocaleDateString("en",{month:"short",day:"numeric"})}` : "";
    document.getElementById("heroWarning").textContent = "";
    document.getElementById("heroSubtitle").textContent = `Azerbaijan · 168-Hour Forecast`;
  }

  // risk ring
  const pct = row.probability || 0;
  const lvl = row.risk_level || "Low";
  const col = riskColor(lvl);
  document.getElementById("heroRiskPct").textContent   = fmt(pct);
  document.getElementById("heroRiskLevel").textContent = lvl;
  document.getElementById("heroRiskLevel").style.color = col;
  const arc = document.getElementById("riskArc");
  arc.style.strokeDashoffset = CIRC - (CIRC * Math.min(pct, 1));
  arc.style.stroke = col;
}

/* ─── DETAIL PANEL ────────────────────────────────────── */
function renderPanel() {
  const row = currentRow();
  if (!row || !row.region) return;

  document.getElementById("panelCity").textContent = row.region;
  document.getElementById("panelRegionSelect").value = row.region;
  const chip = document.getElementById("dpRiskChip");
  const lvl = row.risk_level || "Low";
  chip.textContent = lvl;
  chip.style.background = riskColor(lvl);
  document.getElementById("dpRiskPct").textContent = fmt(row.probability || 0);
  document.getElementById("dpRiskRow").style.background = riskBg(lvl);
  document.getElementById("dpTemp").textContent = forecastMode === "daily"
    ? `${f1(row.temp_min ?? row.temperature ?? 0)}–${f1(row.temp_max ?? row.temperature ?? 0)}°C`
    : `${f1(row.temperature || 0)}°C`;
  document.getElementById("dpWind").textContent     = `${f1(row.wind || 0)} km/h`;
  document.getElementById("dpHumidity").textContent = `${f1(row.humidity || 0)}%`;

  if (forecastMode === "daily") {
    document.getElementById("dpRain").textContent = `${Number(row.rain || 0).toFixed(2)} mm`;
    document.getElementById("dpSummary").textContent = row.climate_summary || "";
    document.getElementById("dpWarning").textContent = row.warning || "";
  } else {
    document.getElementById("dpRain").textContent = `${f1(row.solar || 0)} W/m²`;
    document.getElementById("dpSummary").textContent = "";
    document.getElementById("dpWarning").textContent = "";
  }

  // mini trend — daily or hourly
  if (forecastMode === "daily") {
    const rr = forecast.filter(d => d.region === selectedRegion);
    Plotly.react("dpTrendChart", [{
      x: rr.map(d => d.date), y: rr.map(d => d.probability*100),
      type:"scatter", mode:"lines", line:{color:"#4a9eda",width:2},
      fill:"tozeroy", fillcolor:"rgba(74,158,218,.10)",
      hovertemplate:"%{x}<br>%{y:.1f}%<extra></extra>",
    }], {
      margin:{t:4,r:4,b:24,l:30}, paper_bgcolor:"transparent", plot_bgcolor:"transparent",
      yaxis:{ticksuffix:"%",gridcolor:"#eef2f6",tickfont:{size:10}},
      xaxis:{gridcolor:"#eef2f6",tickfont:{size:9}},
    }, {displayModeBar:false, responsive:true});
  } else {
    const rr = hourlyRowsForRegion();
    Plotly.react("dpTrendChart", [{
      x: rr.map(d => d.timestamp), y: rr.map(d => (d.probability||0)*100),
      type:"scatter", mode:"lines", line:{color:"#4a9eda",width:2},
      fill:"tozeroy", fillcolor:"rgba(74,158,218,.10)",
      hovertemplate:"%{x}<br>%{y:.1f}%<extra></extra>",
    }], {
      margin:{t:4,r:4,b:24,l:30}, paper_bgcolor:"transparent", plot_bgcolor:"transparent",
      yaxis:{ticksuffix:"%",gridcolor:"#eef2f6",tickfont:{size:10}},
      xaxis:{gridcolor:"#eef2f6",tickfont:{size:9}},
    }, {displayModeBar:false, responsive:true});
  }
}

/* ─── FORECAST STRIP ──────────────────────────────────── */
function renderStrip() {
  const strip = document.getElementById("forecastStrip");
  if (forecastMode === "daily") {
    document.getElementById("stripTitle").textContent = "30-Day Outlook";
    const rows = forecast.filter(d => d.region === selectedRegion);
    strip.innerHTML = rows.map(row => {
      const dt = new Date(row.date+"T00:00");
      const active = row.date === selectedDate ? " active" : "";
      const col = riskColor(row.risk_level);
      return `<div class="fc-card${active}" data-date="${row.date}">
        <div class="fc-date">${dt.toLocaleDateString("en",{weekday:"short"})}<br>${dt.toLocaleDateString("en",{month:"short",day:"numeric"})}</div>
        <div class="fc-risk-dot" style="background:${col}">${Math.round(row.probability*100)}</div>
        <div class="fc-temp">${f1(row.temp_max ?? row.temperature)}°/${f1(row.temp_min ?? row.temperature)}°</div>
        <div class="fc-wind">${f1(row.wind)} km/h</div>
        <div class="fc-label" style="background:${col}22;color:${col}">${row.risk_level}</div>
      </div>`;
    }).join("");
    strip.querySelectorAll(".fc-card").forEach(c => {
      c.onclick = () => { selectedDate = c.dataset.date; document.getElementById("datePicker").value = selectedDate; renderAll(); };
    });
  } else {
    document.getElementById("stripTitle").textContent = "168-Hour Outlook";
    const rows = hourlyRowsForRegion();
    const sampled = rows.filter((_,i) => i % 3 === 0);
    strip.innerHTML = sampled.map((row,i) => {
      const ts = new Date(row.timestamp);
      const prob = row.probability || 0;
      const lvl  = row.risk_level || "Low";
      const col  = riskColor(lvl);
      const active = i === 0 ? " active" : "";
      return `<div class="fc-card${active}" data-ts="${row.timestamp}">
        <div class="fc-date">${ts.toLocaleDateString("en",{weekday:"short"})}<br>${ts.getHours().toString().padStart(2,"0")}:00</div>
        <div class="fc-risk-dot" style="background:${col}">${Math.round(prob*100)}</div>
        <div class="fc-temp">${f1(row.temperature)}°</div>
        <div class="fc-wind">${f1(row.wind)} km/h</div>
        <div class="fc-label" style="background:${col}22;color:${col}">${lvl}</div>
      </div>`;
    }).join("");
  }
}

/* ─── DAILY CHARTS ────────────────────────────────────── */
const LAYOUT = (extra={}) => ({
  margin:{t:8,r:12,b:40,l:46}, paper_bgcolor:"transparent", plot_bgcolor:"transparent",
  yaxis:{gridcolor:"#eef2f6", ...extra.yaxis},
  xaxis:{gridcolor:"#eef2f6", ...extra.xaxis},
  legend:{orientation:"h",y:1.12,font:{size:12}},
  ...extra,
});
const PCFG = {displayModeBar:false, responsive:true};

function renderDailyCharts() {
  const rr = forecast.filter(d => d.region === selectedRegion);
  Plotly.react("mainRiskChart", [{
    x:rr.map(d=>d.date), y:rr.map(d=>d.probability*100),
    type:"bar", marker:{color:rr.map(d=>riskColor(d.risk_level)), opacity:0.85, line:{width:0}},
    hovertemplate:"%{x}<br>Risk: %{y:.1f}%<extra></extra>",
  }], LAYOUT({yaxis:{ticksuffix:"%",gridcolor:"#eef2f6"}}), PCFG);

  Plotly.react("mainWeatherChart", [
    {x:rr.map(d=>d.date), y:rr.map(d=>d.temp_max ?? d.temperature), name:"Max °C", type:"scatter", mode:"lines+markers", line:{color:"#e06730",width:2}, marker:{size:4}},
    {x:rr.map(d=>d.date), y:rr.map(d=>d.temp_min ?? d.temperature), name:"Min °C", type:"scatter", mode:"lines+markers", line:{color:"#4a9eda",width:2}, marker:{size:4}},
    {x:rr.map(d=>d.date), y:rr.map(d=>d.wind),        name:"Wind km/h", type:"scatter", mode:"lines+markers", line:{color:"#e06730",width:2}, marker:{size:4}},
    {x:rr.map(d=>d.date), y:rr.map(d=>d.humidity),     name:"Humidity %", type:"scatter", mode:"lines", line:{color:"#22a66e",width:1.5,dash:"dot"}, yaxis:"y2"},
  ], LAYOUT({yaxis:{gridcolor:"#eef2f6"}, yaxis2:{overlaying:"y",side:"right",showgrid:false,ticksuffix:"%"}}), PCFG);
}

/* ─── HOURLY CHARTS ───────────────────────────────────── */
function renderHourlyCharts() {
  const rr = hourlyRowsForRegion();
  if (!rr.length) return;
  const hasRisk = rr[0].probability !== undefined;

  const hl = (title) => LAYOUT({yaxis:{title,gridcolor:"#eef2f6"}});

  if (hasRisk) {
    const cols = rr.map(d => riskColor(d.risk_level||"Low"));
    Plotly.react("hourlyRiskChart", [
      {x:rr.map(d=>d.timestamp), y:rr.map(d=>(d.probability||0)*100), type:"scatter", mode:"lines",
       line:{color:"#4a9eda",width:2}, fill:"tozeroy", fillcolor:"rgba(74,158,218,.06)",
       name:"Fire Risk", hovertemplate:"%{x}<br>%{y:.1f}%<extra></extra>"},
      {x:rr.map(d=>d.timestamp), y:rr.map(d=>(d.probability||0)*100), type:"bar",
       marker:{color:cols,opacity:0.25}, showlegend:false, hoverinfo:"skip"},
    ], LAYOUT({yaxis:{title:"Fire Risk %",ticksuffix:"%",gridcolor:"#eef2f6"},barmode:"overlay"}), PCFG);
  }

  Plotly.react("hourlyTempChart", [{
    x:rr.map(d=>d.timestamp), y:rr.map(d=>d.temperature), type:"scatter", mode:"lines",
    line:{color:"#4a9eda",width:2}, fill:"tozeroy", fillcolor:"rgba(74,158,218,.06)",
    hovertemplate:"%{x}<br>%{y:.1f}°C<extra></extra>",
  }], hl("°C"), PCFG);

  Plotly.react("hourlyHumidityChart", [{
    x:rr.map(d=>d.timestamp), y:rr.map(d=>d.humidity), type:"scatter", mode:"lines",
    line:{color:"#22a66e",width:2}, fill:"tozeroy", fillcolor:"rgba(34,166,110,.06)",
    hovertemplate:"%{x}<br>%{y:.1f}%<extra></extra>",
  }], hl("%"), PCFG);

  Plotly.react("hourlyWindChart", [{
    x:rr.map(d=>d.timestamp), y:rr.map(d=>d.wind), type:"scatter", mode:"lines",
    line:{color:"#e06730",width:2}, fill:"tozeroy", fillcolor:"rgba(224,103,48,.06)",
    hovertemplate:"%{x}<br>%{y:.1f} km/h<extra></extra>",
  }], hl("km/h"), PCFG);
}

/* ─── TABLE ───────────────────────────────────────────── */
function renderTable() {
  const rf = document.getElementById("riskFilter").value;
  const df = document.getElementById("tableDateFilter").value;

  if (forecastMode === "daily") {
    document.getElementById("tableTitle").textContent = "Detailed Forecast";
    let rows = forecast.filter(d => d.region === selectedRegion);
    if (rf !== "All") rows = rows.filter(d => d.risk_level === rf);
    if (df) rows = rows.filter(d => d.date === df);
    document.getElementById("forecastTable").innerHTML = rows.map(r => `
      <tr>
        <td>${r.date}</td><td>${r.region}</td>
        <td><span class="risk-chip" style="background:${riskColor(r.risk_level)}">${r.risk_level}</span></td>
        <td>${fmt(r.probability)}</td>
        <td>${f1(r.temp_max ?? r.temperature)}°/${f1(r.temp_min ?? r.temperature)}°C</td><td>${f1(r.wind)} km/h</td><td>${f1(r.humidity)}%</td>
      </tr>`).join("");
  } else {
    document.getElementById("tableTitle").textContent = "Hourly Forecast Detail";
    let rows = hourlyRowsForRegion();
    if (rf !== "All") rows = rows.filter(d => (d.risk_level||"Low") === rf);
    document.getElementById("forecastTable").innerHTML = rows.map(r => {
      const prob = r.probability || 0;
      const lvl  = r.risk_level || "Low";
      return `<tr>
        <td>${r.timestamp}</td><td>${r.region}</td>
        <td><span class="risk-chip" style="background:${riskColor(lvl)}">${lvl}</span></td>
        <td>${fmt(prob)}</td>
        <td>${f1(r.temperature)}°C</td><td>${f1(r.wind)} km/h</td><td>${f1(r.humidity)}%</td>
      </tr>`;
    }).join("");
  }
}

/* ─── MODE TOGGLE ─────────────────────────────────────── */
function setMode(mode) {
  forecastMode = mode;
  document.getElementById("toggleDaily").classList.toggle("active", mode==="daily");
  document.getElementById("toggleHourly").classList.toggle("active", mode==="hourly");
  document.getElementById("chartsSection").style.display        = mode==="daily" ? "" : "none";
  document.getElementById("hourlyChartsSection").style.display  = mode==="hourly" ? "" : "none";
  // Show table in both modes
  document.getElementById("tableSection").style.display         = "";
  document.getElementById("chartRiskTitle").textContent = mode==="daily" ? "Risk Trend — 30 Days" : "Fire Risk — 168 Hours";

  // Update model status badge
  if (mode === "hourly" && metrics.hourly_model) {
    document.getElementById("modelStatus").textContent =
      `${metrics.hourly_model.model_name} · ${metrics.hourly_model.prediction_horizon_hours}h`;
  } else {
    document.getElementById("modelStatus").textContent =
      `${metrics.selected_model || "Model"} · ${metrics.prediction_horizon_days || 30}-day`;
  }
  renderAll();
}

/* ─── RENDER ALL ──────────────────────────────────────── */
function renderAll() {
  renderHero();
  renderMap();
  renderPanel();
  renderStrip();
  renderTable();
  if (forecastMode === "daily") {
    renderDailyCharts();
  } else {
    renderHourlyCharts();
  }
}

/* ─── BOOT ────────────────────────────────────────────── */
loadData().catch(err => {
  console.error("MANHEIM load error:", err);
  document.querySelector("main").innerHTML =
    `<div style="padding:60px 28px;text-align:center"><h2>Could not load data</h2><p>${err.message}</p></div>`;
  document.body.classList.add("loaded");
});

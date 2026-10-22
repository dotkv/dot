"use strict";

(() => {
  const byID = (id) => document.getElementById(id);
  const chart = byID("chart");
  const context = chart.getContext("2d");
  const scrubber = byID("scrubber");
  const metricButtons = [...document.querySelectorAll("[data-metric]")];
  const ranges = [...document.querySelectorAll("[data-range]")];
  const metrics = {
    disk_bytes: { label: "On-disk size", color: "--accent", unit: "bytes" },
    obsolete_bytes: { label: "Obsolete bytes", color: "--obsolete", unit: "bytes" },
    entry_count: { label: "Entries", color: "--entries", unit: "count" },
    index_mapped_bytes: { label: "Index mmap", color: "--update", unit: "bytes" },
  };
  const operations = ["get", "set", "delete", "update"];
  const operationColors = { get: "--accent", set: "--obsolete", delete: "--entries", update: "--update" };
  const tooltip = byID("tooltip");
  for (const op of operations) {
    const name = op[0].toUpperCase() + op.slice(1);
    for (const [suffix, label, unit] of [["ops", "ops/sec", "rate"], ["mean_ns", "mean latency", "latency"], ["p99_ns", "p99 latency", "latency"]]) {
      const key = `${op}_${suffix}`;
      metrics[key] = { label: `${name} ${label}`, color: operationColors[op], unit, dash: suffix === "p99_ns" ? [5, 3] : [] };
    }
  }
  for (const [key, metric] of Object.entries(metrics)) {
    const marker = document.createElement("span");
    marker.id = `point-${key}`;
    marker.className = "hover-point";
    marker.style.setProperty("--series-color", `var(${metric.color})`);
    marker.hidden = true;
    tooltip.before(marker);
    const row = document.createElement("div");
    row.id = `tooltip-${key}`;
    row.className = "tooltip-row";
    const labelElement = document.createElement("span");
    const dot = document.createElement("i");
    dot.className = "series-dot";
    dot.style.setProperty("--series-color", `var(${metric.color})`);
    labelElement.append(dot, metric.label);
    const value = document.createElement("strong");
    value.id = `tooltip-value-${key}`;
    row.append(labelElement, value);
    tooltip.append(row);
  }
  const selected = new Set(["disk_bytes"]);
  // A ring bounds both memory and rendering work, even at very short intervals.
  const capacity = 20_000;
  const history = new Array(capacity);
  let head = 0;
  let count = 0;
  const at = (index) => history[(head + index) % capacity];
  const number = new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 });
  const decimal = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });
  const fixedDecimal = new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  const compactRate = new Intl.NumberFormat(undefined, { notation: "compact", minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const clock = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  const timestamp = new Intl.DateTimeFormat(undefined, {
    year: "numeric", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
    second: "2-digit", fractionalSecondDigits: 3,
  });
  const byteUnits = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  const latencyUnits = ["ns", "µs", "ms", "s"];
  let range = 60_000;
  let interval = 1000;
  let latest = null;
  let following = true;
  // Keep absolute bounds while paused, even as the ring receives or evicts data.
  let viewport = null;
  let view = null;
  let pointer = null;
  let pan = null;
  let scrub = null;
  let frame = null;
  let socket = null;
  let retry = null;
  let stopped = false;
  let serverStart = 0;
  let serverRetention = 0;
  let historyError = "";
  let historyLoadError = "";
  let diskStale = false;
  let loaded = null;
  let pending = null;
  let historyRequest = null;
  let historyRetryAt = 0;

  const oldestTime = () => serverStart ? Math.max(serverStart, serverRetention ? Date.now() - serverRetention : serverStart) : at(0).time;
  function updateNotice() {
    notice([diskStale ? "Disk usage is unavailable; showing its previous reading." : "", historyError || historyLoadError].filter(Boolean).join(" "));
  }

  function byteUnit(value) {
    return Math.min(byteUnits.length - 1, Math.max(0, Math.floor(Math.log2(value || 1) / 10)));
  }

  function bytes(value) {
    const unit = byteUnit(value);
    return `${(value / 1024 ** unit).toLocaleString(undefined, { maximumFractionDigits: unit ? 1 : 0 })} ${byteUnits[unit]}`;
  }

  function format(key, value, exact = false) {
    if (value === null) return "—";
    if (metrics[key].unit === "rate") return rate(value);
    if (metrics[key].unit === "latency") {
      const unit = latencyUnit(value);
      const display = `${key.endsWith("p99_ns") ? "≈ " : ""}${fixedDecimal.format(value / 1000 ** unit)} ${latencyUnits[unit]}`;
      return exact && unit > 0 ? `${display} (${fixedDecimal.format(value)} ns)` : display;
    }
    if (metrics[key].unit !== "bytes") return number.format(value);
    return exact && value >= 1024 ? `${bytes(value)} (${number.format(value)} bytes)` : bytes(value);
  }

  function rate(value) {
    return (value < 1000 ? fixedDecimal : compactRate).format(value);
  }

  function latencyUnit(value) {
    return Math.min(latencyUnits.length - 1, Math.max(0, Math.floor(Math.log10(value || 1) / 3)));
  }

  function status(text, connected = false) {
    byID("status").textContent = text;
    byID("status-dot").classList.toggle("connected", connected);
  }

  function notice(text) {
    byID("notice").textContent = text;
    byID("notice").hidden = !text;
  }

  function requestDraw() {
    if (frame !== null) return;
    frame = requestAnimationFrame(() => { frame = null; draw(); });
  }

  function chartSample(sample, resolution = sample.interval_ms) {
    if (![sample.time, sample.interval_ms, sample.disk_bytes, sample.obsolete_bytes, sample.entry_count, sample.index_mapped_bytes]
      .every((value) => Number.isFinite(value) && value >= 0)) throw new Error("Invalid snapshot");
    const point = {
      time: sample.time, interval_ms: Math.max(1, resolution), gap_before: Boolean(sample.gap_before), disk_bytes: sample.disk_bytes,
      obsolete_bytes: sample.obsolete_bytes, entry_count: sample.entry_count,
      index_mapped_bytes: sample.index_mapped_bytes,
    };
    for (const op of operations) {
      const reading = sample.operations?.[op];
      if (!reading || !Number.isFinite(reading.ops_per_second) || reading.ops_per_second < 0
        || ![reading.mean_ns, reading.p99_ns].every((value) => value === null || (Number.isFinite(value) && value >= 0))) throw new Error("Invalid operation metrics");
      point[`${op}_ops`] = reading.ops_per_second;
      point[`${op}_mean_ns`] = reading.mean_ns;
      point[`${op}_p99_ns`] = reading.p99_ns;
    }
    if (!sample.ready) point.disk_bytes = point.obsolete_bytes = point.entry_count = point.index_mapped_bytes = null;
    return point;
  }

  function receive(sample) {
    const point = chartSample(sample);
    interval = Math.max(1, sample.interval_ms);
    status("Connected", true);
    if (Number.isFinite(sample.history_start) && sample.history_start > 0) serverStart = sample.history_start;
    if (Number.isFinite(sample.history_retention_ms) && sample.history_retention_ms > 0) serverRetention = sample.history_retention_ms;
    historyError = sample.history_error || "";
    diskStale = sample.disk_stale;
    updateNotice();
    // Reconnecting can send the current snapshot again. Store only chart fields.
    if (latest && sample.time < latest.time) return;
    point.gap_before ||= Boolean(latest && point.time - latest.time > interval * 3);
    latest = point;
    for (const op of operations) {
      for (const suffix of ["ops", "mean_ns", "p99_ns"]) {
        const key = `${op}_${suffix}`;
        byID(`value-${key}`).textContent = format(key, latest[key]);
      }
    }
    if (!count || sample.time > at(count - 1).time) {
      history[(head + count) % capacity] = latest;
      if (count < capacity) count++;
      else { head = (head + 1) % capacity; loaded = null; }
    }
    byID("empty").hidden = true;
    requestDraw();
  }

  function loadHistory(start, span) {
    if (!serverStart || stopped || historyError || Date.now() < historyRetryAt) return;
    const end = Math.min(start + span, latest.time);
    start = Math.max(start, oldestTime());
    const keys = [...selected].sort().join(",");
    const resolution = Math.max(1000, span * 1.5 / 500 + 1);
    const covers = (cache) => cache && cache.keys === keys && cache.start <= start && cache.end >= end && cache.resolution <= resolution;
    if (covers(loaded)) {
      if (pending && !covers(pending)) { historyRequest?.abort(); pending = null; }
      return;
    }
    if (covers(pending)) return;
    historyRequest?.abort();
    const controller = new AbortController();
    historyRequest = controller;
    const queryStart = Math.max(oldestTime(), Math.floor(start - span / 4));
    const queryEnd = Math.max(queryStart, Math.floor(Math.min(Date.now(), end + span / 4)));
    const liveAfter = latest.time;
    const coverage = { keys, start: queryStart, end: queryEnd >= latest.time ? Infinity : queryEnd,
      resolution: Math.max(1000, Math.floor((queryEnd - queryStart) / 500) + 1) };
    pending = coverage;
    const url = new URL("history", window.location.href);
    url.searchParams.set("start", queryStart);
    url.searchParams.set("end", queryEnd);
    url.searchParams.set("metrics", keys);
    fetch(url, { signal: controller.signal }).then(async (response) => {
      if (!response.ok) throw new Error("History unavailable");
      const data = await response.json();
      if (controller.signal.aborted || stopped) return;
      if (!Array.isArray(data.samples) || data.samples.length > capacity || !Number.isFinite(data.resolution_ms) || data.resolution_ms < 1000) throw new Error("Invalid history");
      const points = data.samples.map((sample) => chartSample(sample, data.resolution_ms));
      if (points.some((point, i) => i && point.time <= points[i - 1].time)) throw new Error("Invalid history order");
      // Keep every live reading from the request's start, even if the response
      // has not saved it yet. Prefer history on overlap: it has authoritative
      // gap markers for periods when the browser was disconnected.
      const seen = new Set(points.map((point) => point.time));
      for (let i = 0; i < count; i++) {
        const point = at(i);
        if (point.time >= liveAfter && !seen.has(point.time)) {
          points.push(point);
          seen.add(point.time);
        }
      }
      if (!seen.has(latest.time)) points.push(latest);
      points.sort((a, b) => a.time - b.time);
      head = 0;
      count = Math.min(capacity, points.length);
      for (let i = 0; i < count; i++) history[i] = points[points.length - count + i];
      loaded = { ...coverage, resolution: data.resolution_ms };
      historyLoadError = "";
      updateNotice();
      requestDraw();
    }).catch(() => {
      if (controller.signal.aborted || stopped) return;
      historyLoadError = "Metric history could not be loaded; retrying shortly.";
      historyRetryAt = Date.now() + 5000;
      updateNotice();
    }).finally(() => {
      if (historyRequest === controller) pending = null;
    });
  }

  function lowerBound(time, low = 0) {
    let high = count;
    while (low < high) {
      const mid = Math.floor((low + high) / 2);
      if (at(mid).time < time) low = mid + 1;
      else high = mid;
    }
    return low;
  }

  function visibleWindow() {
    if (!following) return viewport;
    const end = Math.max(Date.now(), latest.time);
    const start = range === Infinity ? oldestTime() : end - range;
    return { start, span: Math.max(interval, end - start) };
  }

  function historyBounds(span, end = Math.max(Date.now(), latest.time)) {
    return { start: Math.min(oldestTime(), end - span), end };
  }

  function setFollowing(enabled) {
    if (!enabled && following) viewport = view ? { start: view.start, span: view.span } : visibleWindow();
    following = enabled;
    requestDraw();
  }

  function navigate(start, span, end) {
    // Use the gesture's live edge so incoming samples cannot move its target.
    const bounds = historyBounds(span, end);
    setFollowing(start >= bounds.end - span);
    viewport = { start: Math.max(bounds.start, Math.min(bounds.end - span, start)), span };
  }

  function updateNavigation(window) {
    const oldest = oldestTime();
    const newest = Math.max(Date.now(), latest.time);
    const duration = Math.max(1, newest - oldest);
    const maximum = Math.max(0, newest - oldest - window.span);
    const position = Math.max(0, Math.min(maximum, window.start - oldest));
    const left = Math.max(0, Math.min(1, (window.start - oldest) / duration));
    const right = Math.max(0, Math.min(1, (window.start + window.span - oldest) / duration));
    const width = Math.max(0.3, (right - left) * 100);
    byID("scrubber-window").style.left = `${Math.min(100 - width, left * 100)}%`;
    byID("scrubber-window").style.width = `${width}%`;
    scrubber.setAttribute("aria-valuemax", String(Math.round(maximum)));
    scrubber.setAttribute("aria-valuenow", String(Math.round(position)));
    scrubber.setAttribute("aria-valuetext", `Viewing ${timestamp.format(window.start)} to ${timestamp.format(window.start + window.span)}`);
    scrubber.setAttribute("aria-disabled", String(maximum === 0));
    scrubber.tabIndex = maximum > 0 ? 0 : -1;
    chart.classList.toggle("pannable", maximum > 0);
    const label = newest - oldest >= 86_400_000 ? timestamp : clock;
    for (const [id, time] of [["loaded-start", oldest], ["loaded-end", latest.time]]) {
      byID(id).textContent = label.format(time);
      byID(id).dateTime = new Date(time).toISOString();
      byID(id).title = timestamp.format(time);
    }
  }

  function niceStep(value) {
    const magnitude = 10 ** Math.floor(Math.log10(value));
    const fraction = value / magnitude;
    return (fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10) * magnitude;
  }

  function axis(key, maximum) {
    // Round in the displayed unit so byte labels stay at 100 KiB, 200 KiB, etc.
    const kind = metrics[key].unit;
    const unit = kind === "bytes" ? byteUnit(maximum) : kind === "latency" ? latencyUnit(maximum) : 0;
    const divisor = (kind === "bytes" ? 1024 : 1000) ** unit;
    const step = Math.max(kind === "rate" ? 0 : 1, niceStep(Math.max(maximum / divisor, kind === "rate" ? 0.01 : 1) / 4));
    const ticks = Math.max(1, Math.ceil(maximum / divisor / step));
    return { ceiling: ticks * step * divisor, ticks, step, unit };
  }

  function timeStep(span, width) {
    const target = span / Math.max(2, Math.floor(width / 125));
    const steps = [1000, 2000, 5000, 10_000, 15_000, 30_000, 60_000, 120_000,
      300_000, 600_000, 900_000, 1_800_000, 3_600_000, 7_200_000, 10_800_000,
      21_600_000, 43_200_000, 86_400_000];
    return steps.find((step) => step >= target) || niceStep(target / 86_400_000) * 86_400_000;
  }

  function draw() {
    const bounds = chart.getBoundingClientRect();
    const width = bounds.width, height = bounds.height;
    if (!width || !height) return;
    const scale = window.devicePixelRatio || 1;
    chart.width = Math.round(width * scale);
    chart.height = Math.round(height * scale);
    context.setTransform(scale, 0, 0, scale, 0, 0);
    if (!latest) return;
    const colors = getComputedStyle(document.documentElement);
    const muted = colors.getPropertyValue("--muted").trim();
    const grid = colors.getPropertyValue("--grid").trim();
    const keys = [...selected];
    const left = 86, right = width - 20, top = 20, bottom = height - 34;
    const { start, span } = visibleWindow();
    const end = start + span;
    loadHistory(start, span);
    const low = lowerBound(start);
    let high = lowerBound(end, low);
    if (high < count && at(high).time === end) high++;
    const first = Math.max(0, low - 1);
    const x = (time) => left + (time - start) / span * (right - left);
    const maxima = keys.map((key) => {
      let maximum = 0;
      // Use all loaded samples so panning does not change the value scale.
      for (let i = 0; i < count; i++) maximum = Math.max(maximum, at(i)[key]);
      return maximum;
    });
    const limits = axis(keys[0], Math.max(...maxima));
    const y = (value) => bottom - value / limits.ceiling * (bottom - top);
    const series = keys.map((key) => ({ key, color: colors.getPropertyValue(metrics[key].color).trim(), y }));
    context.font = "11px -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif";
    for (let i = 0; i <= limits.ticks; i++) {
      const row = bottom - (bottom - top) * i / limits.ticks;
      context.strokeStyle = grid;
      context.lineWidth = 1;
      context.beginPath(); context.moveTo(left, row); context.lineTo(right, row); context.stroke();
      context.fillStyle = muted;
      context.textAlign = "right";
      const unit = metrics[keys[0]].unit;
      const suffix = unit === "bytes" ? ` ${byteUnits[limits.unit]}` : unit === "latency" ? ` ${latencyUnits[limits.unit]}` : "";
      const value = limits.step * i;
      const label = unit === "rate" ? rate(value) : `${(unit === "latency" ? fixedDecimal : decimal).format(value)}${suffix}`;
      context.fillText(label, left - 12, row + 4);
    }
    const step = timeStep(span, right - left);
    const offset = new Date(start).getTimezoneOffset() * 60_000;
    const firstTick = Math.ceil((start - offset) / step) * step + offset;
    context.fillStyle = muted;
    const dateClock = span >= 86_400_000
      ? new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" }) : clock;
    for (let time = firstTick; time <= start + span; time += step) {
      context.textAlign = "center";
      // Keep centered labels inside the canvas at either end.
      const label = dateClock.format(time);
      const half = context.measureText(label).width / 2;
      const column = Math.max(left + half, Math.min(right - half, x(time)));
      context.fillText(label, column, height - 9);
    }

    context.save();
    context.beginPath(); context.rect(left, top - 3, right - left + 3, bottom - top + 6); context.clip();
    for (const line of series) {
      // Keep first/last and extrema per pixel; compaction drops remain visible.
      const points = [];
      const gaps = new Set();
      let bucket = null;
      let missing = false;
      function flush() {
        if (!bucket) return;
        const keep = [...new Set([bucket.first, bucket.min, bucket.max, bucket.last])];
        keep.sort((a, b) => a.time - b.time);
        points.push(...keep);
      }
      for (let i = first; i < high; i++) {
        const point = at(i);
        if (point[line.key] === null) {
          flush(); bucket = null; missing = true;
          continue;
        }
        const pixel = Math.floor(x(point.time));
        if (missing || point.gap_before || (i > 0 && point.time - at(i - 1).time > Math.max(point.interval_ms, at(i - 1).interval_ms) * 3)) {
          flush();
          bucket = null;
          gaps.add(point);
          missing = false;
        }
        if (!bucket || bucket.pixel !== pixel) {
          flush();
          bucket = { pixel, first: point, last: point, min: point, max: point };
        } else {
          bucket.last = point;
          if (point[line.key] < bucket.min[line.key]) bucket.min = point;
          if (point[line.key] > bucket.max[line.key]) bucket.max = point;
        }
      }
      flush();
      context.strokeStyle = line.color;
      context.fillStyle = line.color;
      context.lineWidth = 2;
      context.lineJoin = "round";
      context.setLineDash(metrics[line.key].dash || []);
      context.beginPath();
      for (let i = 0; i < points.length; i++) {
        const point = points[i];
        if (i === 0 || gaps.has(point)) context.moveTo(x(point.time), line.y(point[line.key]));
        else context.lineTo(x(point.time), line.y(point[line.key]));
      }
      context.stroke();
      context.setLineDash([]);
      if (points.length) {
        const last = points[points.length - 1];
        context.beginPath(); context.arc(x(last.time), line.y(last[line.key]), 3, 0, Math.PI * 2); context.fill();
      }
    }
    context.restore();
    view = { low, high, start, span, x, left, right, top, bottom, width, height, series };
    updateNavigation({ start, span });
    showHover();
    const values = keys.map((key) => `${metrics[key].label}: ${format(key, latest[key])}`).join(". ");
    const period = `${timestamp.format(start)} to ${timestamp.format(end)}`;
    chart.setAttribute("aria-label", `${keys.map((key) => metrics[key].label).join(", ")} from ${period}. ${following ? "Following latest samples" : "Paused"}. Latest values: ${values}. ${number.format(high - low)} snapshots.`);
  }

  function showHover() {
    const crosshair = byID("crosshair");
    for (const key of Object.keys(metrics)) byID(`point-${key}`).hidden = true;
    if (!pointer || !view || view.low >= view.high || pointer.x < view.left || pointer.x > view.right || pointer.y < view.top || pointer.y > view.bottom) {
      tooltip.hidden = crosshair.hidden = true;
      return;
    }
    const time = view.start + (pointer.x - view.left) / (view.right - view.left) * view.span;
    let index = Math.min(view.high - 1, lowerBound(time, view.low));
    if (index > 0 && at(index).gap_before && time > at(index - 1).time && time < at(index).time) {
      tooltip.hidden = crosshair.hidden = true;
      return;
    }
    if (index > view.low && time - at(index - 1).time < at(index).time - time) index--;
    const sample = at(index);
    const column = view.x(sample.time);
    crosshair.hidden = false;
    crosshair.style.left = `${column}px`;
    crosshair.style.top = `${view.top}px`;
    crosshair.style.height = `${view.bottom - view.top}px`;
    byID("tooltip-time").textContent = timestamp.format(sample.time);
    byID("tooltip-time").dateTime = new Date(sample.time).toISOString();
    for (const key of Object.keys(metrics)) {
      byID(`tooltip-${key}`).hidden = !selected.has(key);
      if (selected.has(key)) byID(`tooltip-value-${key}`).textContent = format(key, sample[key], true);
    }
    for (const line of view.series) {
      const marker = byID(`point-${line.key}`);
      if (sample[line.key] === null) continue;
      marker.hidden = false;
      marker.style.left = `${column}px`;
      marker.style.top = `${line.y(sample[line.key])}px`;
    }
    tooltip.hidden = false;
    const box = tooltip.getBoundingClientRect();
    const side = pointer.x + 14 + box.width <= view.width ? pointer.x + 14 : pointer.x - box.width - 14;
    tooltip.style.left = `${Math.max(4, Math.min(view.width - box.width - 4, side))}px`;
    tooltip.style.top = `${Math.max(4, Math.min(view.height - box.height - 4, pointer.y - box.height - 14))}px`;
  }

  function connect() {
    if (stopped) return;
    const url = new URL("ws", window.location.href);
    url.protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    socket = new WebSocket(url);
    socket.addEventListener("open", () => {
      // Backfill periods when this browser was offline but the server kept
      // collecting, rather than treating a network disconnect as an outage.
      loaded = null;
      historyRequest?.abort();
      pending = null;
      status("Connected", true);
    });
    socket.addEventListener("message", (event) => {
      try { receive(JSON.parse(event.data)); }
      catch { status("Invalid snapshot"); }
    });
    socket.addEventListener("close", () => {
      if (stopped) return;
      status("Reconnecting…");
      retry = setTimeout(connect, 1000);
    });
    socket.addEventListener("error", () => socket.close());
  }

  for (const button of metricButtons) {
    button.addEventListener("click", () => {
      const key = button.dataset.metric;
      if (selected.has(key)) {
        if (selected.size === 1) return;
        selected.delete(key);
      } else {
        for (const other of selected) {
          if (metrics[other].unit !== metrics[key].unit) selected.delete(other);
        }
        selected.add(key);
      }
      for (const other of metricButtons) other.setAttribute("aria-pressed", String(selected.has(other.dataset.metric)));
      requestDraw();
    });
  }
  for (const button of ranges) {
    button.addEventListener("click", () => {
      range = button.dataset.range === "all" ? Infinity : Number(button.dataset.range);
      if (!following) {
        const end = viewport.start + viewport.span;
        const span = range === Infinity ? Math.max(interval, end - oldestTime()) : range;
        viewport = { start: end - span, span };
      }
      for (const other of ranges) other.setAttribute("aria-pressed", String(other === button));
      requestDraw();
    });
  }
  chart.addEventListener("wheel", (event) => {
    if (!view || event.ctrlKey || event.metaKey) return;
    const window = visibleWindow();
    const bounds = historyBounds(window.span);
    if (bounds.end - bounds.start <= window.span) return;
    const delta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
    if (!delta) return;
    const pixels = delta * (event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? view.right - view.left : 1);
    event.preventDefault();
    navigate(window.start + pixels / (view.right - view.left) * window.span, window.span, bounds.end);
  }, { passive: false });
  chart.addEventListener("pointerdown", (event) => {
    if (!view || event.button !== 0) return;
    const bounds = chart.getBoundingClientRect();
    const x = event.clientX - bounds.left, y = event.clientY - bounds.top;
    if (x < view.left || x > view.right || y < view.top || y > view.bottom) return;
    const window = visibleWindow();
    const history = historyBounds(window.span);
    if (history.end - history.start <= window.span) return;
    setFollowing(false);
    pan = { id: event.pointerId, x: event.clientX, end: history.end, ...window };
    pointer = null;
    showHover();
    chart.classList.toggle("panning", true);
    chart.setPointerCapture(event.pointerId);
  });
  chart.addEventListener("pointermove", (event) => {
    if (pan && pan.id === event.pointerId) {
      navigate(pan.start - (event.clientX - pan.x) / (view.right - view.left) * pan.span, pan.span, pan.end);
      return;
    }
    const bounds = chart.getBoundingClientRect();
    pointer = { x: event.clientX - bounds.left, y: event.clientY - bounds.top };
    showHover();
  });
  chart.addEventListener("pointerleave", () => { pointer = null; showHover(); });
  for (const event of ["pointerup", "pointercancel", "lostpointercapture"]) {
    chart.addEventListener(event, () => {
      pan = null;
      pointer = null;
      chart.classList.toggle("panning", false);
      showHover();
    });
  }
  function scrubTo(event) {
    const fraction = Math.max(0, Math.min(1, (event.clientX - scrub.left) / scrub.width));
    navigate(scrub.oldest + fraction * scrub.duration - scrub.offset, scrub.span, scrub.oldest + scrub.duration);
  }
  scrubber.addEventListener("pointerdown", (event) => {
    if (!view || event.button !== 0) return;
    const window = visibleWindow();
    const oldest = oldestTime();
    const duration = Math.max(Date.now(), latest.time) - oldest;
    if (duration <= window.span) return;
    const bounds = scrubber.getBoundingClientRect();
    if (!bounds.width) return;
    const time = oldest + (event.clientX - bounds.left) / bounds.width * duration;
    const offset = time >= window.start && time <= window.start + window.span ? time - window.start : window.span / 2;
    scrub = { id: event.pointerId, oldest, duration, left: bounds.left, width: bounds.width, offset, span: window.span };
    event.preventDefault();
    scrubber.focus({ preventScroll: true });
    scrubber.setPointerCapture(event.pointerId);
    scrubTo(event);
  });
  scrubber.addEventListener("pointermove", (event) => {
    if (scrub && scrub.id === event.pointerId) scrubTo(event);
  });
  for (const event of ["pointerup", "pointercancel", "lostpointercapture"]) {
    scrubber.addEventListener(event, () => { scrub = null; });
  }
  scrubber.addEventListener("keydown", (event) => {
    if (!view) return;
    const window = visibleWindow();
    const bounds = historyBounds(window.span);
    if (bounds.end - bounds.start <= window.span) return;
    const starts = {
      ArrowLeft: window.start - window.span / 10, ArrowRight: window.start + window.span / 10,
      PageUp: window.start - window.span, PageDown: window.start + window.span,
      Home: bounds.start, End: bounds.end - window.span,
    };
    if (!(event.key in starts)) return;
    event.preventDefault();
    navigate(starts[event.key], window.span, bounds.end);
  });
  new ResizeObserver(requestDraw).observe(chart);
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", requestDraw);
  window.addEventListener("pagehide", () => {
    stopped = true;
    clearTimeout(retry);
    historyRequest?.abort();
    pending = null;
    socket?.close();
  });
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) { stopped = false; connect(); }
  });
  connect();
})();

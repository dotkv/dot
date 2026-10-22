const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { test } = require("node:test");
const vm = require("node:vm");

const source = readFileSync(`${__dirname}/app.js`, "utf8");
const html = readFileSync(`${__dirname}/index.html`, "utf8");
const now = Date.parse("2026-10-22T16:57:18.400Z");

function dashboard() {
  const elements = {};
  let currentTime = now;
  class Element {
    constructor() {
      this.hidden = false;
      this.style = { setProperty() {} };
      this.dataset = {};
      this.attributes = {};
      this.listeners = {};
      this.classList = { toggle() {} };
    }
    set id(value) { elements[value] = this; }
    addEventListener(type, listener) { this.listeners[type] = listener; }
    before() {}
    append() {}
    replaceChildren() {}
    focus() {}
    setPointerCapture() {}
    emit(type, data = {}) { this.listeners[type]?.(data); }
    setAttribute(key, value) { this.attributes[key] = value; }
    getBoundingClientRect() { return { left: 0, top: 0, width: 900, height: 330 }; }
  }
  for (const [, id] of html.matchAll(/id="([^"]+)"/g)) elements[id] = new Element();
  const buttons = [...html.matchAll(/data-(metric|range)="([^"]+)" aria-pressed="([^"]+)"/g)].map((match) => {
    const element = new Element();
    element.dataset[match[1]] = match[2];
    element.attributes["aria-pressed"] = match[3];
    return element;
  });
  const labels = [];
  const paths = [];
  const context = new Proxy({
    setTransform() { labels.length = paths.length = 0; },
    fillText(text, x, y) { labels.push({ text, x, y }); },
    measureText(text) { return { width: text.length * 6 }; },
    moveTo(x, y) { paths.push({ move: true, x, y, color: this.strokeStyle }); },
    lineTo(x, y) { paths.push({ move: false, x, y, color: this.strokeStyle }); },
  }, { get: (target, key) => target[key] || (() => {}) });
  elements.chart.getContext = () => context;
  let socket;
  class WebSocket extends Element {
    constructor() { super(); socket = this; }
    close() { this.emit("close"); }
  }
  let scheduled;
  const styles = { "--muted": "gray", "--grid": "silver", "--accent": "blue", "--obsolete": "orange", "--entries": "green", "--update": "purple" };
  const environment = {
    document: {
      getElementById: (id) => { assert.ok(elements[id], `Missing HTML element: ${id}`); return elements[id]; },
      querySelectorAll: (query) => buttons.filter((button) => query.includes("metric") ? button.dataset.metric : button.dataset.range),
      createElement: () => new Element(),
      documentElement: {},
    },
    window: {
      devicePixelRatio: 1,
      location: { href: "http://localhost/nested/dot/", protocol: "http:" },
      matchMedia: () => new Element(), addEventListener() {},
    },
    WebSocket, URL, Intl,
    Date: class extends Date { static now() { return currentTime; } },
    getComputedStyle: () => ({ getPropertyValue: (key) => styles[key] }),
    requestAnimationFrame(callback) { scheduled = callback; return 1; },
    ResizeObserver: class { observe() {} },
    setTimeout() {}, clearTimeout() {},
  };
  vm.runInNewContext(source, environment);
  function flush() { const callback = scheduled; scheduled = null; callback?.(); }
  return {
    elements, labels, paths,
    receive(values = {}) {
      const reading = { ops_per_second: 0, latency_samples: 0, mean_ns: null, p99_ns: null };
      const operations = Object.fromEntries(["get", "set", "delete", "update"].map((op) => [op, reading]));
      socket.emit("message", { data: JSON.stringify({ time: now, interval_ms: 1000, ready: true, disk_bytes: 123_456, obsolete_bytes: 32_000, entry_count: 299, index_mapped_bytes: 8 << 20, stale_shards: 0, operations, ...values }) });
    },
    click(key) {
      buttons.find((button) => button.dataset.metric === key || button.dataset.range === key).emit("click");
      flush();
    },
    pressed: (key) => buttons.find((button) => button.dataset.metric === key).attributes["aria-pressed"],
    disconnect: () => socket.emit("close"),
    advance: (ms) => { currentTime += ms; },
    flush,
  };
}

const yLabels = (ui) => ui.labels.filter((label) => label.y !== 321).map((label) => label.text);

test("value ticks use round numbers in a consistent unit and time ticks align to the clock", () => {
  const ui = dashboard();
  ui.receive(); ui.flush();
  assert.deepEqual(yLabels(ui), ["0 KiB", "50 KiB", "100 KiB", "150 KiB"]);
  const clock = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  const timeLabels = ui.labels.filter((label) => label.y === 321).map((label) => label.text);
  assert.deepEqual(timeLabels, ["16:56:20", "16:56:30", "16:56:40", "16:56:50", "16:57:00", "16:57:10"].map((time) => clock.format(Date.parse(`2026-10-22T${time}Z`))));
  ui.click("entry_count");
  assert.deepEqual(yLabels(ui), ["0", "100", "200", "300"]);
});

test("changing units replaces incompatible selections and retains at least one metric", () => {
  const ui = dashboard();
  ui.receive(); ui.flush();
  ui.click("obsolete_bytes");
  assert.equal(ui.pressed("disk_bytes"), "true");
  assert.equal(ui.pressed("obsolete_bytes"), "true");
  ui.click("entry_count");
  assert.equal(ui.pressed("entry_count"), "true");
  assert.equal(ui.pressed("disk_bytes"), "false");
  assert.equal(ui.pressed("obsolete_bytes"), "false");
  assert.deepEqual(yLabels(ui), ["0", "100", "200", "300"]);
  assert.doesNotMatch(ui.elements.chart.attributes["aria-label"], /On-disk size|Obsolete bytes|scaled independently/);
  ui.click("entry_count");
  assert.equal(ui.pressed("entry_count"), "true");
  ui.click("disk_bytes");
  assert.equal(ui.pressed("entry_count"), "false");
  assert.equal(ui.pressed("disk_bytes"), "true");
  assert.equal(ui.pressed("obsolete_bytes"), "false");
  assert.deepEqual(yLabels(ui), ["0 KiB", "50 KiB", "100 KiB", "150 KiB"]);
  ui.click("obsolete_bytes");
  ui.click("disk_bytes");
  assert.equal(ui.pressed("disk_bytes"), "false");
  assert.equal(ui.pressed("obsolete_bytes"), "true");
  ui.click("obsolete_bytes");
  assert.equal(ui.pressed("obsolete_bytes"), "true");
});

test("same-unit selections share a visible axis covering every series and historical peak", () => {
  const ui = dashboard();
  ui.receive({ time: now - 30_000, disk_bytes: 256 * 1024, obsolete_bytes: 512 * 1024 });
  ui.receive({ disk_bytes: 16 * 1024, obsolete_bytes: 16 * 1024 });
  ui.click("obsolete_bytes");
  assert.deepEqual(yLabels(ui), ["0 KiB", "200 KiB", "400 KiB", "600 KiB"]);
  assert.doesNotMatch(ui.elements.chart.attributes["aria-label"], /Each metric is scaled independently/);
  const disk = ui.paths.filter((point) => point.color === "blue");
  const obsolete = ui.paths.filter((point) => point.color === "orange");
  assert.equal(disk.length, 2);
  assert.equal(obsolete.length, 2);
  assert.equal(disk[1].y, obsolete[1].y, "Equal byte values must use the same height");
  assert.ok(Math.abs((296 - obsolete[0].y) - 2 * (296 - disk[0].y)) < 1e-9);
  for (const point of [...disk, ...obsolete]) assert.ok(point.y >= 20 && point.y <= 296);

  ui.elements.chart.emit("pointermove", { clientX: 880, clientY: 100 });
  assert.equal(ui.elements["point-disk_bytes"].style.top, ui.elements["point-obsolete_bytes"].style.top);
  ui.click("entry_count");
  assert.deepEqual(yLabels(ui), ["0", "100", "200", "300"]);
  ui.click("obsolete_bytes");
  ui.click("disk_bytes");
  assert.deepEqual(yLabels(ui), ["0 KiB", "200 KiB", "400 KiB", "600 KiB"]);
  ui.click("obsolete_bytes");
  assert.deepEqual(yLabels(ui), ["0 KiB", "100 KiB", "200 KiB", "300 KiB"]);
});

test("shared axes fit the preceding sample drawn across the visible range boundary", () => {
  const ui = dashboard();
  ui.receive({ time: now - 60_500, disk_bytes: 128 * 1024, obsolete_bytes: 768 * 1024 });
  ui.receive({ time: now - 59_500, disk_bytes: 32 * 1024, obsolete_bytes: 16 * 1024 });
  ui.click("obsolete_bytes");
  assert.deepEqual(yLabels(ui), ["0 KiB", "200 KiB", "400 KiB", "600 KiB", "800 KiB"]);
  const points = ui.paths.filter((point) => point.color === "blue" || point.color === "orange");
  assert.equal(points.length, 4);
  for (const point of points) assert.ok(point.y >= 20 && point.y <= 296);
});

test("same-unit selections with zero values keep a finite axis and visible points", () => {
  const ui = dashboard();
  ui.receive({ disk_bytes: 0, obsolete_bytes: 0 });
  ui.click("obsolete_bytes");
  assert.deepEqual(yLabels(ui), ["0 B", "1 B"]);
  const points = ui.paths.filter((point) => point.color === "blue" || point.color === "orange");
  assert.equal(points.length, 2);
  for (const point of points) assert.equal(point.y, 296);
});

test("hover shows the nearest sample's timestamp and exact values for selected metrics", () => {
  const ui = dashboard();
  ui.receive({ time: now - 30_000, disk_bytes: 123_457, obsolete_bytes: 32_001, entry_count: 217 });
  ui.receive(); ui.flush();
  ui.click("obsolete_bytes");
  ui.elements.chart.emit("pointermove", { clientX: 450, clientY: 100 });
  assert.equal(ui.elements.tooltip.hidden, false);
  assert.equal(ui.elements["tooltip-time"].dateTime, new Date(now - 30_000).toISOString());
  assert.match(ui.elements["tooltip-value-disk_bytes"].textContent, /123,457 bytes/);
  assert.match(ui.elements["tooltip-value-obsolete_bytes"].textContent, /32,001 bytes/);
  assert.equal(ui.elements["tooltip-entry_count"].hidden, true);
  assert.equal(ui.elements["point-obsolete_bytes"].hidden, false);
  assert.equal(ui.elements["point-entry_count"].hidden, true);
  ui.click("entry_count");
  assert.equal(ui.elements["tooltip-value-entry_count"].textContent, "217");
  assert.equal(ui.elements["tooltip-disk_bytes"].hidden, true);
  assert.equal(ui.elements["tooltip-obsolete_bytes"].hidden, true);
  assert.equal(ui.elements["point-disk_bytes"].hidden, true);
  assert.equal(ui.elements["point-obsolete_bytes"].hidden, true);
  assert.equal(ui.elements["point-entry_count"].hidden, false);
  ui.elements.chart.emit("pointerleave");
  assert.equal(ui.elements.tooltip.hidden, true);
  assert.equal(ui.elements.crosshair.hidden, true);
});

test("history evicts older samples after 20,000 snapshots and ignores duplicates on reconnect", () => {
  const ui = dashboard();
  const total = 50_000;
  for (let i = 0; i < total; i++) ui.receive({ time: now - (total - 1 - i) * 1000, entry_count: i });
  ui.receive({ entry_count: total - 1 });
  ui.click("all");
  assert.match(ui.elements.chart.attributes["aria-label"], /20,000 snapshots/);
  ui.elements.chart.emit("pointermove", { clientX: 86, clientY: 100 });
  assert.equal(ui.elements["tooltip-time"].dateTime, new Date(now - 19_999_000).toISOString());
  // Resampling must still connect the line when neighboring retained points
  // are farther apart than the original sampling cadence.
  const connections = ui.paths.filter((point) => !point.move).length;
  assert.ok(connections > 100, `Only ${connections} line segments survived pixel resampling`);
});

test("removed notices stay absent while reconnecting and scan errors retain their own notice", () => {
  const ui = dashboard();
  ui.receive(); ui.flush();
  ui.disconnect();
  assert.equal(ui.elements.status.textContent, "Reconnecting…");
  assert.equal(ui.elements.notice.hidden, true);
  ui.receive({ disk_stale: true });
  assert.match(ui.elements.notice.textContent, /Disk usage is unavailable/);
  assert.doesNotMatch(html, /History stays in this tab|id="coverage"|excluding metadata/);
});

const pointerEvent = (x, y = 100) => ({ button: 0, pointerId: 1, clientX: x, clientY: y, preventDefault() {} });
const keyEvent = (key) => ({ key, preventDefault() {} });
const windowDescription = (ui) => ui.elements.scrubber.attributes["aria-valuetext"];
const isFollowing = (ui) => ui.elements.chart.attributes["aria-label"].includes("Following latest samples");

function loadHistory(ui, seconds = 180) {
  for (let i = seconds; i >= 0; i--) ui.receive({ time: now - i * 1000, disk_bytes: (i + 1) * 1024 });
  ui.flush();
}

test("pausing holds the drawn time window, scale, and hover while new readings arrive", () => {
  const ui = dashboard();
  loadHistory(ui);
  ui.elements.scrubber.emit("keydown", keyEvent("Home")); ui.flush();
  const labels = structuredClone(ui.labels);
  const paths = structuredClone(ui.paths);
  const description = windowDescription(ui);
  ui.advance(500);
  assert.equal(isFollowing(ui), false);
  assert.equal(windowDescription(ui), description, "Pausing must retain the already drawn window");
  ui.advance(9500);
  ui.receive({ time: now + 10_000, disk_bytes: 1024 ** 4 }); ui.flush();
  assert.deepEqual(ui.labels, labels, "Off-screen peaks must not rescale a paused chart");
  assert.deepEqual(ui.paths, paths, "New samples must not slide existing points");
  ui.elements.chart.emit("pointermove", pointerEvent(880));
  assert.equal(ui.elements["tooltip-time"].dateTime, new Date(now - 120_000).toISOString());
  assert.match(ui.elements.chart.attributes["aria-label"], /Paused.*61 snapshots/);
  ui.elements.scrubber.emit("keydown", keyEvent("End")); ui.flush();
  assert.equal(isFollowing(ui), true);
  assert.notEqual(windowDescription(ui), description);
  assert.notDeepEqual(yLabels(ui), labels.filter((label) => label.y !== 321).map((label) => label.text));
});

test("scrolling and dragging pan a fixed-size window within loaded history", () => {
  const ui = dashboard();
  loadHistory(ui);
  let prevented = false;
  ui.elements.chart.emit("wheel", { deltaX: 0, deltaY: -397, deltaMode: 0, preventDefault() { prevented = true; } });
  ui.flush();
  assert(prevented);
  assert.equal(isFollowing(ui), false);
  assert.equal(Number(ui.elements.scrubber.attributes["aria-valuenow"]), 90_000);
  assert.equal(Number(ui.elements.scrubber.attributes["aria-valuemax"]), 120_000);
  assert.ok(Math.abs(parseFloat(ui.elements["scrubber-window"].style.width) - 100 / 3) < 1e-8);
  ui.elements.chart.emit("pointerdown", pointerEvent(300));
  ui.elements.chart.emit("pointermove", pointerEvent(697)); ui.flush();
  assert.equal(Number(ui.elements.scrubber.attributes["aria-valuenow"]), 60_000);
  ui.elements.chart.emit("pointerup");
  const description = windowDescription(ui);
  ui.elements.chart.emit("pointermove", pointerEvent(800)); ui.flush();
  assert.equal(windowDescription(ui), description, "Releasing the pointer must finish the drag");
  ui.elements.chart.emit("wheel", { deltaX: -1e6, deltaY: 0, deltaMode: 0, preventDefault() {} }); ui.flush();
  assert.equal(Number(ui.elements.scrubber.attributes["aria-valuenow"]), 0);
  ui.elements.chart.emit("wheel", { deltaX: 1e6, deltaY: 0, deltaMode: 0, preventDefault() {} }); ui.flush();
  assert.equal(Number(ui.elements.scrubber.attributes["aria-valuenow"]), 120_000);
  assert.equal(isFollowing(ui), true, "Reaching the newest data must resume following");
  const liveWindow = windowDescription(ui);
  ui.advance(1000);
  ui.receive({ time: now + 1000 }); ui.flush();
  assert.notEqual(windowDescription(ui), liveWindow, "New samples must advance the window after scrolling to the end");
});

test("scrubber clicks, thumb drags, and keyboard controls navigate the loaded timeline", () => {
  const ui = dashboard();
  loadHistory(ui);
  ui.elements.scrubber.emit("pointerdown", pointerEvent(450)); ui.flush();
  assert.equal(Number(ui.elements.scrubber.attributes["aria-valuenow"]), 60_000);
  ui.elements.scrubber.emit("pointerup");
  ui.elements.scrubber.emit("pointerdown", pointerEvent(450));
  ui.elements.scrubber.emit("pointermove", pointerEvent(600)); ui.flush();
  assert.equal(Number(ui.elements.scrubber.attributes["aria-valuenow"]), 90_000);
  ui.elements.scrubber.emit("pointercancel");
  ui.elements.scrubber.emit("pointermove", pointerEvent(0)); ui.flush();
  assert.equal(Number(ui.elements.scrubber.attributes["aria-valuenow"]), 90_000);
  for (const [key, expected] of [["Home", 0], ["ArrowRight", 6000], ["PageDown", 66_000], ["ArrowLeft", 60_000], ["PageUp", 0], ["End", 120_000]]) {
    ui.elements.scrubber.emit("keydown", keyEvent(key)); ui.flush();
    assert.equal(Number(ui.elements.scrubber.attributes["aria-valuenow"]), expected, key);
    assert.equal(isFollowing(ui), key === "End");
  }
});

test("chart and scrubber drags resume following at the live edge even as data arrives", () => {
  for (const target of ["chart", "scrubber"]) {
    const ui = dashboard();
    loadHistory(ui);
    ui.elements.scrubber.emit("keydown", keyEvent("Home")); ui.flush();
    ui.elements[target].emit("pointerdown", pointerEvent(target === "chart" ? 800 : 300));
    ui.advance(1000);
    ui.receive({ time: now + 1000 }); ui.flush();
    ui.elements[target].emit("pointermove", pointerEvent(target === "chart" ? -788 : 900)); ui.flush();
    ui.elements[target].emit("pointerup");
    assert.equal(isFollowing(ui), true, target);
    assert.equal(Number(ui.elements.scrubber.attributes["aria-valuenow"]), 121_000);
  }
});

test("clicking the right end of the scrubber resumes following", () => {
  const ui = dashboard();
  loadHistory(ui);
  ui.elements.scrubber.emit("keydown", keyEvent("Home")); ui.flush();
  ui.elements.scrubber.emit("pointerdown", pointerEvent(900)); ui.flush();
  ui.elements.scrubber.emit("pointerup");
  assert.equal(isFollowing(ui), true);
  assert.equal(Number(ui.elements.scrubber.attributes["aria-valuenow"]), 120_000);
});

test("All and range changes stay paused, including when retained data is evicted", () => {
  const ui = dashboard();
  loadHistory(ui);
  ui.click("all");
  assert.equal(ui.elements.scrubber.attributes["aria-disabled"], "true");
  ui.click("60000");
  ui.elements.scrubber.emit("keydown", keyEvent("Home")); ui.flush();
  ui.click("all");
  const description = windowDescription(ui);
  ui.advance(10_000);
  ui.receive({ time: now + 10_000, disk_bytes: 1024 ** 4 }); ui.flush();
  assert.equal(windowDescription(ui), description);
  ui.click("60000");
  assert.equal(isFollowing(ui), false);
  assert.match(ui.elements.chart.attributes["aria-label"], /61 snapshots/);
  const minute = windowDescription(ui);
  // Eviction must not push a paused time window forward or select newer samples.
  for (let i = 1; i <= 20_000; i++) ui.receive({ time: now + 10_000 + i * 1000 });
  ui.flush();
  assert.equal(windowDescription(ui), minute);
  assert.match(ui.elements.chart.attributes["aria-label"], /0 snapshots/);
  ui.elements.chart.emit("pointermove", pointerEvent(880));
  assert.equal(ui.elements.tooltip.hidden, true);
  ui.elements.scrubber.emit("keydown", keyEvent("End")); ui.flush();
  assert.match(ui.elements.chart.attributes["aria-label"], /Following latest samples.*61 snapshots/);
});

test("short histories do not intercept page scrolling or browser zoom gestures", () => {
  const ui = dashboard();
  ui.receive(); ui.flush();
  const unexpected = () => assert.fail("Should allow normal scrolling");
  assert.equal(ui.elements.scrubber.attributes["aria-disabled"], "true");
  assert.equal(ui.elements.scrubber.tabIndex, -1);
  ui.elements.chart.emit("wheel", { deltaX: 0, deltaY: -100, preventDefault: unexpected });
  loadHistory(ui);
  for (const modifier of ["ctrlKey", "metaKey"]) {
    ui.elements.chart.emit("wheel", { deltaX: 0, deltaY: -100, [modifier]: true, preventDefault: unexpected });
  }
  assert.equal(isFollowing(ui), true);
});

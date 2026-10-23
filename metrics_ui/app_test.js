"use strict";

const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

// Exercise the dashboard's WebSocket, selection, and canvas behavior without
// requiring a browser or adding a JavaScript dependency to the Go library.
function dashboard() {
  const html = fs.readFileSync(path.join(__dirname, "index.html"), "utf8");
  const elements = new Map();
  const buttons = [];
  const frames = [];
  const drawing = [];
  class Element {
    constructor() {
      this.listeners = {};
      this.dataset = {};
      this.attributes = {};
      this.hidden = false;
      this.textContent = "";
      this.style = { setProperty() {} };
      this.classList = { toggle() {} };
    }
    set id(value) { elements.set(value, this); this.elementID = value; }
    get id() { return this.elementID; }
    addEventListener(event, callback) { this.listeners[event] = callback; }
    setAttribute(name, value) { this.attributes[name] = value; }
    getBoundingClientRect() { return { width: 1000, height: 330, left: 0, top: 0 }; }
    before() {}
    append() {}
    replaceChildren() {}
    click() { this.listeners.click(); }
  }
  for (const [, attributes] of html.matchAll(/<[a-z]+\b([^>]*)>/g)) {
    const id = attributes.match(/\bid="([^"]+)"/);
    const metric = attributes.match(/\bdata-metric="([^"]+)"/);
    const range = attributes.match(/\bdata-range="([^"]+)"/);
    if (!id && !metric && !range) continue;
    const element = new Element();
    if (id) element.id = id[1];
    if (metric) element.dataset.metric = metric[1];
    if (range) element.dataset.range = range[1];
    if (metric || range) buttons.push(element);
  }
  const context = new Proxy({}, {
    get(_, name) {
      if (name === "measureText") return (text) => ({ width: text.length * 6 });
      return (...args) => {
        for (const value of args) {
          if (typeof value === "number") assert(Number.isFinite(value), `${String(name)} received ${value}`);
        }
        drawing.push([name, ...args]);
      };
    },
  });
  elements.get("chart").getContext = () => context;
  let socket;
  const sandbox = {
    document: {
      getElementById: (id) => { assert(elements.has(id), `missing element ${id}`); return elements.get(id); },
      querySelectorAll: (selector) => buttons.filter((button) => selector === "[data-metric]" ? button.dataset.metric : button.dataset.range),
      createElement: () => new Element(),
      documentElement: {},
    },
    window: {
      location: { href: "http://localhost/nested/dot/metrics/", protocol: "http:" },
      devicePixelRatio: 1,
      matchMedia: () => ({ addEventListener() {} }),
      addEventListener() {},
    },
    WebSocket: class {
      constructor(url) { this.url = url; this.listeners = {}; socket = this; }
      addEventListener(event, callback) { this.listeners[event] = callback; }
      close() {}
    },
    ResizeObserver: class { observe() {} },
    requestAnimationFrame(callback) { frames.push(callback); return frames.length; },
    getComputedStyle: () => ({ getPropertyValue: () => "#2563eb" }),
    URL,
    setTimeout() {},
    clearTimeout() {},
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, "app.js"), "utf8"), sandbox);
  return {
    elements, buttons, socket, drawing, html,
    receive(sample) { socket.listeners.message({ data: JSON.stringify(sample) }); },
    draw() { while (frames.length) frames.shift()(); },
    button(key) { return buttons.find((button) => button.dataset.metric === key); },
  };
}

function snapshot(time = Date.now()) {
  const reading = { count: 25, ops_per_second: 2.5, latency_samples: 25, mean_ns: 1500, p99_ns: 2047 };
  return {
    time, interval_ms: 1000, disk_bytes: 4096, obsolete_bytes: 512, entry_count: 10,
    index_mapped_bytes: 8 << 20,
    ready: true, disk_stale: false, latency_enabled: true, operation_window_ms: 10_000,
    operations: Object.fromEntries(["get", "set", "delete", "update", "scan"].map((op) => [op, { ...reading }])),
  };
}

test("operation values, prefix-safe WebSocket, and removal of graph footer notes", () => {
  const ui = dashboard();
  assert.equal(ui.socket.url.href, "ws://localhost/nested/dot/metrics/ws");
  const sample = snapshot();
  sample.operations.get.ops_per_second = 0.24;
  Object.assign(sample.operations.set, { ops_per_second: 4400, mean_ns: 2000 });
  Object.assign(sample.operations.delete, { ops_per_second: 1000, mean_ns: 1_000_000 });
  Object.assign(sample.operations.update, { ops_per_second: 1_000_000, mean_ns: 0 });
  Object.assign(sample.operations.scan, { ops_per_second: 0.5, mean_ns: 2_500_000, p99_ns: 4_194_303 });
  ui.receive(sample);
  ui.draw();
  assert.equal(ui.elements.get("status").textContent, "Connected");
  assert.equal(ui.elements.get("value-get_ops").textContent, "0.24");
  assert.equal(ui.elements.get("value-set_ops").textContent, "4.4K");
  assert.equal(ui.elements.get("value-delete_ops").textContent, "1.0K");
  assert.equal(ui.elements.get("value-update_ops").textContent, "1.0M");
  assert.equal(ui.elements.get("value-scan_ops").textContent, "0.50");
  assert.equal(ui.elements.get("value-get_mean_ns").textContent, "1.50 µs");
  assert.equal(ui.elements.get("value-set_mean_ns").textContent, "2.00 µs");
  assert.equal(ui.elements.get("value-delete_mean_ns").textContent, "1.00 ms");
  assert.equal(ui.elements.get("value-update_mean_ns").textContent, "0.00 ns");
  assert.equal(ui.elements.get("value-get_p99_ns").textContent, "≈ 2.05 µs");
  assert.equal(ui.elements.get("value-scan_mean_ns").textContent, "2.50 ms");
  assert.equal(ui.elements.get("value-scan_p99_ns").textContent, "≈ 4.19 ms");
  assert(!ui.html.includes("history-footer"));
  assert(!ui.html.includes('id="cadence"'));
  assert(ui.html.indexOf('id="notice"') < ui.html.indexOf('class="history"'));
});

test("table selections graph compatible series, with fractional rates and latency units", () => {
  const ui = dashboard();
  ui.receive(snapshot());
  ui.button("get_ops").click();
  ui.button("set_ops").click();
  ui.draw();
  assert.equal(ui.button("disk_bytes").attributes["aria-pressed"], "false");
  assert.equal(ui.button("get_ops").attributes["aria-pressed"], "true");
  assert.equal(ui.button("set_ops").attributes["aria-pressed"], "true");
  assert(ui.drawing.some(([call, label]) => call === "fillText" && label === "2.00"));
  assert(!ui.drawing.some(([call, label]) => call === "fillText" && label.includes("ops/sec")));
  assert.match(ui.elements.get("chart").attributes["aria-label"], /Get ops\/sec: 2\.50\. /);
  ui.elements.get("chart").listeners.pointermove({ clientX: 980, clientY: 100 });
  assert.equal(ui.elements.get("tooltip-value-get_ops").textContent, "2.50");
  ui.button("get_mean_ns").click();
  ui.button("get_p99_ns").click();
  ui.draw();
  assert.equal(ui.button("get_ops").attributes["aria-pressed"], "false");
  assert.equal(ui.button("get_mean_ns").attributes["aria-pressed"], "true");
  assert.equal(ui.button("get_p99_ns").attributes["aria-pressed"], "true");
  assert(ui.drawing.some(([call, dash]) => call === "setLineDash" && dash.length === 2));
  assert(ui.drawing.some(([call, label]) => call === "fillText" && label.endsWith(" µs")));
  assert.equal(ui.elements.get("tooltip-value-get_mean_ns").textContent, "1.50 µs (1,500.00 ns)");
});

test("empty latency is unavailable, produces a graph gap, and works before storage is ready", () => {
  const ui = dashboard();
  const first = snapshot(Date.now() - 2000);
  ui.receive(first);
  const empty = snapshot(first.time + 1000);
  empty.ready = false;
  empty.disk_stale = true;
  for (const metric of Object.values(empty.operations)) {
    Object.assign(metric, { count: 0, ops_per_second: 0, latency_samples: 0, mean_ns: null, p99_ns: null });
  }
  ui.receive(empty);
  assert.equal(ui.elements.get("value-get_mean_ns").textContent, "—");
  assert.equal(ui.elements.get("value-get_p99_ns").textContent, "—");
  assert.equal(ui.elements.get("value-get_ops").textContent, "0.00");
  assert.equal(ui.elements.get("value-scan_mean_ns").textContent, "—");
  assert.equal(ui.elements.get("value-scan_p99_ns").textContent, "—");
  assert.equal(ui.elements.get("value-scan_ops").textContent, "0.00");
  ui.draw();
  assert.match(ui.elements.get("chart").attributes["aria-label"], /On-disk size: —/);
  ui.button("get_mean_ns").click();
  ui.draw();
  ui.receive(snapshot(first.time + 2000));
  ui.draw();
  assert.equal(ui.elements.get("status").textContent, "Connected");
});

test("counts-only collection shows latency as unavailable", () => {
  const ui = dashboard();
  const counts = snapshot();
  counts.latency_enabled = false;
  counts.operation_window_ms = 500;
  for (const metric of Object.values(counts.operations)) {
    Object.assign(metric, { latency_samples: 0, mean_ns: null, p99_ns: null });
  }
  ui.receive(counts);
  ui.draw();
  assert.equal(ui.elements.get("value-update_mean_ns").textContent, "—");
  assert.equal(ui.elements.get("value-scan_mean_ns").textContent, "—");
  assert.equal(ui.elements.get("value-scan_p99_ns").textContent, "—");
});

test("Scan throughput and latency graph alongside operations with matching units", () => {
  const ui = dashboard();
  ui.receive(snapshot());
  ui.button("scan_ops").click();
  ui.button("get_ops").click();
  ui.draw();
  assert.equal(ui.button("scan_ops").attributes["aria-pressed"], "true");
  assert.equal(ui.button("get_ops").attributes["aria-pressed"], "true");
  assert.match(ui.elements.get("chart").attributes["aria-label"], /Scan ops\/sec: 2\.50/);
  ui.button("scan_mean_ns").click();
  ui.button("scan_p99_ns").click();
  ui.draw();
  assert.equal(ui.button("scan_ops").attributes["aria-pressed"], "false");
  assert.equal(ui.button("get_ops").attributes["aria-pressed"], "false");
  assert.match(ui.elements.get("chart").attributes["aria-label"], /Scan mean latency: 1\.50 µs/);
  ui.elements.get("chart").listeners.pointermove({ clientX: 980, clientY: 100 });
  assert.equal(ui.elements.get("tooltip-value-scan_mean_ns").textContent, "1.50 µs (1,500.00 ns)");
  assert.equal(ui.elements.get("tooltip-value-scan_p99_ns").textContent, "≈ 2.05 µs (2,047.00 ns)");
  assert.equal(ui.elements.get("point-scan_mean_ns").hidden, false);
});

test("index mmap shares the byte scale and exposes its exact mapping size", () => {
  const ui = dashboard();
  ui.receive(snapshot());
  ui.button("index_mapped_bytes").click();
  ui.draw();
  assert.equal(ui.button("disk_bytes").attributes["aria-pressed"], "true");
  assert.equal(ui.button("index_mapped_bytes").attributes["aria-pressed"], "true");
  assert.match(ui.elements.get("chart").attributes["aria-label"], /Index mmap: 8 MiB/);
  const chart = ui.elements.get("chart");
  chart.listeners.pointermove({ clientX: 980, clientY: 100 });
  assert.equal(ui.elements.get("tooltip-value-index_mapped_bytes").textContent, "8 MiB (8,388,608 bytes)");
  assert.equal(ui.elements.get("point-index_mapped_bytes").hidden, false);

  ui.button("entry_count").click();
  ui.draw();
  assert.equal(ui.button("index_mapped_bytes").attributes["aria-pressed"], "false");
  assert.equal(ui.elements.get("point-index_mapped_bytes").hidden, true);
});

test("index mmap waits for complete storage readings and recovers after gaps", () => {
  const ui = dashboard();
  const first = snapshot(Date.now() - 2000);
  first.ready = false;
  ui.receive(first);
  ui.button("index_mapped_bytes").click();
  ui.draw();
  assert.match(ui.elements.get("chart").attributes["aria-label"], /Index mmap: —/);
  ui.elements.get("chart").listeners.pointermove({ clientX: 960, clientY: 100 });
  assert.equal(ui.elements.get("point-index_mapped_bytes").hidden, true);

  const next = snapshot(first.time + 1000);
  ui.receive(next);
  ui.draw();
  assert.match(ui.elements.get("chart").attributes["aria-label"], /Index mmap: 8 MiB/);
});

"use strict";

const menu = document.querySelector(".menu-toggle");
const navigation = document.querySelector("#site-nav");
const mobile = window.matchMedia("(max-width: 760px)");

function closeMenu() {
  menu.setAttribute("aria-expanded", "false");
  navigation.classList.remove("is-open");
}

function syncMenu() {
  menu.hidden = !mobile.matches;
  closeMenu();
}

if (menu && navigation) {
  document.documentElement.classList.add("nav-enhanced");
  syncMenu();
  mobile.addEventListener("change", syncMenu);
  menu.addEventListener("click", () => {
    const expanded = menu.getAttribute("aria-expanded") !== "true";
    menu.setAttribute("aria-expanded", String(expanded));
    navigation.classList.toggle("is-open", expanded);
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && menu.getAttribute("aria-expanded") === "true") {
      closeMenu();
      menu.focus();
    }
  });
}

if (navigator.clipboard?.writeText) {
  const status = document.querySelector("#copy-status");
  document.querySelectorAll("pre > code").forEach((code) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "copy-button";
    button.textContent = "Copy";
    button.setAttribute("aria-label", "Copy code to clipboard");
    code.parentElement.append(button);
    button.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(code.textContent);
        button.textContent = "Copied";
        status.textContent = "Code copied to clipboard.";
      } catch {
        button.textContent = "Select code";
        const range = document.createRange();
        range.selectNodeContents(code);
        const selection = window.getSelection();
        selection.removeAllRanges();
        selection.addRange(range);
        status.textContent = "Clipboard unavailable. Code selected; use your copy shortcut.";
      }
      window.setTimeout(() => {
        button.textContent = "Copy";
        status.textContent = "";
      }, 2500);
    });
  });
}

const sections = Array.from(document.querySelectorAll('.toc a[href^="#"]'), (link) => ({
  link,
  heading: document.getElementById(link.hash.slice(1)),
})).filter(({ heading }) => heading);

if (sections.length) {
  let currentLink;
  let scheduled = false;

  function updateSection() {
    scheduled = false;
    const readingLine = parseFloat(getComputedStyle(document.documentElement).scrollPaddingTop) + 1;
    let current = sections[0];
    for (const section of sections) {
      if (section.heading.getBoundingClientRect().top > readingLine) break;
      current = section;
    }
    if (window.scrollY > 0 && window.scrollY + window.innerHeight >= document.documentElement.scrollHeight - 2) {
      current = sections[sections.length - 1];
    }
    if (current.link !== currentLink) {
      currentLink?.removeAttribute("aria-current");
      current.link.setAttribute("aria-current", "location");
      currentLink = current.link;
    }
  }

  function scheduleSection() {
    if (scheduled) return;
    scheduled = true;
    window.requestAnimationFrame(updateSection);
  }

  window.addEventListener("scroll", scheduleSection, { passive: true });
  window.addEventListener("resize", scheduleSection);
  window.addEventListener("hashchange", scheduleSection);
  window.addEventListener("load", scheduleSection);
  updateSection();
}

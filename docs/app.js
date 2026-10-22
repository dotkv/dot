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
    code.parentElement.classList.add("has-copy");
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

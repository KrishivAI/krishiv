// krishiv-live.js — tiny vendored replacement for the narrow slice of htmx
// this ops console actually used (periodic fragment polling + a theme toggle).
//
// No external dependencies, no CDN, CSP-friendly (`script-src 'self'`).
//
// Behavior:
//   * Any element with `id="live-region"` and a `data-live-interval` (ms)
//     attribute is refreshed on that interval. We re-fetch the current page,
//     parse it, and swap ONLY the `#live-region` subtree — so the nav, page
//     heading, breadcrumb, scroll position and focus outside the region are
//     preserved (unlike the old full-`<main>` swap).
//   * A `.refresh-note` element (if present) is shown while a refresh is in
//     flight, mirroring the old hx-indicator.
//   * Any element with `data-theme-toggle` flips the document color-scheme.
(function () {
  "use strict";

  function applyStoredTheme() {
    try {
      var t = localStorage.getItem("krishiv-theme");
      if (t === "dark" || t === "light") {
        document.documentElement.style.colorScheme = t;
      }
    } catch (e) {
      /* localStorage may be unavailable; ignore */
    }
  }

  function toggleTheme() {
    var cur = document.documentElement.style.colorScheme;
    var next = cur === "dark" ? "light" : "dark";
    document.documentElement.style.colorScheme = next;
    try {
      localStorage.setItem("krishiv-theme", next);
    } catch (e) {
      /* ignore */
    }
  }

  function setIndicator(visible) {
    var note = document.querySelector(".refresh-note");
    if (note) {
      note.hidden = !visible;
    }
  }

  async function refreshLiveRegion() {
    var region = document.getElementById("live-region");
    if (!region) {
      return;
    }
    setIndicator(true);
    try {
      var resp = await fetch(window.location.href, {
        headers: { "X-Requested-With": "krishiv-live" },
        credentials: "same-origin",
      });
      if (!resp.ok) {
        return;
      }
      var text = await resp.text();
      var doc = new DOMParser().parseFromString(text, "text/html");
      var fresh = doc.getElementById("live-region");
      // Only swap if the region is still present after the await.
      var current = document.getElementById("live-region");
      if (fresh && current) {
        current.innerHTML = fresh.innerHTML;
      }
    } catch (e) {
      /* transient network/parse error — keep the stale view, try again next tick */
    } finally {
      setIndicator(false);
    }
  }

  function startPolling() {
    var region = document.getElementById("live-region");
    if (!region) {
      return;
    }
    var interval = parseInt(region.getAttribute("data-live-interval"), 10);
    if (!interval || interval < 1000) {
      return;
    }
    setInterval(refreshLiveRegion, interval);
  }

  function init() {
    applyStoredTheme();
    document.addEventListener("click", function (evt) {
      var el = evt.target.closest("[data-theme-toggle]");
      if (el) {
        evt.preventDefault();
        toggleTheme();
      }
    });
    startPolling();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

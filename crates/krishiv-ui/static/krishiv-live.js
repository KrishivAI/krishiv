// krishiv-live.js — vendored live-refresh + theme toggle + sidebar drawer.
//
// No external dependencies, no CDN, CSP-friendly (`script-src 'self'`).
(function () {
  "use strict";

  function applyStoredTheme() {
    try {
      var t = localStorage.getItem("krishiv-theme");
      if (t === "dark" || t === "light") {
        document.documentElement.style.colorScheme = t;
      }
    } catch (e) { /* ignore */ }
  }

  function toggleTheme() {
    var cur = document.documentElement.style.colorScheme;
    var next = cur === "dark" ? "light" : "dark";
    document.documentElement.style.colorScheme = next;
    try {
      localStorage.setItem("krishiv-theme", next);
    } catch (e) { /* ignore */ }
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
      var headers = window.krishivAuthHeaders
        ? window.krishivAuthHeaders({ "X-Requested-With": "krishiv-live" })
        : { "X-Requested-With": "krishiv-live" };
      var resp = await fetch(window.location.href, {
        headers: headers,
        credentials: "same-origin",
      });
      if (!resp.ok) {
        return;
      }
      var text = await resp.text();
      var doc = new DOMParser().parseFromString(text, "text/html");
      var fresh = doc.getElementById("live-region");
      var current = document.getElementById("live-region");
      if (fresh && current) {
        current.innerHTML = fresh.innerHTML;
      }
    } catch (e) {
      /* transient error — keep stale view */
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

  // ── Sidebar / mobile drawer ────────────────────────────────────────

  function openSidebar() {
    var sidebar = document.querySelector("[data-sidebar]");
    var overlay = document.querySelector("[data-sidebar-overlay]");
    if (sidebar) sidebar.classList.add("open");
    if (overlay) overlay.classList.add("active");
  }

  function closeSidebar() {
    var sidebar = document.querySelector("[data-sidebar]");
    var overlay = document.querySelector("[data-sidebar-overlay]");
    if (sidebar) sidebar.classList.remove("open");
    if (overlay) overlay.classList.remove("active");
  }

  function initSidebar() {
    var menuBtn = document.querySelector("[data-mobile-menu]");
    var overlay = document.querySelector("[data-sidebar-overlay]");

    if (menuBtn) {
      menuBtn.addEventListener("click", function (e) {
        e.preventDefault();
        openSidebar();
      });
    }

    if (overlay) {
      overlay.addEventListener("click", function () {
        closeSidebar();
      });
    }

    // Close sidebar on Escape
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") {
        closeSidebar();
      }
    });
  }

  // ── Search highlight ───────────────────────────────────────────────

  function initSearch() {
    var searchInputs = document.querySelectorAll(".topbar-search input, .sidebar-search input");
    searchInputs.forEach(function (input) {
      input.addEventListener("keydown", function (e) {
        if (e.key === "Escape") {
          input.blur();
          input.value = "";
        }
      });
    });
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
    initSidebar();
    initSearch();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

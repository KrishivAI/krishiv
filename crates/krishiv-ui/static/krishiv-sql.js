// krishiv-sql.js — SQL editor submission.
//
// Posts the query as JSON to /api/v1/sql and renders the JSON response into
// `#results`. CSP-friendly (`script-src 'self'`), no external dependencies.
(function () {
  "use strict";

  function renderError(target, message) {
    target.innerHTML = "";
    var wrapper = document.createElement("div");
    wrapper.className = "empty-state";
    wrapper.innerHTML =
      '<div class="empty-icon" style="color: var(--error);">&#x2715;</div>' +
      '<div class="empty-title" style="color: var(--error);">Query Error</div>' +
      '<p class="empty-desc">' + escapeHtml(message) + '</p>';
    target.appendChild(wrapper);
  }

  function escapeHtml(str) {
    var div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }

  function renderResults(target, data) {
    var tpl = document.getElementById("result-template");
    var clone = tpl.content.cloneNode(true);

    if (data.error) {
      renderError(target, data.error);
      return;
    }

    var headRow = clone.querySelector("thead tr");
    (data.columns || []).forEach(function (col) {
      var th = document.createElement("th");
      th.textContent = col;
      headRow.appendChild(th);
    });

    var tbody = clone.querySelector("tbody");
    (data.rows || []).forEach(function (row) {
      var tr = document.createElement("tr");
      row.forEach(function (cell) {
        var td = document.createElement("td");
        td.className = "td-mono";
        td.textContent = cell === null ? "NULL" : String(cell);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });

    var meta = clone.querySelector(".refresh-note");
    if (meta) {
      meta.textContent = data.row_count + " row(s) in " + data.elapsed_ms + " ms";
    }

    target.innerHTML = "";
    target.appendChild(clone);
  }

  function init() {
    var form = document.querySelector(".sql-form");
    if (!form) {
      return;
    }
    var results = document.getElementById("results");
    var spinner = document.getElementById("spinner");

    form.addEventListener("submit", async function (evt) {
      evt.preventDefault();
      var query = form.querySelector("[name=query]").value;
      if (spinner) {
        spinner.style.display = "inline";
      }
      try {
        var headers = window.krishivAuthHeaders
          ? window.krishivAuthHeaders({ "Content-Type": "application/json" })
          : { "Content-Type": "application/json" };
        var resp = await fetch("/api/v1/sql", {
          method: "POST",
          headers: headers,
          credentials: "same-origin",
          body: JSON.stringify({ query: query }),
        });
        var data = await resp.json();
        renderResults(results, data);
      } catch (e) {
        renderError(results, String(e));
      } finally {
        if (spinner) {
          spinner.style.display = "none";
        }
      }
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();

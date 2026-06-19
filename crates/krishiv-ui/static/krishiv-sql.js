// krishiv-sql.js — SQL editor submission without htmx.
//
// Posts the query as JSON to /api/v1/sql and renders the JSON response into
// `#results`. Replaces the former htmx hx-post + json-enc + htmx:beforeSwap
// approach so the page needs no external script and works under a strict
// `script-src 'self'` CSP.
(function () {
  "use strict";

  function renderError(target, message) {
    target.innerHTML = "";
    var p = document.createElement("p");
    p.className = "meta";
    p.textContent = "Error: " + message;
    target.appendChild(p);
  }

  function renderResults(target, data) {
    var tpl = document.getElementById("result-template");
    var clone = tpl.content.cloneNode(true);

    if (data.error) {
      clone.querySelector("p.meta").textContent = "Error: " + data.error;
      target.innerHTML = "";
      target.appendChild(clone);
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
        td.textContent = cell === null ? "NULL" : String(cell);
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });

    clone.querySelector("p.meta").textContent =
      data.row_count + " row(s) in " + data.elapsed_ms + " ms";

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
        spinner.classList.add("active");
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
          spinner.classList.remove("active");
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

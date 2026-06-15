// krishiv-auth.js — shared bearer-token helper for same-origin UI fetches.
(function () {
  "use strict";

  function authHeaders(extra) {
    var headers = extra ? Object.assign({}, extra) : {};
    var meta = document.querySelector('meta[name="krishiv-ui-token"]');
    if (meta && meta.content) {
      headers.Authorization = "Bearer " + meta.content;
    }
    return headers;
  }

  window.krishivAuthHeaders = authHeaders;
})();

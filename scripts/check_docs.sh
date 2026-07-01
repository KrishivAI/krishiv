#!/usr/bin/env bash
# check_docs.sh — Validate docs content for correctness.
# Run from repo root: bash scripts/check_docs.sh

set -euo pipefail

DOCS_DIR="web/lib/docs-content"
FACTS="web/PRODUCT_FACTS.md"
ERRORS=0
WARNINGS=0

echo "=== Krishiv docs validation ==="

# ── 1. Banned overclaim words ───────────────────────────────────────────────
# Only flag words when used to describe Krishiv positively (not in comparisons).
# We check for the word followed by common overclaim patterns.
echo ""
echo "--- Checking for banned overclaim words ---"
BANNED_PATTERNS=(
  "Krishiv is production-ready"
  "Krishiv is battle-tested"
  "Krishiv is enterprise-grade"
  "Krishiv is world-class"
  "Krishiv is best-in-class"
  "Krishiv is revolutionary"
  "Krishiv is industry-leading"
  "fully production-ready"
  "completely battle-tested"
)
for pattern in "${BANNED_PATTERNS[@]}"; do
  # shellcheck disable=SC2086
  matches=$(grep -ril "$pattern" "$DOCS_DIR"/*.ts 2>/dev/null || true)
  if [ -n "$matches" ]; then
    echo "  FOUND overclaim: \"$pattern\""
    echo "$matches" | while read -r f; do echo "    -> $f"; done
    ERRORS=$((ERRORS + 1))
  fi
done
if [ $ERRORS -eq 0 ]; then
  echo "  OK — no banned overclaim words found"
fi

# ── 2. Maturity badge consistency ───────────────────────────────────────────
echo ""
echo "--- Checking maturity badge usage ---"
VALID_STATUSES="Available|Experimental|In Progress|Preview|Planned"
BAD_STATUS_COUNT=0
for f in "$DOCS_DIR"/*.ts; do
  # Only check files that define DocPage arrays
  grep -q "DocPage\[\]" "$f" 2>/dev/null || continue
  bad=$(grep -oP "status:\s*'[^']*'" "$f" 2>/dev/null | grep -vP "status:\s*'($VALID_STATUSES)'" || true)
  if [ -n "$bad" ]; then
    echo "  INVALID status in $f:"
    echo "$bad" | while read -r line; do echo "    $line"; done
    BAD_STATUS_COUNT=$((BAD_STATUS_COUNT + 1))
  fi
done
if [ $BAD_STATUS_COUNT -eq 0 ]; then
  echo "  OK — all status values are valid"
else
  ERRORS=$((ERRORS + BAD_STATUS_COUNT))
fi

# ── 3. Every DocPage has slug, title, description, status, group ────────────
echo ""
echo "--- Checking page metadata completeness ---"
INCOMPLETE=0
for f in "$DOCS_DIR"/*.ts; do
  grep -q "DocPage\[\]" "$f" 2>/dev/null || continue
  missing=""
  grep -qP "slug:" "$f" || missing="$missing slug"
  grep -qP "title:" "$f" || missing="$missing title"
  grep -qP "description:" "$f" || missing="$missing description"
  grep -qP "status:" "$f" || missing="$missing status"
  grep -qP "group:" "$f" || missing="$missing group"
  if [ -n "$missing" ]; then
    echo "  MISSING in $(basename "$f"):$missing"
    INCOMPLETE=$((INCOMPLETE + 1))
  fi
done
if [ $INCOMPLETE -eq 0 ]; then
  echo "  OK — all pages have required metadata"
else
  WARNINGS=$((WARNINGS + INCOMPLETE))
fi

# ── 4. Check that GROUP_ORDER entries have at least one page ────────────────
echo ""
echo "--- Checking group coverage ---"
for f in "$DOCS_DIR"/*.ts; do
  grep -q "DocPage\[\]" "$f" 2>/dev/null || continue
  groups=$(grep -oP "group:\s*'[^']*'" "$f" | sed "s/group: '//;s/'//" | sort -u)
  while IFS= read -r group; do
    [ -z "$group" ] && continue
    count=$(grep -rl "group: '$group'" "$DOCS_DIR"/*.ts 2>/dev/null | wc -l)
    if [ "$count" -eq 0 ]; then
      echo "  EMPTY group: $group"
      WARNINGS=$((WARNINGS + 1))
    fi
  done <<< "$groups"
done
echo "  Group coverage check complete"

# ── 5. Check for duplicate slug definitions ─────────────────────────────────
echo ""
echo "--- Checking for duplicate slugs ---"
ALL_SLUGS=""
for f in "$DOCS_DIR"/*.ts; do
  grep -q "DocPage\[\]" "$f" 2>/dev/null || continue
  slugs=$(grep -oP "slug:\s*'[^']*'" "$f" | sed "s/slug: '//;s/'//" )
  ALL_SLUGS="$ALL_SLUGS"$'\n'"$slugs"
done
ALL_SLUGS=$(echo "$ALL_SLUGS" | grep -v '^$' | sort)
DUPES=$(echo "$ALL_SLUGS" | uniq -d)
if [ -n "$DUPES" ]; then
  echo "  DUPLICATE slugs found:"
  echo "$DUPES" | while read -r s; do echo "    $s"; done
  ERRORS=$((ERRORS + 1))
else
  echo "  OK — no duplicate slugs ($(echo "$ALL_SLUGS" | wc -l) unique slugs)"
fi

# ── 6. Check PRODUCT_FACTS.md ───────────────────────────────────────────────
echo ""
echo "--- Checking PRODUCT_FACTS.md ---"
if [ -f "$FACTS" ]; then
  echo "  OK — $FACTS exists"
else
  echo "  WARNING — $FACTS not found"
  WARNINGS=$((WARNINGS + 1))
fi

# ── 7. Check for internal links pointing to non-existent docs pages ─────────
echo ""
echo "--- Checking internal doc links ---"
BROKEN=0
# Build a set of known slugs
KNOWN_SLUGS=""
for f in "$DOCS_DIR"/*.ts; do
  grep -q "DocPage\[\]" "$f" 2>/dev/null || continue
  slugs=$(grep -oP "slug:\s*'[^']*'" "$f" | sed "s/slug: '//;s/'//")
  KNOWN_SLUGS="$KNOWN_SLUGS"$'\n'"$slugs"
done
KNOWN_SLUGS=$(echo "$KNOWN_SLUGS" | grep -v '^$' | sort -u)

for f in "$DOCS_DIR"/*.ts; do
  links=$(grep -oP 'href="/docs/latest/[^"]*"' "$f" 2>/dev/null | grep -oP '/docs/latest/[^"]*' || true)
  while IFS= read -r link; do
    [ -z "$link" ] && continue
    slug=$(echo "$link" | sed 's|/docs/latest/||')
    if ! echo "$KNOWN_SLUGS" | grep -qFx "$slug"; then
      echo "  UNRESOLVED: $slug (from $(basename "$f"))"
      BROKEN=$((BROKEN + 1))
    fi
  done <<< "$links"
done
if [ $BROKEN -eq 0 ]; then
  echo "  OK — all internal doc links resolve"
else
  WARNINGS=$((WARNINGS + BROKEN))
fi

# ── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Summary ==="
echo "  Errors:   $ERRORS"
echo "  Warnings: $WARNINGS"
if [ $ERRORS -gt 0 ]; then
  echo "  FAILED — fix errors before pushing"
  exit 1
else
  echo "  PASSED"
  exit 0
fi

#!/usr/bin/env zsh
# use-env.sh — switch the active .env for ewura-migration
#
# Usage:
#   ./use-env.sh dev         → point .env at .env.dev
#   ./use-env.sh test        → point .env at .env.test
#   ./use-env.sh staging     → point .env at .env.staging
#   ./use-env.sh production  → point .env at .env.production  ⚠️  LIVE
#   ./use-env.sh show        → print the current DATABASE_URL (masked password)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"
TARGET="${1:-show}"

case "$TARGET" in
  dev|test|staging|production)
    SRC="$SCRIPT_DIR/.env.$TARGET"
    if [[ ! -f "$SRC" ]]; then
      echo "ERROR: $SRC not found." >&2
      exit 1
    fi

    # Extra safety gate for production
    if [[ "$TARGET" == "production" ]]; then
      echo ""
      echo "⚠️  ⚠️  ⚠️  WARNING: You are about to switch to PRODUCTION ⚠️  ⚠️  ⚠️"
      echo "   This points to the LIVE database."
      echo "   Only proceed if staging has been fully verified."
      echo ""
      printf "   Type 'yes' to continue: "
      read confirm
      if [[ "$confirm" != "yes" ]]; then
        echo "Aborted. No changes made."
        exit 0
      fi
    fi

    cp "$SRC" "$ENV_FILE"
    echo "✅  Switched to $TARGET  →  $(grep DATABASE_URL "$ENV_FILE" | sed 's/:\/\/[^:]*:[^@]*@/:\/\/***:***@/')"
    echo "   Restart uvicorn to apply: uvicorn app.main:app --reload"
    ;;
  show)
    if [[ -f "$ENV_FILE" ]]; then
      echo "Current .env:"
      grep DATABASE_URL "$ENV_FILE" | sed 's/:\/\/[^:]*:[^@]*@/:\/\/***:***@/'
    else
      echo "No .env file found."
    fi
    ;;
  *)
    echo "Usage: $0 {dev|test|staging|production|show}" >&2
    exit 1
    ;;
esac

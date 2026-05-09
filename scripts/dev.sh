#!/usr/bin/env bash
# Start the FastAPI backend and the Streamlit UI together as a single
# foreground command. Ctrl+C cleanly stops both.
#
# Usage:
#   bash scripts/dev.sh
#   ./scripts/dev.sh    (after `chmod +x scripts/dev.sh`)

set -euo pipefail

# Activate the project venv if not already active.
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  if [[ -f .venv/bin/activate ]]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
  else
    echo "No .venv found. Run 'python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt' first." >&2
    exit 1
  fi
fi

# Start uvicorn in the background, capture its PID for cleanup.
echo "Starting FastAPI on http://localhost:8000 ..."
uvicorn api.main:app --reload --port 8000 --log-level warning &
API_PID=$!

# When this script exits for ANY reason (Ctrl+C, error, normal end),
# make sure we don't leave uvicorn lingering.
cleanup() {
  echo
  echo "Stopping FastAPI (PID $API_PID) ..."
  kill "$API_PID" 2>/dev/null || true
  wait "$API_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# Give uvicorn a moment to start up before launching Streamlit so the
# UI's API health check goes green on first paint.
sleep 2

echo "Starting Streamlit on http://localhost:8501 ..."
streamlit run ui/Home.py

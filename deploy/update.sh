#!/usr/bin/env bash
# One-command update for the Pi:  ./deploy/update.sh   (add --force to run during :00-:09)
# Fetches GitHub, fast-forwards only, runs the tests, then restarts tcp-server.
# If anything fails before the restart, the running service is left untouched.
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVICE="${SERVICE:-tcp-server}"
PYTHON="${PYTHON:-$APP_DIR/.venv/bin/python}"
FORCE=0
[ "${1:-}" = "--force" ] && FORCE=1
SUDO=""
[ "$(id -u)" -ne 0 ] && SUDO="sudo"

cd "$APP_DIR"

# Restarting drops every logger and collector, so keep it out of the hourly collection window.
minute=$((10#$(date +%M)))
if [ "$minute" -lt 10 ] && [ "$FORCE" -eq 0 ]; then
  echo "It is :$(printf '%02d' "$minute") - the hourly collection window (:00-:09). Wait until :10 or run with --force."
  exit 1
fi

git fetch --quiet origin
branch="$(git rev-parse --abbrev-ref HEAD)"
if [ "$(git rev-parse HEAD)" = "$(git rev-parse '@{u}')" ]; then
  echo "Already up to date on '$branch' ($(git rev-parse --short HEAD)). Nothing to do."
  exit 0
fi

echo "Incoming changes on '$branch':"
git log --oneline 'HEAD..@{u}'
before_requirements="$(git rev-parse HEAD:requirements.txt)"
git pull --ff-only --quiet
echo "Updated to $(git rev-parse --short HEAD)."

if [ "$(git rev-parse HEAD:requirements.txt)" != "$before_requirements" ]; then
  echo "requirements.txt changed - installing dependencies."
  "$PYTHON" -m pip install -r requirements.txt
fi

echo "Running tests..."
if ! "$PYTHON" -m unittest discover -s tests >/tmp/tcp-server-update-tests.log 2>&1; then
  tail -n 25 /tmp/tcp-server-update-tests.log
  echo "Tests FAILED - service NOT restarted (still running the previous code in memory)."
  echo "To go back:  git reset --hard ORIG_HEAD"
  exit 1
fi

echo "Restarting $SERVICE..."
$SUDO systemctl restart "$SERVICE"
sleep 2
$SUDO systemctl --no-pager --lines=5 status "$SERVICE" || true
echo "Done. Dashboard: http://$(hostname -I | awk '{print $1}'):8888"

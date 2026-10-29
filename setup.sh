#!/usr/bin/env bash
set -euo pipefail

SERVICE_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_NAME="$(basename "$SERVICE_DIR")"
BINARY="$SERVICE_DIR/$SERVICE_NAME"
PID_FILE="$SERVICE_DIR/$SERVICE_NAME.pid"
LOG_DIR="$SERVICE_DIR/log"
LOG_FILE="$LOG_DIR/$SERVICE_NAME.log"
STOP_TIMEOUT="${STOP_TIMEOUT:-15}"
UNIT_NAME="starland-$SERVICE_NAME.service"

acquire_lock() {
  exec 9>"$SERVICE_DIR/.setup.lock"
  flock -n 9 || { echo "$SERVICE_NAME is being operated" >&2; exit 1; }
}

matches_binary() {
  local pid="$1" actual
  actual="$(readlink "/proc/$pid/exe" 2>/dev/null || true)"
  [[ "$actual" == "$BINARY" ]]
}

find_pids() {
  local proc_exe pid actual
  for proc_exe in /proc/[0-9]*/exe; do
    actual="$(readlink "$proc_exe" 2>/dev/null || true)"
    if [[ "$actual" == "$BINARY" ]]; then
      pid="${proc_exe#/proc/}"
      printf '%s\n' "${pid%/exe}"
    fi
  done
}

running_pids() {
  local pid
  if [[ -f "$PID_FILE" ]]; then
    pid="$(<"$PID_FILE")"
    if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null && matches_binary "$pid"; then
      printf '%s\n' "$pid"
      return
    fi
  fi
  find_pids
}

write_pid() {
  local pid="$1" tmp="$PID_FILE.tmp.$$"
  printf '%s\n' "$pid" >"$tmp"
  mv -f "$tmp" "$PID_FILE"
}

start_service() {
  local pid
  if systemctl is-active --quiet "$UNIT_NAME"; then
    pid="$(systemctl show "$UNIT_NAME" --property MainPID --value)"
    write_pid "$pid"
    echo "$SERVICE_NAME is already running (pid $pid)"
    return
  fi
  mapfile -t pids < <(running_pids)
  if (( ${#pids[@]} > 0 )); then
    write_pid "${pids[0]}"
    echo "$SERVICE_NAME is already running (pid ${pids[*]})"
    return
  fi
  [[ -x "$BINARY" ]] || { echo "executable not found: $BINARY" >&2; return 1; }
  mkdir -p "$LOG_DIR"
  systemctl reset-failed "$UNIT_NAME" 2>/dev/null || true
  systemd-run \
    --unit "$UNIT_NAME" \
    --collect \
    --property "WorkingDirectory=$SERVICE_DIR" \
    --property "KillSignal=SIGTERM" \
    --property "TimeoutStopSec=${STOP_TIMEOUT}s" \
    --property "Restart=no" \
    /bin/bash -c 'exec "$1" -d "$2" >>"$3" 2>&1' bash "$BINARY" "$SERVICE_DIR/conf" "$LOG_FILE" >/dev/null
  sleep 1
  if ! systemctl is-active --quiet "$UNIT_NAME"; then
    rm -f "$PID_FILE"
    echo "$SERVICE_NAME failed to start; check $LOG_FILE" >&2
    return 1
  fi
  pid="$(systemctl show "$UNIT_NAME" --property MainPID --value)"
  write_pid "$pid"
  echo "$SERVICE_NAME started (pid $pid)"
}

stop_service() {
  local pid deadline alive
  if systemctl is-active --quiet "$UNIT_NAME"; then
    systemctl stop "$UNIT_NAME"
  fi
  mapfile -t pids < <(running_pids)
  if (( ${#pids[@]} == 0 )); then
    rm -f "$PID_FILE"
    echo "$SERVICE_NAME is stopped"
    return
  fi
  for pid in "${pids[@]}"; do
    kill -TERM "$pid"
  done
  deadline=$((SECONDS + STOP_TIMEOUT))
  while (( SECONDS < deadline )); do
    alive=0
    for pid in "${pids[@]}"; do
      if kill -0 "$pid" 2>/dev/null && matches_binary "$pid"; then alive=1; fi
    done
    (( alive == 0 )) && break
    sleep 1
  done
  for pid in "${pids[@]}"; do
    if kill -0 "$pid" 2>/dev/null && matches_binary "$pid"; then
      kill -KILL "$pid"
    fi
  done
  rm -f "$PID_FILE"
  echo "$SERVICE_NAME stopped"
}

status_service() {
  local pid
  if systemctl is-active --quiet "$UNIT_NAME"; then
    pid="$(systemctl show "$UNIT_NAME" --property MainPID --value)"
    echo "$SERVICE_NAME is running (pid $pid)"
    return
  fi
  mapfile -t pids < <(running_pids)
  if (( ${#pids[@]} == 0 )); then
    echo "$SERVICE_NAME is stopped"
    return 1
  fi
  echo "$SERVICE_NAME is running (pid ${pids[*]})"
}

case "${1:-}" in
  start) acquire_lock; start_service ;;
  stop) acquire_lock; stop_service ;;
  restart) acquire_lock; stop_service; start_service ;;
  status) status_service ;;
  *) echo "Usage: $0 {start|stop|restart|status}" >&2; exit 2 ;;
esac

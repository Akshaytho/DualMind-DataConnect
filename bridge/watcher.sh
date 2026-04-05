#!/bin/bash
# DualMind Bridge v3.0 — DataConnect Build

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$SCRIPT_DIR/bridge.log"
SECRETS_FILE="$SCRIPT_DIR/.secrets"
TURNS_LOG="$SCRIPT_DIR/.turns_log"
POLL_INTERVAL=60
MAX_TURNS_PER_HOUR=15
COOLDOWN=15

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') | $1" | tee -a "$LOG_FILE"; }

command -v claude &>/dev/null || { log "ERROR: claude not found"; exit 1; }
[ -f "$SECRETS_FILE" ] || { log "ERROR: .secrets missing"; exit 1; }

GIT_TOKEN=$(cat "$SECRETS_FILE")
cd "$REPO_DIR" || exit 1
git remote set-url origin "https://Akshaytho:${GIT_TOKEN}@github.com/Akshaytho/DualMind-DataConnect.git" 2>/dev/null
git config user.email "bridge@dualmind.ai"
git config user.name "DualMind Bridge"

caffeinate -d &
CAF=$!
trap 'kill $CAF 2>/dev/null; log "Stopped."; exit 0' EXIT INT TERM

log "=== DualMind Bridge v3.0 — DataConnect ==="

MIND_A='You are Kiran (Mind A) building DataConnect. CHECKLIST: 1) Read STATUS.json 2) Read MEMORY.md 3) tail -80 CONVERSATION.md 4) Read only relevant code 5) Read mind-a/PERSONALITY.md. DO WORK. If code changed: cd workspace && python -m pytest -x -q — ALL MUST PASS. DEVIL ADVOCATE before agreeing. AFTER: append to CONVERSATION.md (under 60 lines), update STATUS.json (current_turn=MIND_B, increment), update MEMORY.md, git add -A && git commit -m "[Mind A] Turn N: desc" && git push. SAVE CREDITS.'

MIND_B='You are Arjun (Mind B) building DataConnect. CHECKLIST: 1) Read STATUS.json 2) Read MEMORY.md 3) tail -80 CONVERSATION.md 4) Read only relevant code 5) Read mind-b/PERSONALITY.md. DO WORK. If code changed: cd workspace && python -m pytest -x -q — ALL MUST PASS. DEVIL ADVOCATE before agreeing. AFTER: append to CONVERSATION.md (under 60 lines), update STATUS.json (current_turn=MIND_A, increment), update MEMORY.md, git add -A && git commit -m "[Mind B] Turn N: desc" && git push. SAVE CREDITS.'

rate_ok() {
    touch "$TURNS_LOG"
    local AGO=$(date -v-1H +%s 2>/dev/null || date -d '1 hour ago' +%s 2>/dev/null)
    local N=$(awk -v c="$AGO" '$1>=c' "$TURNS_LOG" 2>/dev/null | wc -l | tr -d ' ')
    [ "$N" -ge "$MAX_TURNS_PER_HOUR" ] && { log "RATE LIMIT $N/$MAX_TURNS_PER_HOUR"; sleep 600; return 1; }
    log "Rate: $N/$MAX_TURNS_PER_HOUR"; return 0
}

run_turn() {
    cd "$REPO_DIR"; [ -f STATUS.json ] || return 1
    local T=$(python3 -c "import json; print(json.load(open('STATUS.json'))['current_turn'])" 2>/dev/null)
    local N=$(python3 -c "import json; print(json.load(open('STATUS.json')).get('turn_number','?'))" 2>/dev/null)
    local U=$(python3 -c "import json; print(json.load(open('STATUS.json')).get('user_action_needed',False))" 2>/dev/null)
    log "Turn $N | $T | User:$U"
    [ "$U" = "True" ] && return 1
    rate_ok || return 1
    if [ "$T" = "MIND_A" ]; then
        log ">>> Mind A..."; claude -p "$MIND_A" --dangerously-skip-permissions 2>&1 | tail -3 | while read l; do log "  A:$l"; done
        log ">>> A done"; date +%s >> "$TURNS_LOG"; return 0
    elif [ "$T" = "MIND_B" ]; then
        log ">>> Mind B..."; claude -p "$MIND_B" --dangerously-skip-permissions 2>&1 | tail -3 | while read l; do log "  B:$l"; done
        log ">>> B done"; date +%s >> "$TURNS_LOG"; return 0
    fi; return 1
}

while true; do
    cd "$REPO_DIR"; git fetch origin main 2>/dev/null
    L=$(git rev-parse HEAD 2>/dev/null); R=$(git rev-parse origin/main 2>/dev/null)
    [ "$L" != "$R" ] && { log "Pulling..."; git pull origin main --no-rebase 2>/dev/null; }
    K=true; while $K; do K=false
        run_turn && { sleep "$COOLDOWN"; git pull origin main --no-rebase 2>/dev/null; K=true; }
    done; sleep "$POLL_INTERVAL"
done

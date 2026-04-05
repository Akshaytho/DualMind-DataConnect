#!/bin/bash
# DualMind Bridge v3 — DataConnect Build

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$SCRIPT_DIR/bridge.log"
SECRETS_FILE="$SCRIPT_DIR/.secrets"
TURNS_LOG="$SCRIPT_DIR/.turns_log"
POLL_INTERVAL=90
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

log "==========================================="
log "DualMind Bridge v3 — DataConnect"
log "==========================================="

MIND_A='You are Kiran (Mind A, The Pragmatist) building DataConnect.

CHECKLIST:
1. Read STATUS.json — confirm your turn (MIND_A)
2. Read MEMORY.md
3. Read last entries: tail -80 CONVERSATION.md
4. Read CODING_RULES.md before writing any code
5. Read only relevant code files (check MEMORY.md code map)
6. Read mind-a/PERSONALITY.md

DO YOUR WORK. If you write code:
- Follow ALL 30 rules in CODING_RULES.md
- Run: cd workspace && python -m pytest -x -q
- ALL tests MUST pass before committing

DEVIL ADVOCATE: State the strongest argument against before agreeing.

AFTER:
- Append to CONVERSATION.md (under 60 lines)
- Update STATUS.json (current_turn=MIND_B, increment turn_number)
- Update MEMORY.md code map if files changed
- git add -A && git commit -m "[Mind A] Turn N: desc" && git push origin main'

MIND_B='You are Arjun (Mind B, The Architect) building DataConnect.

CHECKLIST:
1. Read STATUS.json — confirm your turn (MIND_B)
2. Read MEMORY.md
3. Read last entries: tail -80 CONVERSATION.md
4. Read CODING_RULES.md before writing any code
5. Read only relevant code files (check MEMORY.md code map)
6. Read mind-b/PERSONALITY.md

DO YOUR WORK. If you write code:
- Follow ALL 30 rules in CODING_RULES.md
- Run: cd workspace && python -m pytest -x -q
- ALL tests MUST pass before committing

DEVIL ADVOCATE: State the strongest argument against before agreeing.

AFTER:
- Append to CONVERSATION.md (under 60 lines)
- Update STATUS.json (current_turn=MIND_A, increment turn_number)
- Update MEMORY.md code map if files changed
- git add -A && git commit -m "[Mind B] Turn N: desc" && git push origin main'

rate_check() {
    touch "$TURNS_LOG"
    local AGO=$(date -v-1H +%s 2>/dev/null || date -d '1 hour ago' +%s 2>/dev/null)
    local N=$(awk -v c="$AGO" '$1>=c' "$TURNS_LOG" 2>/dev/null | wc -l | tr -d ' ')
    [ "$N" -ge "$MAX_TURNS_PER_HOUR" ] && { log "RATE LIMIT ($N/hr). Pausing 10m."; sleep 600; return 1; }
    log "Rate: $N/$MAX_TURNS_PER_HOUR/hr"
    return 0
}

run_turn() {
    cd "$REPO_DIR"
    [ -f STATUS.json ] || return 1
    local T=$(python3 -c "import json;print(json.load(open('STATUS.json'))['current_turn'])" 2>/dev/null)
    local N=$(python3 -c "import json;print(json.load(open('STATUS.json')).get('turn_number','?'))" 2>/dev/null)
    local U=$(python3 -c "import json;print(json.load(open('STATUS.json')).get('user_action_needed',False))" 2>/dev/null)
    log "Turn $N | Current: $T | User: $U"
    [ "$U" = "True" ] && { log "User needed — pausing"; return 1; }
    rate_check || return 1

    if [ "$T" = "MIND_A" ]; then
        log ">>> Mind A (Kiran) starting..."
        claude -p "$MIND_A" --dangerously-skip-permissions 2>&1 | tail -5 | while read l; do log "  A: $l"; done
        log ">>> Mind A done"; date +%s >> "$TURNS_LOG"; return 0
    elif [ "$T" = "MIND_B" ]; then
        log ">>> Mind B (Arjun) starting..."
        claude -p "$MIND_B" --dangerously-skip-permissions 2>&1 | tail -5 | while read l; do log "  B: $l"; done
        log ">>> Mind B done"; date +%s >> "$TURNS_LOG"; return 0
    fi
    return 1
}

while true; do
    cd "$REPO_DIR"
    git fetch origin main 2>/dev/null
    L=$(git rev-parse HEAD 2>/dev/null); R=$(git rev-parse origin/main 2>/dev/null)
    [ "$L" != "$R" ] && { log "Pulling..."; git pull origin main --no-rebase 2>/dev/null; }

    K=true
    while $K; do
        K=false
        if run_turn; then sleep "$COOLDOWN"; git pull origin main --no-rebase 2>/dev/null; K=true; fi
    done
    sleep "$POLL_INTERVAL"
done

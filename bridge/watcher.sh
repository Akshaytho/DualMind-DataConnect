#!/bin/bash
# DualMind Bridge v3.1 — Hardened
# Fixes: timeout, stuck detection, rollback, linting

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
LOG_FILE="$SCRIPT_DIR/bridge.log"
SECRETS_FILE="$SCRIPT_DIR/.secrets"
TURNS_LOG="$SCRIPT_DIR/.turns_log"
POLL_INTERVAL=60
MAX_TURNS_PER_HOUR=15
COOLDOWN=15
TURN_TIMEOUT=600  # 10 minutes max per turn
MAX_STUCK_RETRIES=2

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

log "=== DualMind Bridge v3.1 — Hardened ==="

MIND_A='You are Kiran (Mind A) building DataConnect.

CHECKLIST:
1. Read STATUS.json — confirm your turn (MIND_A)
2. Read MEMORY.md — persistent context
3. tail -80 CONVERSATION.md — recent discussion
4. Read ONLY code files relevant to THIS turn (check MEMORY.md code map)
5. Read mind-a/PERSONALITY.md

DO YOUR WORK. Follow TASK.md and CODING_RULES.md strictly.

IF YOU WROTE CODE:
- cd workspace && python -m pytest -x -q — ALL MUST PASS
- Pin any new dependency in requirements.txt with exact version
- Never log database query results or connection strings with passwords
- All database queries MUST be read-only SELECT via SQLAlchemy parameterized queries
- Add API key auth to any HTTP endpoint

DEVIL ADVOCATE: Before agreeing with Mind B, state the strongest counter-argument.

AFTER:
- Append turn to CONVERSATION.md (under 60 lines, format per PROTOCOL.md)
- Update STATUS.json (current_turn=MIND_B, increment turn_number)
- Update MEMORY.md code map if you added/changed files
- git add -A && git commit -m "[Mind A] Turn N: desc" && git push origin main

SAVE CREDITS: Only read files you need. Dont rewrite working code. Be concise.'

MIND_B='You are Arjun (Mind B) building DataConnect.

CHECKLIST:
1. Read STATUS.json — confirm your turn (MIND_B)
2. Read MEMORY.md — persistent context
3. tail -80 CONVERSATION.md — recent discussion
4. Read ONLY code files relevant to THIS turn (check MEMORY.md code map)
5. Read mind-b/PERSONALITY.md

DO YOUR WORK. Follow TASK.md and CODING_RULES.md strictly.

IF YOU WROTE CODE:
- cd workspace && python -m pytest -x -q — ALL MUST PASS
- Pin any new dependency in requirements.txt with exact version
- Never log database query results or connection strings with passwords
- All database queries MUST be read-only SELECT via SQLAlchemy parameterized queries
- Add API key auth to any HTTP endpoint

DEVIL ADVOCATE: Before agreeing with Mind A, state the strongest counter-argument.

AFTER:
- Append turn to CONVERSATION.md (under 60 lines, format per PROTOCOL.md)
- Update STATUS.json (current_turn=MIND_A, increment turn_number)
- Update MEMORY.md code map if you added/changed files
- git add -A && git commit -m "[Mind B] Turn N: desc" && git push origin main

SAVE CREDITS: Only read files you need. Dont rewrite working code. Be concise.'

LAST_TURN_NUM=""
STUCK_COUNT=0

rate_ok() {
    touch "$TURNS_LOG"
    local AGO=$(date -v-1H +%s 2>/dev/null || date -d '1 hour ago' +%s 2>/dev/null)
    local N=$(awk -v c="$AGO" '$1>=c' "$TURNS_LOG" 2>/dev/null | wc -l | tr -d ' ')
    [ "$N" -ge "$MAX_TURNS_PER_HOUR" ] && { log "RATE LIMIT $N/$MAX_TURNS_PER_HOUR. Pause 10m."; sleep 600; return 1; }
    log "Rate: $N/$MAX_TURNS_PER_HOUR"; return 0
}

get_turn_num() {
    python3 -c "import json; print(json.load(open('STATUS.json')).get('turn_number','?'))" 2>/dev/null
}

run_turn() {
    cd "$REPO_DIR"; [ -f STATUS.json ] || return 1
    local T=$(python3 -c "import json; print(json.load(open('STATUS.json'))['current_turn'])" 2>/dev/null)
    local N=$(get_turn_num)
    local U=$(python3 -c "import json; print(json.load(open('STATUS.json')).get('user_action_needed',False))" 2>/dev/null)
    log "Turn $N | $T | User:$U"
    [ "$U" = "True" ] && { log "User needed — pausing"; return 1; }
    rate_ok || return 1

    # Stuck detection: if turn number hasn't changed after running
    if [ "$N" = "$LAST_TURN_NUM" ]; then
        STUCK_COUNT=$((STUCK_COUNT + 1))
        log "WARNING: Turn $N stuck ($STUCK_COUNT/$MAX_STUCK_RETRIES)"
        if [ "$STUCK_COUNT" -ge "$MAX_STUCK_RETRIES" ]; then
            log "CRITICAL: Turn $N stuck after $MAX_STUCK_RETRIES attempts. Rolling back and pausing."
            git checkout -- STATUS.json CONVERSATION.md 2>/dev/null
            osascript -e 'display notification "DualMind stuck on turn '"$N"'. Check logs." with title "DualMind"' 2>/dev/null
            STUCK_COUNT=0
            sleep 300
            return 1
        fi
    else
        STUCK_COUNT=0
    fi
    LAST_TURN_NUM="$N"

    local PROMPT=""
    local LABEL=""
    if [ "$T" = "MIND_A" ]; then
        PROMPT="$MIND_A"; LABEL="Mind A"
    elif [ "$T" = "MIND_B" ]; then
        PROMPT="$MIND_B"; LABEL="Mind B"
    else
        return 1
    fi

    log ">>> $LABEL starting (timeout ${TURN_TIMEOUT}s)..."
    
    # Run with timeout
    timeout "$TURN_TIMEOUT" claude -p "$PROMPT" --dangerously-skip-permissions 2>&1 | tail -5 | while read l; do log "  $LABEL: $l"; done
    local EXIT=$?

    if [ "$EXIT" -eq 124 ]; then
        log "TIMEOUT: $LABEL exceeded ${TURN_TIMEOUT}s. Killing and retrying."
        return 0  # will retry via stuck detection
    fi

    # Verify turn actually advanced
    local NEW_N=$(get_turn_num)
    if [ "$NEW_N" != "$N" ]; then
        log ">>> $LABEL completed. Turn $N → $NEW_N"
        date +%s >> "$TURNS_LOG"
        STUCK_COUNT=0
    else
        log "WARNING: $LABEL finished but turn didn't advance ($N)"
    fi

    return 0
}

# --- Main Loop ---
while true; do
    cd "$REPO_DIR"
    
    # Git recovery
    GIT_STATUS=$(git status --porcelain 2>/dev/null)
    if [ -n "$GIT_STATUS" ]; then
        log "WARNING: Dirty git. Cleaning..."
        git stash 2>/dev/null
        git checkout main 2>/dev/null
    fi
    
    git fetch origin main 2>/dev/null
    L=$(git rev-parse HEAD 2>/dev/null); R=$(git rev-parse origin/main 2>/dev/null)
    [ "$L" != "$R" ] && { log "Pulling..."; git pull origin main --no-rebase 2>/dev/null; }

    K=true; while $K; do K=false
        if run_turn; then
            sleep "$COOLDOWN"
            git pull origin main --no-rebase 2>/dev/null
            K=true
        fi
    done
    sleep "$POLL_INTERVAL"
done

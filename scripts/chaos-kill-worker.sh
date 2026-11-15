#!/usr/bin/env bash
# Chaos test: SIGKILL a delivery worker in the middle of a burst and prove no event is lost.
#
# Usage: scripts/chaos-kill-worker.sh [events] [workers]
# Requires the compose stack (docker compose up -d) and python3.
set -euo pipefail
G=${GATEWAY:-http://localhost:8080}
EVENTS=${1:-20000}
WORKERS=${2:-3}

json() { python3 -c "import sys,json; d=json.load(sys.stdin); print($1)"; }
post() { curl -sf -XPOST "$G$1" -H 'Content-Type: application/json' -d "${2:-{\}}"; }

echo "▶ resetting receivers and the integrity window; all receivers healthy"
post /api/chaos/presets/all-healthy >/dev/null
post /api/sim/stop >/dev/null
post /api/chaos/reset-stats >/dev/null

echo "▶ scaling to $WORKERS workers"
post /api/infra/workers/scale "{\"replicas\":$WORKERS}" >/dev/null
for _ in $(seq 1 30); do
  running=$(curl -sf $G/api/infra | json "sum(1 for w in d['workers'] if w['status']=='running' and w['partitions'])")
  [ "$running" -ge "$WORKERS" ] && break; sleep 2
done
echo "  $running workers own partitions"

echo "▶ firing a burst of $EVENTS events"
post /api/sim/burst "{\"count\":$EVENTS}" >/dev/null
sleep 6

victim=$(curl -sf $G/api/infra | json "[w['id'] for w in d['workers'] if w['status']=='running' and w['partitions']][0]")
parts=$(curl -sf $G/api/infra | json "[w['partitions'] for w in d['workers'] if w['id']=='$victim'][0]")
echo "▶ SIGKILL worker $victim (owns partitions $parts) mid-burst"
post "/api/infra/workers/$victim/kill" >/dev/null

echo "▶ waiting for the backlog to drain"
for i in $(seq 1 120); do
  sleep 3
  read -r sim_done lag retrying in_progress < <(
    { curl -sf $G/api/sim/status; echo; curl -sf $G/api/infra; echo; curl -sf $G/api/integrity; echo; } | python3 -c "
import sys,json
sim,infra,integ=[json.loads(l) for l in sys.stdin if l.strip()]
lag=sum(g['total_lag'] for g in infra['consumer_groups'] if g['group'] in ('boomerang-delivery','boomerang-fanout','boomerang-recorder'))
ret=sum(t['depth'] for t in infra['retry_tiers'])
print(int(not sim['running']), lag, ret, integ['in_progress'])")
  printf "  t=%3ss lag=%-6s retrying=%-6s in_progress=%s\n" $((i*3)) "$lag" "$retrying" "$in_progress"
  [ "$sim_done" = 1 ] && [ "$lag" = 0 ] && [ "$retrying" = 0 ] && [ "$in_progress" = 0 ] && break
done
sleep 4
curl -sf $G/api/integrity | python3 -c "
import sys,json
d=json.load(sys.stdin)
print()
print('  deliveries created      ', d['deliveries_created'])
print('  delivered               ', d['delivered'])
print('  dead-lettered           ', d['dead'])
print('  in progress             ', d['in_progress'])
print('  confirmed by receivers  ', d['receivers_unique'])
print('  duplicates (redelivered)', d['receivers_duplicates'])
print('  LOST                    ', d['lost'])
ok = d['lost']==0 and d['in_progress']==0 and d['deliveries_created']==d['delivered']+d['dead'] and d['receivers_unique']>=d['delivered']
print()
print('✅ PASS: no events lost' if ok else '❌ FAIL: accounting mismatch')
sys.exit(0 if ok else 1)"

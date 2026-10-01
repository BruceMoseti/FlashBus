#!/usr/bin/env bash
# Collects Linux perf counters for a FlashBus benchmark.
#
# This script checks whether the counters it wants actually exist before
# reporting anything. Virtual machines usually expose no virtual PMU, and there
# `perf stat -e cycles` prints "<not supported>" rather than failing: it is very
# easy to end up quoting cache-miss numbers that were never measured.
#
# usage: scripts/run_perf.sh [--build DIR] [--record] -- <binary> [args...]
#        scripts/run_perf.sh                 # defaults to a queue_bench run

set -uo pipefail

BUILD="${BUILD:-build/default}"
RECORD=0
OUT="${OUT:-results/perf}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build) BUILD="$2"; shift 2 ;;
    --record) RECORD=1; shift ;;
    --out) OUT="$2"; shift 2 ;;
    --) shift; break ;;
    *) break ;;
  esac
done

if [[ $# -gt 0 ]]; then
  TARGET=("$@")
else
  TARGET=("${BUILD}/benchmarks/queue_bench" --messages 20000000 --payload 48 --mode throughput)
fi

# Counts one event and echoes the value, or nothing if it cannot be counted.
#
# Checking that a perf binary exists is not enough. Ubuntu's /usr/bin/perf is a
# wrapper that looks for a binary matching the running kernel and, when it finds
# none, prints installation advice and exits successfully. Grepping its output
# for "not supported" then finds nothing and every event looks available --
# which is the exact mistake this script exists to prevent. So each candidate
# has to actually produce a count before it is trusted.
count_event() {
  local perf="$1" event="$2" line
  # A counted event's first CSV field is a number; task-clock reports a float
  # in milliseconds, so both forms have to match. Anything else -- including
  # "<not supported>" and "<not counted>" -- means the event is unusable.
  line=$("${perf}" stat -e "${event}" -x, true 2>&1 | grep -E "^[0-9]+(\.[0-9]+)?," | head -1)
  [[ -n "${line}" ]] && echo "${line%%,*}"
}

PERF=""
for candidate in $(command -v perf 2>/dev/null) /usr/lib/linux-tools/*/perf; do
  [[ -x "${candidate}" ]] || continue
  if [[ -n "$(count_event "${candidate}" task-clock)" ]]; then
    PERF="${candidate}"
    break
  fi
  echo "note: ${candidate} cannot count even task-clock; skipping it"
done

if [[ -z "${PERF}" ]]; then
  echo
  echo "No working perf on this machine."
  echo "  Ubuntu:  sudo apt-get install linux-tools-common linux-tools-\$(uname -r)"
  echo "  Fedora:  sudo dnf install perf"
  echo
  echo "FlashBus collects context switches, page faults, CPU time and resident"
  echo "memory itself through getrusage, and those appear in every result row,"
  echo "so the benchmarks remain usable without perf. What is lost is the"
  echo "microarchitectural view: cycles, branch misses and cache misses."
  exit 127
fi

PARANOID=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null || echo unknown)
echo "perf:            ${PERF}"
echo "paranoid level:  ${PARANOID}  (needs <= 2 for user-space counters, or run as root)"

HARDWARE=(cycles instructions branches branch-misses cache-references cache-misses
          stalled-cycles-frontend)
SOFTWARE=(task-clock context-switches cpu-migrations page-faults minor-faults major-faults)

available=()
unsupported=()
for event in "${HARDWARE[@]}" "${SOFTWARE[@]}"; do
  if [[ -n "$(count_event "${PERF}" "${event}")" ]]; then
    available+=("${event}")
  else
    unsupported+=("${event}")
  fi
done

echo
if [[ ${#unsupported[@]} -gt 0 ]]; then
  echo "NOT available here: ${unsupported[*]}"
  if [[ " ${unsupported[*]} " == *" cycles "* ]]; then
    echo
    echo "  No hardware PMU. This is almost certainly a VM without a virtual PMU."
    echo "  Cache, branch and cycle counts cannot be measured on this machine, so any"
    echo "  microarchitectural claim has to come from a bare-metal host instead."
  fi
  echo
fi
if [[ ${#available[@]} -eq 0 ]]; then
  echo "No usable events at all; nothing to collect."
  exit 1
fi
echo "collecting: ${available[*]}"

mkdir -p "${OUT}"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
EVENTS=$(IFS=,; echo "${available[*]}")

echo
echo "=== perf stat -e ${EVENTS} -- ${TARGET[*]}"
"${PERF}" stat -e "${EVENTS}" -- "${TARGET[@]}" 2> >(tee "${OUT}/stat-${STAMP}.txt" >&2)

if [[ ${RECORD} -eq 1 ]]; then
  echo
  echo "=== perf record"
  "${PERF}" record -g --call-graph=fp -o "${OUT}/perf-${STAMP}.data" -- "${TARGET[@]}" >/dev/null
  "${PERF}" report -i "${OUT}/perf-${STAMP}.data" --stdio --percent-limit 1 \
    > "${OUT}/report-${STAMP}.txt" 2>/dev/null
  echo "wrote ${OUT}/report-${STAMP}.txt"
  head -40 "${OUT}/report-${STAMP}.txt"
fi

echo
echo "wrote ${OUT}/stat-${STAMP}.txt"

#!/bin/bash
# Detached gate runner: bash skills/gating/gate.sh <label> <command...>
# Writes $SCRATCH/<label>.log and $SCRATCH/<label>.done (EXIT=<code>).
# Survives tool timeouts and session teardown (setsid + nohup + /dev/null stdin).
set -u
label=$1; shift
scratch=${KRISHIV_SCRATCH:-${CLAUDE_SCRATCHPAD:-/tmp/krishiv-gates}}
mkdir -p "$scratch"
log="$scratch/$label.log"; done_file="$scratch/$label.done"
rm -f "$done_file"
export CXXFLAGS="${CXXFLAGS:--include cstdint}"
pid_file="$scratch/$label.pid"
setsid -f nohup bash -c "echo \$\$ > '$pid_file'; cd '$PWD' && { $* ; } > '$log' 2>&1; echo \"EXIT=\$?\" > '$done_file'" </dev/null >/dev/null 2>&1
sleep 0.2
echo "detached: pid=$(cat "$pid_file" 2>/dev/null) log=$log done=$done_file"
echo "stop it with: kill -- -\$(cat $pid_file)   # the whole process group"

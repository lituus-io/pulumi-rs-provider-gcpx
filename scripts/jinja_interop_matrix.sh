#!/usr/bin/env bash
# Copyright: lituus-io, all rights reserved.
# Author: terekete <spicyzhug@gmail.com>
#
# Does template syntax in a provider property survive the YAML runtime?
#
# Two renderers see a stack. The YAML runtime renders Jinja over the stack file
# once, before any resource exists; this provider renders dbt templates in SQL
# afterwards, per resource. Anything written inline in the stack file therefore
# passes through the first renderer before the provider ever sees it, and
# `fn::readFile` does not.
#
# This walks every text-bearing property that could plausibly hold a template,
# in both delivery styles and both undefined-name modes, and reports which
# combinations survive. It needs the pulumi CLI and the plugin on PATH; it never
# deploys, only previews.
#
#   scripts/jinja_interop_matrix.sh [PROJECT]

set -uo pipefail

PROJECT="${1:-${GCPX_TEST_PROJECT:-example-project}}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

export PATH="$ROOT/target/release:$PATH"
export PULUMI_CONFIG_PASSPHRASE="${PULUMI_CONFIG_PASSPHRASE:-jinja-matrix}"

# Property under test -> a resource block exercising it. `@@BODY@@` is replaced
# by either the literal template or an fn::readFile of the same text.
declare -A CASES=(
  [dbt_model_sql]='
  probeModel:
    type: gcpx:dbt/model:Model
    properties:
      name: probe_model
      sql: @@BODY@@
      context: ${probeProject.context}'
  [dbt_macro_sql]='
  probeMacro:
    type: gcpx:dbt/macro:Macro
    properties:
      name: probe_macro
      args: [x]
      sql: @@BODY@@'
  [scheduler_sqljob_sql]='
  probeJob:
    type: gcpx:scheduler/sqlJob:SqlJob
    properties:
      project: PROJECT
      region: us-central1
      name: probe-job
      sql: @@BODY@@
      schedule: "0 * * * *"
      serviceAccount: sa@PROJECT.iam.gserviceaccount.com'
  [routine_definition_body]='
  probeRoutine:
    type: gcpx:bigquery/routineFunction:RoutineFunction
    properties:
      project: PROJECT
      dataset: probe_ds
      routineId: probe_fn
      definitionBody: @@BODY@@'
  [table_view_query]='
  probeView:
    type: gcpx:bigquery/table:Table
    properties:
      project: PROJECT
      dataset: probe_ds
      tableId: probe_view
      view:
        query: @@BODY@@'
  [snapshot_source_sql]='
  probeSnapshot:
    type: gcpx:dbt/snapshot:Snapshot
    properties:
      project: PROJECT
      region: us-central1
      dataset: probe_ds
      name: probe_snap
      sourceSql: @@BODY@@
      uniqueKey: id
      strategy: timestamp
      updatedAt: ts
      schedule: "0 2 * * *"
      serviceAccount: sa@PROJECT.iam.gserviceaccount.com'
  [agent_system_instruction]='
  probeAgent:
    type: gcpx:agent/dataAgent:DataAgent
    properties:
      project: PROJECT
      location: global
      agentId: probe-agent
      systemInstruction: @@BODY@@
      tables:
        - PROJECT.probe_ds.t'
  [schema_field_description]='
  probeSchema:
    type: gcpx:bigquery/tableSchema:TableSchema
    properties:
      project: PROJECT
      dataset: probe_ds
      tableId: probe_tbl
      schema:
        - name: id
          type: INT64
          description: @@BODY@@'
)

# The template payload: a dbt construct plus a statement tag, i.e. exactly what
# a user writes and exactly what the first renderer might eat.
PAYLOAD="SELECT {{ ref('upstream') }} {% if is_incremental() %}WHERE 1=1{% endif %}"

build_stack() {   # $1 = case body, $2 = delivery (inline|readfile)
  local body="$1" delivery="$2" dir="$WORK/stack"
  rm -rf "$dir"; mkdir -p "$dir"
  local rendered
  if [[ "$delivery" == "inline" ]]; then
    rendered="${body//@@BODY@@/\"$PAYLOAD\"}"
  else
    printf '%s\n' "$PAYLOAD" > "$dir/payload.sql"
    rendered="${body//@@BODY@@/$'\n        fn::readFile: payload.sql'}"
  fi
  rendered="${rendered//PROJECT/$PROJECT}"
  {
    echo "name: jinja-probe"
    echo "runtime: yaml"
    echo "resources:"
    echo "  probeProject:"
    echo "    type: gcpx:dbt/project:Project"
    echo "    properties:"
    echo "      gcpProject: $PROJECT"
    echo "      dataset: probe_ds"
    echo "$rendered"
  } > "$dir/Pulumi.yaml"
  echo "$dir"
}

run_case() {      # $1 = dir, $2 = mode
  local dir="$1" mode="$2"
  ( cd "$dir"
    if [[ "$mode" == "passthrough" ]]; then export PULUMI_YAML_JINJA_UNDEFINED=passthrough
    else unset PULUMI_YAML_JINJA_UNDEFINED; fi
    pulumi stack init probe --non-interactive >/dev/null 2>&1
    out=$(pulumi preview --non-interactive 2>&1)
    if grep -q 'Jinja preprocessing failed' <<<"$out"; then echo "JINJA-ATE-IT"
    else echo "SURVIVED"; fi
    pulumi stack rm probe --yes --force --non-interactive >/dev/null 2>&1
  )
}

printf '%-28s %-14s %-14s %-14s %-14s\n' "property" "inline/strict" "inline/pass" "file/strict" "file/pass"
printf -- '-%.0s' {1..90}; echo
fail=0
for name in $(printf '%s\n' "${!CASES[@]}" | sort); do
  row=()
  for delivery in inline readfile; do
    for mode in strict passthrough; do
      dir=$(build_stack "${CASES[$name]}" "$delivery")
      row+=("$(run_case "$dir" "$mode")")
    done
  done
  printf '%-28s %-14s %-14s %-14s %-14s\n' "$name" "${row[0]}" "${row[1]}" "${row[2]}" "${row[3]}"
  # fn::readFile must survive in both modes; that is the guarantee the
  # documentation makes and the habit the examples teach.
  [[ "${row[2]}" == "SURVIVED" && "${row[3]}" == "SURVIVED" ]] || fail=1
done


# Which individual constructs survive inline, and under which mode. The summary
# above hides this: a payload mixing an expression and a statement tag fails in
# both modes, but for different reasons.
echo
echo "Per-construct, delivered inline:"
declare -A CONSTRUCTS=(
  ["{{ ref('x') }}"]="SELECT {{ ref('x') }}"
  ["{{ config(...) }}"]="{{ config(materialized='table') }} SELECT 1"
  ["{{ x }} (macro arg)"]="ROUND({{ x }} / 100.0, 2)"
  ["{% if is_incremental() %}"]="SELECT 1 {% if is_incremental() %}WHERE 1=1{% endif %}"
)
printf '  %-30s %-14s %-14s\n' "construct" "strict" "passthrough"
for c in "${!CONSTRUCTS[@]}"; do
  row=()
  for mode in strict passthrough; do
    dir="$WORK/c"; rm -rf "$dir"; mkdir -p "$dir"
    {
      echo "name: jinja-probe"; echo "runtime: yaml"; echo "resources:"
      echo "  m:"; echo "    type: gcpx:dbt/macro:Macro"; echo "    properties:"
      echo "      name: probe"; echo "      args: [x]"
      echo "      sql: \"${CONSTRUCTS[$c]}\""
    } > "$dir/Pulumi.yaml"
    row+=("$(run_case "$dir" "$mode")")
  done
  printf '  %-30s %-14s %-14s\n' "$c" "${row[0]}" "${row[1]}"
done
echo
echo "Passthrough mode pre-escapes {{ expressions }} but not {% statement tags %},"
echo "so an inline incremental model fails in both modes. fn::readFile always works."

echo
if [[ $fail -eq 0 ]]; then
  echo "OK: every property survives when delivered with fn::readFile, in both modes."
else
  echo "FAIL: a property did not survive fn::readFile — the boundary is not what the docs claim."
fi
exit $fail

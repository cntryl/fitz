#!/usr/bin/env bash
set -euo pipefail

results_path="${1:-target/bench_results.json}"
release_ids_path="${2:-config/bench_release_ids.txt}"
stress_dir="${3:-target/stress}"

test -f "$results_path"
test -f "$release_ids_path"
test -d "$stress_dir"

if find "$stress_dir" -type d \( -name 'tierr-*' -o -name 'tier-*' \) -print -quit | grep -q .; then
  echo "legacy Tier 4 suite IDs found under $stress_dir" >&2
  exit 1
fi

jq -e --rawfile release_ids "$release_ids_path" '
  ($release_ids
    | split("\n")
    | map(gsub("#.*$"; "") | gsub("^\\s+|\\s+$"; ""))
    | map(select(length > 0))) as $ids
  | if ($ids | length) != 14 then
      error("the release manifest must contain exactly 14 primary rows")
    else . end
  | ([.records[] | select(.id as $id | $ids | index($id))]) as $release_records
  | if ($release_records | length) != ($ids | length) then
      error("release manifest IDs are missing from the summary")
    else . end
  | if all($release_records[];
      .metric == "throughput_ops_per_s"
      and .metadata.correctness_passed == "true"
      and (.metadata.quality == "acceptable" or .metadata.quality == "excellent")
    ) then
      true
    else
      error("every release primary row must pass correctness with acceptable quality")
    end
' "$results_path" >/dev/null

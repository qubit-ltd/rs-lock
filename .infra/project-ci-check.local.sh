#!/usr/bin/env bash
set -euo pipefail
project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
for relative_path in .infra/bin/align-ci.sh .infra/bin/ci-check.sh; do
    if [[ ! -f "$project_root/$relative_path" ]]; then
        printf 'missing documented entry point: %s\n' "$relative_path" >&2
        exit 1
    fi
done
exec python3 "$project_root/.infra/check-user-guides.py" --project "$project_root"

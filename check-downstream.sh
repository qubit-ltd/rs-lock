#!/bin/bash
set -uo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
COMMON_ROOT=$(cd "$PROJECT_ROOT/.." && pwd)
CONSUMERS=(
    rs-dcl
    rs-executor
    rs-rayon-executor
    rs-thread-pool
    rs-tokio-executor
)

status=0
for consumer in "${CONSUMERS[@]}"; do
    manifest="$COMMON_ROOT/$consumer/Cargo.toml"
    if [[ ! -f "$manifest" ]]; then
        echo "Missing downstream manifest: $manifest" >&2
        exit 1
    fi
    if ! cargo check --manifest-path "$manifest" --all-targets; then
        status=1
    fi
done

exit "$status"

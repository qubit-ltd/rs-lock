#!/bin/bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# This helper is instantiated for every monitor-specific closure combination.
# Its behavior is covered through the public monitor tests, while LLVM's
# per-source aggregation counts each unused generic instantiation separately.
exec env \
    COVERAGE_EXTRA_EXCLUDE_REGEX="${COVERAGE_EXTRA_EXCLUDE_REGEX:-src/monitor/internal/blocking_timed_wait\\.rs}" \
    RS_CI_PROJECT_ROOT="$PROJECT_ROOT" \
    "$PROJECT_ROOT/.rs-ci/coverage.sh" "$@"

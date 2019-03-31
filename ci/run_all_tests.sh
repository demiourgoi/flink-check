#!/bin/bash

set -eu

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

FLINK_CHECK_ROOT="${SCRIPT_DIR}/../flink-check"
FLINK_CHECK_EXAMPLES_ROOT="${SCRIPT_DIR}/../flink-check-examples"

echo "Using SSCHECK_CORE_ROOT=[${SSCHECK_CORE_ROOT}]"
echo "Using FLINK_CHECK_ROOT=[${FLINK_CHECK_ROOT}]"
echo "Using FLINK_CHECK_EXAMPLES_ROOT=[${FLINK_CHECK_EXAMPLES_ROOT}]"

function install_flink_check {
    rm -rf  ~/.ivy2/local/es.ucm.fdi/
    for repo in ${SSCHECK_CORE_ROOT} ${FLINK_CHECK_ROOT}
    do
        echo
        echo '---------------------------------------'
        echo "Installing $(basename ${repo})"
        echo '---------------------------------------'
        echo 
        cd ${repo}
        sbt -no-colors 'publishLocal'
        cd -
    done
}

function run_tests {
    for repo in ${FLINK_CHECK_ROOT} ${FLINK_CHECK_EXAMPLES_ROOT}
    do
        echo
        echo '---------------------------------------'
        echo "Running tests for $(basename ${repo})"
        echo '---------------------------------------'
        echo
        cd ${repo}
        sbt -no-colors 'test'
        cd -
    done
}

install_flink_check
run_tests
# Continuous integration

## Script to run all test

Usage:

```bash
# Adapt to your paths
export SSCHECK_CORE_ROOT="${HOME}/git/investigacion/demiourgoi/sscheck-core"

function run_tests {
  log_file="run_all_tests-$(date +%Y-%m-%d_%H-%M-%S).log"
  ./run_all_tests.sh 2>&1 | tee "${log_file}"
  echo
  echo "log_file=[${log_file}]"
  grep -ni 'Passed: Total' "${log_file}"
}
run_tests
```

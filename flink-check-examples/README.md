# Flink Check examples 

## Build instructions 

This project builds with [sbt](https://www.scala-sbt.org/).

First clone, build and publish locally the master branch of flink-check 
and its dependencies. This is required until we do a release on bitbucket.  

```bash
# Update to the paths of your git clones
export SSCHECK_CORE_ROOT="${HOME}/git/investigacion/demiourgoi/sscheck-core"
export FLINK_CHECK_ROOT="${HOME}/git/investigacion/demiourgoi/flink-check/flink-check"

function install_flink_check {
    rm -rf  ~/.ivy2/local/es.ucm.fdi/
    for repo in ${SSCHECK_CORE_ROOT} ${FLINK_CHECK_ROOT}
    do
        cd ${repo}
        sbt 'publishLocal'
        cd -
    done
}
 
install_flink_check
``` 

That should leave some jars on `~/.ivy2/cache/es.ucm.fdi/sscheck-core`. 
Then import the project on IntelliJ or run it directly with sbt.

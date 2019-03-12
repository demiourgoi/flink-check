# Flink Check

ScalaCheck for Apache Flink

## Development environment

### Using sscheck-core and other dependencies locally 

For devel it's useful to use a local version of sscheck-core 
that has not been released to bintray yet. For that: 

1. Clone [sscheck-core](https://github.com/demiourgoi/sscheck-core) 
2. Launch SBT for that project and run `clean` and then `+publish-local`. 
That should add the jars to `~/.ivy2/cache/es.ucm.fdi/sscheck-core`
3. Run `sbt update` in this project 

Double check the versions are ok if there are problems. See 
[Releasing sscheck](https://github.com/demiourgoi/sscheck/wiki/%5BInternal%5D-Releasing-sscheck)
in the wiki for details

### IntelliJ

- Set "Use SBT shell for build and import" on the project setting
to avoid ["Some keys were defined with the same name" issue](https://stackoverflow.com/questions/47084795/strange-sbt-bug-where-i-cannot-import-sbt-project-due-to-keys-colliding-with-the#47777860)

### Troubleshooting

#### Sbt `[error] java.util.concurrent.ExecutionException: java.lang.OutOfMemoryError: Metaspace`

Seems to be triggered from time to time when adding `import org.apache.flink.streaming.api.scala.extensions._`.
A workaround is restarting the Sbt shell. Looks like a Flink or Sbt issue.  

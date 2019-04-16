# Flink Check

Flink Check is a property-based testing library for Apache Flink that
extends [ScalaCheck](https://www.scalacheck.org/) with linear temporal
logic operators that are suitable for testing Flink data stream transformations.

Flink Check is based on [sscheck](https://github.com/demiourgoi/sscheck),a property-based testing library for Apache Spark, and so it depends on the project [sscheck-core](https://github.com/demiourgoi/sscheck-core), that contains code that is common to both sscheck and Flink Check,
in particular the implementation of the LTLss logic that
the system is based on. LTLss is a discrete time linear
temporal logic for finite words, that is described in 
detail in the paper [Property-based testing for Spark Streaming](https://arxiv.org/abs/1812.11838).

## Getting started

Flink Check has been tested with Scala 2.11.8 and Apache Flink 1.7.2.

See some example properties in the [flink-check-examples project](../flink-check-examples). See also the **scaladoc**.

- [flink-check scaladoc](https://demiourgoi.github.io/doc/flink-check/0.0.1-SNAPSHOT/scala-2.11/api)
- [sscheck-core scaladoc](https://demiourgoi.github.io/doc/sscheck-core/0.0.1-SNAPSHOT/scala-2.11/api)

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

Another option is running all tests following [these instructions](../ci/README.md), which install sscheck-core and flink-check in the system.

### IntelliJ

- Set "Use SBT shell for build and import" on the project settings
to avoid ["Some keys were defined with the same name" issue](https://stackoverflow.com/questions/47084795/strange-sbt-bug-where-i-cannot-import-sbt-project-due-to-keys-colliding-with-the#47777860)

### Troubleshooting

#### Sbt `[error] java.util.concurrent.ExecutionException: java.lang.OutOfMemoryError: Metaspace`

That means [Sbt needs more memory](https://stackoverflow.com/questions/8331135/how-to-prevent-java-lang-outofmemoryerror-permgen-space-at-scala-compilation)
A workaround is shutting down Sbt, exporting `SBT_OPTS` to increase `MaxPermSize` and other options depending on 
your host capabilities, and relaunch Sbt. Options are JVM options so they depend on your JRE version.

```
export SBT_OPTS="-XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=2G -Xmx2G"
```
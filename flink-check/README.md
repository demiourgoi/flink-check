# Flink Check

Flink Check is a property-based testing library for Apache Flink that
extends [ScalaCheck](https://www.scalacheck.org/) with linear temporal
logic operators that are suitable for testing Flink data stream transformations. See this [open access paper](https://ieeexplore.ieee.org/document/8868163) for detailed description of Flink Check.

Flink Check is based on [sscheck](https://github.com/demiourgoi/sscheck),a property-based testing library for Apache Spark, and so it depends on the project [sscheck-core](https://github.com/demiourgoi/sscheck-core), that contains code that is common to both sscheck and Flink Check,
in particular the implementation of the LTLss logic that
the system is based on. LTLss is a discrete time linear
temporal logic for finite words, that is described in 
detail in the paper [Property-based testing for Spark Streaming](https://arxiv.org/abs/1812.11838).

## Getting started

Flink Check has been tested with Scala 2.11.8 and Apache Flink 1.8.0.

See an example property [PollutionFormulas](../flink-check-examples/src/test/scala/es/ucm/fdi/sscheck/flink/demo/pollution/PollutionFormulas.scala) in the [flink-check-examples project](../flink-check-examples). See also the **scaladoc**.

- [flink-check scaladoc](https://demiourgoi.github.io/doc/flink-check/0.0.2/scala-2.11/api)
- [sscheck-core scaladoc](https://demiourgoi.github.io/doc/sscheck-core/0.4.1/scala-2.11/api)

Flink Check is available as a maven dependency in [bintray](https://bintray.com/juanrh/maven/flink-check).

## How properties are executed 

Properties are defined by extending the trait `DataStreamTLProperty`, which includes the method `forAllDataStream` for defining a property that for each test case generates a `DataStream[In]`, applies the `testSubject` to produce a `DataStream[Out]`, and checks those data streams fulfill the specified `formula`.

```scala
type TSeq[A] = Seq[TimedElement[A]]
type TSGen[A] = Gen[TSeq[A]]
type Letter[In, Out] = (DataSet[TimedElement[In]], DataSet[TimedElement[Out]])
def forAllDataStream[In : TypeInformation, 
                    Out : TypeInformation](generator: TSGen[In])
                                          (testSubject: (DataStream[In]) => DataStream[Out])
                                          (formula: FlinkFormula[Letter[In, Out]])
                                          (implicit pp1: TSGen[In] => Pretty): Prop 
```

Each test case is executed in two stages.

- _Test case exercise_: First we generate the test case in memory as a `Seq[TimedElement[Input]]` for `case class TimedElement[T](timestamp: Long, value: T)`, convert it into a `DataStream[In]` and apply the test subject to get a `DataStream[Out]`. The input and output DataStream are then stored in Flink's configured default file system. As the generated test case is a finite sequence, the resulting streams are also finite. 
- _Test case evaluation_: In order to check the formula we evaluate the test case by reading the input and output streams from the file system. Each stream is split into windows as a `Iterator[TimedWindow[T]]` for `case class TimedWindow[T](timestamp: Long, data: DataSet[TimedElement[T]])`. We zip the window iterators for the input and output streams, obtaining a sequence of pairs of `TimedWindow` that is used as a word that is feed to `formula` to check whether the test case passes or not. 
That means [Specs2](https://etorreborre.github.io/specs2/) matchers on Flink data sets can be used in the formulas, see [DataSetMatchers](https://demiourgoi.github.io/doc/flink-check/0.0.1-SNAPSHOT/scala-2.11/api/#es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers$).

### How time is handled in properties

As the LTLss logic uses discrete time, while Flink streams are continuous, we have to perform conversions into the discrete and continous world in both directions:

- Discrete generators are made continous: the generator objects [WindowGen](https://demiourgoi.github.io/doc/sscheck-core/0.0.1-SNAPSHOT/scala-2.11/api/index.html#es.ucm.fdi.sscheck.gen.WindowGen) and [PStreamGen](https://demiourgoi.github.io/doc/sscheck-core/0.0.1-SNAPSHOT/scala-2.11/api/#es.ucm.fdi.sscheck.gen.PStreamGen) in sscheck-core generate sequences of sequences of values, where each nested sequences is a window. The function [`FlinkGenerators.tumblingTimeWindows`](https://demiourgoi.github.io/doc/flink-check/0.0.1-SNAPSHOT/scala-2.11/api/#es.ucm.fdi.sscheck.gen.flink.FlinkGenerators$) converts those into a `Gen[Seq[TimedElement[A]]]` by interpreting each nested sequence as a tumbling window of a specified size. Inside each window, the timestamp of each element is a random value between the window start (inclusive) and the windows end (exclusive), obtained using a uniform distribution. 
  - Optionally a start time can be provided, but `0` is usually conventient because it makes it easier to interpret the test execution logs.
  - Besides combining generators from sscheck-core with `tumblingTimeWindows`, any generator of `Seq[TimedElement[In]]` can be used. The only requisite is that elements are generated in increasing order of timestamp. 
- The continous Flink streams for input and output are discretized for test case evaluation. So they can be interpreted as letters in the LTLss logic. For that a [StreamDiscretizer](https://demiourgoi.github.io/doc/flink-check/0.0.1-SNAPSHOT/scala-2.11/api/#es.ucm.fdi.sscheck.prop.tl.flink.StreamDiscretizer) has to be specified for the formula, which is just a criteria for splitting the streams into windows. Currently we support tumbling time windows, and sliding time windows. 

So the conversions between the discrete and continuous worlds are performed using time based windowing criteria. In a property we can use different windowing criteria for the generator and for the formula, although both should use the same start time --which is `0` by default for all criteria.

#### How we use event time

Flink Check relies on **event time** to make it all work. The timestamp in `TimedElement` is assigned as the event time in the generated input stream, using `DataStream.assignAscendingTimestamps`, which works well because we generate the elements in that order. We made this choice in order to _make tests more predictable_, as then event timing is not so dependent on the performance of the hardware where tests are running. Test subjects don't need to be aware of event time, and the `StreamExecutionEnvironment` that is created for each test case by the default configuration of the trait `DataStreamTLProperty` --which can be changed by overriding the method `buildFreshStreamExecutionEnvironment`-- is already configured to use event time. 

For scenarios where event time is relevant, we have two options. We can use the function [FlinkGenerators.eventTimeToFieldAssigner](https://demiourgoi.github.io/doc/flink-check/0.0.1-SNAPSHOT/scala-2.11/api/#es.ucm.fdi.sscheck.gen.flink.FlinkGenerators$) to specify a function `fieldAssigner: Long => A => A` that sets the relevant fields of the generated elements dependending on the assigned event time, so the event time is consistent with the generated timestamps. For example for a `case class SensorData(timestamp: Long, sensor_id: Int, concentration: Double)` we can use `ts => _.copy(timestamp = ts)` as the field assigner.
Another option is using a custom generator of generator of `Seq[TimedElement[In]]` where we have full control of the timestamps. 

## Development environment

### Using sscheck-core and other dependencies locally

For devel it's useful to use a local version of sscheck-core
that has not been released to bintray yet. For that: 

1. Clone [sscheck-core](https://github.com/demiourgoi/sscheck-core) 
2. Launch SBT for that project and run `clean` and then `publishLocal`.
That should add the jars to `~/.ivy2/cache/es.ucm.fdi/sscheck-core`
3. Run `sbt update` in this project

Another option is running all tests following [these instructions](../ci/README.md), which:

1. Install sscheck-core and flink-check in the system
2. And then run all the
tests in flink-check and flink-check-examples, which
_includes some example Flink Check properties_

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

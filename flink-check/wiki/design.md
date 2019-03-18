# Design 

## Tenets

- Test subjects are functions that transform `DataStream` objects, that can 
be used in actual production code, not models of production code.
- During a test, test subjects are executed by the Flink runtime.
- Assertions can use the Flink API to trigger distributed computations. Even
though we won't validate test execution in a distributed setting in the first
iteration, the design is not impediment for supporting distributed computing 
in the future
- Parallel sources and operations are used in the tests.  
 
## Test case execution 

One problem is that Flink uses constant time, while sscheck's logic is discrete. For that
reason, the logic fits well with Spark Streaming's micro-batching approach. Even though we
can discretize a Flink into a similar format using tumbling windows, sscheck's `Formula`
trait contains a bind operator that is able to define different computations depending on
the contents on each letter/window/micro batch. On the other hand the Flink DAG is quite
rigid and fixed for its whole execution. 
So the **main idea** is separating the _test case exercise_, that applies the test subject 
to a generated test case to produce the output of the test case, and the _test case evaluation_,
that checks the formula and its assertions on the on a discretization of the input and output 
streams for a test case.

On a first version we only support event time because it leads to more deterministic behaviour,
which is a desired property of a testing framework. It's also easy to generate time stamps for 
the generated records, in an ascending order so we have no problem with late events. We can 
cover late events on a later iteration of the testing framework 

### First approach: reconstruct windows on batch 

Here for the _test case exercise_ we use something like `case class TimedValue[T](timestamp: Long, value: T)`
to add the event time timestamp for all the records of both the input and output stream, using a 
`ProcessFunction` along the lines of `AddTimestamp` below:

```scala
class AddTimestamp[T] extends ProcessFunction[T, TimedValue[T]] {
  override def processElement(value: T, 
                              ctx: ProcessFunction[T, TimedValue[T]]#Context,
                              out: Collector[TimedValue[T]]): Unit = {
    out.collect(TimedValue(ctx.timestamp(), value))
}
```

and we just store that using `Streaming File Sink`. Then for the _test case evaluation_ we load the recorded input an
output streams as a pair of data sets, split each of them into a sequence of data sets as tumbling windows, and 
traverse that sequence applying the formula. This has: 

- pros
    - simple: difficult to do it wrong
    - easy to implement, for the first iteration 
    - minimal intermission with the test subject: higher fidelity of the test case exercise
- cons: 
    - the test case is always exercised entirely, even when just evaluating the first letter
    would fail the test
    - mitigation
        - overlap test case exercise and evaluation for different test cases (e.g. using a task 
        pool to generalize to `n` test cases in parallel)
        - the test case evaluation interface should accept an iterable of tuples of data sets,
        so it's easy to support overlapping of test case exercise and evaluation in the future 

### Second approach: overlap test case exercise and evaluation

This also requires a source that can be interrupted (see [How to stop a flink streaming job from program](https://stackoverflow.com/questions/44441153/how-to-stop-a-flink-streaming-job-from-program)).
A similar technique using using `Streaming File Sink` with a `BucketAssigner` that uses the `timestamp`
field of a `TimedValue` could be used to assign records to windows on the flight, provided a start time
and tumbling window size.

Akka or some RPC mechanism could be used to signal test case evaluation completion.  


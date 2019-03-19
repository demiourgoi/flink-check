package es.ucm.fdi.sscheck.flink.poc

import java.nio.file.{Files, Path => JPath}
import java.time.Instant

import es.ucm.fdi.sscheck.gen.BatchGen
import es.ucm.fdi.sscheck.prop.tl.{Formula, Time => SscheckTime}
import es.ucm.fdi.sscheck.prop.tl.Formula._

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.junit.runner.RunWith
import org.scalacheck.{Gen, Prop}
import org.specs2.Specification
import org.specs2.matcher.ResultMatchers
import org.specs2.runner.JUnitRunner

import scala.language.{implicitConversions, reflectiveCalls}
import scala.collection.JavaConverters._

object TimedValue {
  private[this] def tumblingWindowIndex(windowSizeMillis: Long, startTimestamp: Long)(timestamp: Long): Int = {
    val timeOffset = timestamp - startTimestamp
    (timeOffset / windowSizeMillis).toInt
  }

  def tumblingWindows[T](windowSize: Time)(data: DataSet[TimedValue[T]]): Iterator[TimedWindow[T]] =
    if (data.count() <= 0) Iterator.empty
    else {

      val windowSizeMillis = windowSize.toMilliseconds
      val startTimestamp = data.map{_.timestamp}.reduce(scala.math.min(_, _)).collect().head
      val endTimestamp = data.map{_.timestamp}.reduce(scala.math.max(_, _)).collect().head
      val endingWindowIndex = tumblingWindowIndex(windowSizeMillis, startTimestamp)(endTimestamp)

      Iterator.range(0, endingWindowIndex + 1).map { windowIndex =>
        val windowData = data.filter{record =>
          tumblingWindowIndex(windowSizeMillis, startTimestamp)(record.timestamp) == windowIndex
        }
        TimedWindow(startTimestamp + windowIndex*windowSizeMillis, windowData)
      }
  }
}
/** @param timestamp milliseconds since epoch */
case class TimedValue[T](timestamp: Long, value: T)

/** Represents a time window with timed values. Note timestamp can be smaller than the earlier
  * element in data, for example in a tumbling window where a new event starts at a regular rate,
  * independently of the actual elements in the window
  * @param timestamp milliseconds since epoch for the start of the window
  * */
case class TimedWindow[T](timestamp: Long, data: DataSet[TimedValue[T]])

/** Converts each record into a [[TimedValue]], adding the timestamp provided
  * by [[ProcessFunction#Context]]. That means this will fail if the time characteristic
  * is ProcessingTime. So far this has been tested with event time.
  * */
class AddTimestamp[T] extends ProcessFunction[T, TimedValue[T]] {
  override def processElement(value: T, ctx: ProcessFunction[T, TimedValue[T]]#Context,
                              out: Collector[TimedValue[T]]): Unit = {
    out.collect(TimedValue(ctx.timestamp(), value))
  }
}
object TimedInt {
  def apply(line: String) = {
    val parts = line.split(",")
    new TimedValue(parts(0).toLong, parts(1).toInt)
  }
}

object TestCaseGenerator {
  def createTempDir(testCaseName: String): JPath = {
    val tempDir = Files.createTempDirectory(testCaseName)
    tempDir.toFile.deleteOnExit()
    tempDir
  }

  // TODO: support parallelism using SplittableIterator
  // note ScalaCheck will already trigger the generation
  def batchesToStream[A : TypeInformation](batches: Seq[Seq[A]])
                        (windowSize: Time)
                        (implicit env: StreamExecutionEnvironment): DataStream[A] = {
    require(env.getStreamTimeCharacteristic == TimeCharacteristic.EventTime,
      println("Event time required for converting a PDStream value into a Flink DataStream"))

    // "Time-based windows have a start timestamp (inclusive) and an end timestamp (exclusive) that together
    // describe the size of the window."
    // https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/operators/windows.html
    val timestampOffsetGen = Gen.choose(min=0, max=(windowSize.toMilliseconds)-1)
    val startEpoch = Instant.now().toEpochMilli
    println(s"batches.zipWithIndex=[${batches.zipWithIndex}]")
    val timedBatches = batches.zipWithIndex.flatMap{case (batch, i) =>
      batch.map{value =>
        val offset = timestampOffsetGen.sample.getOrElse(0L)
        assume(offset >= 0)
        val timestamp = startEpoch + (i * (windowSize.toMilliseconds)) + offset
        TimedValue(timestamp, value)
      }
    }.sortBy(_.timestamp)

    println(s"timedBatches=[${timedBatches}]")

    // This works fine with event time because [watermarks](https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/event_time.html#event-time-and-watermarks)
    // won't find late events, as timestamps are sorted. This wouldn't work so well with unsorted time stamps
    env.fromCollection(timedBatches)
      .assignAscendingTimestamps( _.timestamp)
      .map(_.value)
  }
}

@RunWith(classOf[JUnitRunner])
class WindowRecorderPoC
  extends Specification with ResultMatchers {

  def is =
    sequential ^ s2"""
where we exercise a test case, store it in a pair of files, and then we read and evaluate
      the recorded test execution $generateExerciseAndEvaluateTestCase
and again $generateExerciseAndEvaluateTestCase
"""

  def fixture = new {
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate event time
  }

  def subjectAdd(xs: DataStream[Int]): DataStream[Int] = xs.map{_ + 1}

  def generateExerciseAndEvaluateTestCase = {
    val f = fixture
    val testCaseInputRecordPath = TestCaseGenerator.createTempDir("WindowRecorderPoC-Input")
    val testCaseOutputRecordPath = TestCaseGenerator.createTempDir("WindowRecorderPoC-Output")
    println(
      s"""
         |testCaseInputRecordPath=[${testCaseInputRecordPath}],
         |testCaseOutputRecordPath=[${testCaseOutputRecordPath}]"""".stripMargin)

    val letterSize = Time.seconds(1)

    {
      implicit val env: StreamExecutionEnvironment = f.env

      println(s"Starting test case generation and exercise")
      val gen = BatchGen.always(BatchGen.ofNtoM(3, 5, Gen.choose(0,100)), 3)
      val testCase = gen.sample.get
      println(s"Generated test case ${testCase}")
      val input = TestCaseGenerator.batchesToStream(testCase)(letterSize)
      val output = subjectAdd(input)

      val timedInput: DataStream[TimedValue[Int]] = input.process(new AddTimestamp())
      val timedOutput = output.process(new AddTimestamp())
      def storeDataStream[A : TypeInformation](stream: DataStream[A])(outputDir: String): Unit = {
        val format = new TypeSerializerOutputFormat[A]
        format.setOutputFilePath(new Path(outputDir))
        stream.writeUsingOutputFormat(format)
      }
      storeDataStream(timedInput)(testCaseInputRecordPath.toString)
      storeDataStream(timedOutput)(testCaseOutputRecordPath.toString)
      env.execute()
      println(s"Completed test case generation and exercise")
    }

    type U = (DataSet[TimedValue[Int]], DataSet[TimedValue[Int]])
    val alwaysPositiveOutputFormula: Formula[U] = always(nowTime[U]{ (letter, time) =>
      val (_input, output) = letter
      val failingRecords = output.map{_.value}.filter{x => ! (x >= 0)}
      failingRecords.count() == 0
    }) during 3 // use 4 for none due to unconclusive always
    var alwaysPositiveOutputNextFormula = alwaysPositiveOutputFormula.nextFormula

    {
      println(s"Starting test case evaluation")
      val env = ExecutionEnvironment.createLocalEnvironment(3)

      def readRecordedStream[T : TypeInformation](path: String) = {
        val timedInputInputFormat = new TypeSerializerInputFormat[TimedValue[T]](
          implicitly[TypeInformation[TimedValue[T]]])
        env.readFile(timedInputInputFormat, path)
      }

      val timedInput = readRecordedStream[Int](testCaseInputRecordPath.toString)
      val timedOutput = readRecordedStream[Int](testCaseOutputRecordPath.toString)

      val inputWindows = TimedValue.tumblingWindows(letterSize)(timedInput)
      val outputWindows = TimedValue.tumblingWindows(letterSize)(timedOutput)
      inputWindows.zip(outputWindows).zipWithIndex.foreach{case ((inputWindow, outputWindow), windowIndex) =>
        val windowStartTimestamp = inputWindow.timestamp
        assume(windowStartTimestamp == outputWindow.timestamp, println(s"input and output window are not aligned"))
        println(s"\nChecking window #$windowIndex with timestamp ${windowStartTimestamp}")
        inputWindow.data.map{x => s"input: $x"}.print()
        outputWindow.data.map{x => s"output: $x"}.print()
        println(s"alwaysPositiveOutputNextFormula.result=[${alwaysPositiveOutputNextFormula.result}]")
        val currentLetter: U = (inputWindow.data, outputWindow.data)
        alwaysPositiveOutputNextFormula =
          alwaysPositiveOutputNextFormula.consume(SscheckTime(windowStartTimestamp))(currentLetter)
      }
    }

    println(s"\n\nalwaysPositiveOutputNextFormula.result=[${alwaysPositiveOutputNextFormula.result}]")
    println(s"Completed test case evaluation")
    alwaysPositiveOutputNextFormula.result === Some(Prop.True)
  }
}

/*
Bug

Generated test case PDStream(Batch(1, 23, 94), Batch(34, 3, 16, 93, 31), Batch(68, 48, 35))
timedBatches=[List(TimedValue(1552970062846,1), TimedValue(1552970063331,94), TimedValue(1552970063360,23), TimedValue(1552970063768,31), TimedValue(1552970063897,93), TimedValue(1552970063944,34), TimedValue(1552970064189,16), TimedValue(1552970064550,3), TimedValue(1552970064686,35), TimedValue(1552970064862,68), TimedValue(1552970065432,48))]
Completed test case generation and exercise
Starting test case evaluation

Checking window #0 with timestamp 1552970062846
input: TimedValue(1552970063360,23)
input: TimedValue(1552970063768,31)
input: TimedValue(1552970063331,94)
input: TimedValue(1552970062846,1)
output: TimedValue(1552970063331,95)

31 should be in the second window

>>> 1552970063768 - 1552970062846
922

The timestamp was assigned wrong on `TestCaseGenerator.batchesToStream`
* */

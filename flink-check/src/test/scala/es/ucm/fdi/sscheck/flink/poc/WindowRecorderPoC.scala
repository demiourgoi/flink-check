package es.ucm.fdi.sscheck.flink.poc

import java.nio.file.{Files, Path => JPath}
import java.time.Instant

import es.ucm.fdi.sscheck.gen.BatchGen
import es.ucm.fdi.sscheck.prop.tl.{Formula, Time => SscheckTime}
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import es.ucm.fdi.sscheck.prop.tl.flink.TimedElement
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
import org.slf4j.LoggerFactory
import org.slf4j.Logger

import scala.language.{implicitConversions, reflectiveCalls}
import scala.collection.JavaConverters._

/** Converts each record into a [[es.ucm.fdi.sscheck.prop.tl.flink.TimedElement]], adding the timestamp provided
  * by [[ProcessFunction#Context]]. That means this will fail if the time characteristic
  * is ProcessingTime. So far this has been tested with event time.
  * */
class AddTimestamp[T] extends ProcessFunction[T, TimedElement[T]] {
  override def processElement(value: T, ctx: ProcessFunction[T, TimedElement[T]]#Context,
                              out: Collector[TimedElement[T]]): Unit = {
    out.collect(TimedElement(ctx.timestamp(), value))
  }
}
object TimedInt {
  def apply(line: String) = {
    val parts = line.split(",")
    new TimedElement(parts(0).toLong, parts(1).toInt)
  }
}

object TestCaseGenerator {
  def createTempDir(testCaseName: String): JPath = {
    val tempDir = Files.createTempDirectory(testCaseName)
    tempDir.toFile.deleteOnExit()
    tempDir
  }

  // TODO: support parallelism using SplittableIterator
  /** Converts batches that represents a sequence of windows into a DataStream where is record is assigned
    * an event time computed interpreting the each batch in batches as a tumbling window of size windowSize
    * with the first window starting at startEpoch. Inside the window, the timestamp of each record is a
    * random value between the window start (inclusive) and the windows end (exclusive)
    *
    * Note: there is no warranty that for each window has a record with the window start time as timestamp.
    * This also allows this method to support empty windows.
    * Note: the DatasStream is created with [[StreamExecutionEnvironment#fromCollection]] so it is not parallel
    */
  def batchesToStream[A : TypeInformation](batches: Seq[Seq[A]])
                                          (windowSize: Time,
                                           startTime: Time = Time.milliseconds(Instant.now().toEpochMilli))
                                          (implicit env: StreamExecutionEnvironment): DataStream[A] = {
    require(env.getStreamTimeCharacteristic == TimeCharacteristic.EventTime,
      println("Event time required for converting a PDStream value into a Flink DataStream"))

    // "Time-based windows have a start timestamp (inclusive) and an end timestamp (exclusive) that together
    // describe the size of the window."
    // https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/operators/windows.html
    val timestampOffsetGen = Gen.choose(min=0, max=(windowSize.toMilliseconds)-1)
    println(s"batches.zipWithIndex=[${batches.zipWithIndex}]")
    val timedBatches = batches.zipWithIndex.flatMap{case (batch, i) =>
      batch.map{value =>
        val offset = timestampOffsetGen.sample.getOrElse(0L)
        val timestamp = startTime.toMilliseconds + (i * (windowSize.toMilliseconds)) + offset
        TimedElement(timestamp, value)
      }.sortBy(_.timestamp)
    }

    println(s"timedBatches=[${timedBatches}]") // FIXME remove

    // This works fine with event time because [watermarks](https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/event_time.html#event-time-and-watermarks)
    // won't find late events, as timestamps are sorted. This wouldn't work so well with unsorted time stamps
    env.fromCollection(timedBatches)
      .assignAscendingTimestamps( _.timestamp)
      .map(_.value)
  }
}

object WindowRecorderPoC {
  val logger = LoggerFactory.getLogger(WindowRecorderPoC.getClass)
}
@RunWith(classOf[JUnitRunner])
class WindowRecorderPoC
  extends Specification with ResultMatchers {

  import WindowRecorderPoC._

  def is =
    sequential ^ s2"""
where we exercise a test case, store it in a pair of files, and then we read and evaluate
      the recorded test execution $generateExerciseAndEvaluateTestCase
and again generateExerciseAndEvaluateTestCase
and again generateExerciseAndEvaluateTestCase
and again generateExerciseAndEvaluateTestCase
and again generateExerciseAndEvaluateTestCase
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
    logger.warn(
      s"""
         |testCaseInputRecordPath=[${testCaseInputRecordPath}],
         |testCaseOutputRecordPath=[${testCaseOutputRecordPath}]""".stripMargin)

    // FIXME: wrap this into some batch coordinate case class, as these two tend to go together
    val letterSize = Time.seconds(1)
    val startTime = Time.milliseconds(0)

    {
      implicit val env: StreamExecutionEnvironment = f.env

      logger.info(s"Starting test case generation and exercise")
      val gen = BatchGen.always(BatchGen.ofNtoM(3, 5, Gen.choose(0,100)), 3)
      val testCase = gen.sample.get
      logger.info(s"Generated test case ${testCase}")
      val input = TestCaseGenerator.batchesToStream(testCase)(letterSize, startTime)
      val output = subjectAdd(input)

      val timedInput: DataStream[TimedElement[Int]] = input.process(new AddTimestamp())
      val timedOutput = output.process(new AddTimestamp())
      def storeDataStream[A : TypeInformation](stream: DataStream[A])(outputDir: String): Unit = {
        val format = new TypeSerializerOutputFormat[A]
        format.setOutputFilePath(new Path(outputDir))
        stream.writeUsingOutputFormat(format)
      }
      storeDataStream(timedInput)(testCaseInputRecordPath.toString)
      storeDataStream(timedOutput)(testCaseOutputRecordPath.toString)
      env.execute()
      logger.info(s"Completed test case generation and exercise")
    }

    val threshold = 0
    type U = (DataSet[TimedElement[Int]], DataSet[TimedElement[Int]])
    val alwaysPositiveOutputFormula: Formula[U] = always(nowTime[U]{ (letter, time) =>
      val (_input, output) = letter
      output should foreachElement{_ > 0} and
      (output should foreachTimedElement{_.value > 0}) and
      (output should existsElement{_ > 0}) and
      (output should existsTimedElement{_.value > 0}) and
      // using explicit closure context fixed the "task no serializable" issue when capturing variables in the
      // assertion closure inside a non serializable test class
      (output should foreachElement(threshold){threshold =>_ > threshold}) and
      (output should foreachTimedElement(threshold){threshold => _.value > threshold}) and
      (output should existsElement(threshold){threshold => _ > threshold}) and
      (output should existsTimedElement(threshold){threshold => _.value > threshold})
    }) during 3 // use 4 for none due to unconclusive always
    var alwaysPositiveOutputNextFormula = alwaysPositiveOutputFormula.nextFormula
//
//    {
//      logger.info(s"Starting test case evaluation")
//      val env = ExecutionEnvironment.createLocalEnvironment(3)
//
//      def readRecordedStream[T : TypeInformation](path: String) = {
//        val timedInputInputFormat = new TypeSerializerInputFormat[TimedElement[T]](
//          implicitly[TypeInformation[TimedElement[T]]])
//        env.readFile(timedInputInputFormat, path)
//      }
//
//      val timedInput = readRecordedStream[Int](testCaseInputRecordPath.toString)
//      val timedOutput = readRecordedStream[Int](testCaseOutputRecordPath.toString)
//
//      val inputWindows = TimedValue.tumblingWindows(letterSize, startTime)(timedInput)
//      val outputWindows = TimedValue.tumblingWindows(letterSize, startTime)(timedOutput)
//      inputWindows.zip(outputWindows).zipWithIndex.foreach{case ((inputWindow, outputWindow), windowIndex) =>
//        val windowStartTimestamp = inputWindow.timestamp
//        assume(windowStartTimestamp == outputWindow.timestamp, logger.error(s"input and output window are not aligned"))
//        logger.debug(s"\nChecking window #$windowIndex with timestamp ${windowStartTimestamp}")
//        inputWindow.data.map{x => s"input: $x"}.print()
//        outputWindow.data.map{x => s"output: $x"}.print()
//        logger.debug(s"alwaysPositiveOutputNextFormula.result=[${alwaysPositiveOutputNextFormula.result}]")
//        val currentLetter: U = (inputWindow.data, outputWindow.data)
//        alwaysPositiveOutputNextFormula =
//          alwaysPositiveOutputNextFormula.consume(SscheckTime(windowStartTimestamp))(currentLetter)
//      }
//    }
//
//    logger.info(s"\n\nalwaysPositiveOutputNextFormula.result=[${alwaysPositiveOutputNextFormula.result}]")
//    logger.info(s"Completed test case evaluation")
//    alwaysPositiveOutputNextFormula.result === Some(Prop.True)

    ok
  }
}

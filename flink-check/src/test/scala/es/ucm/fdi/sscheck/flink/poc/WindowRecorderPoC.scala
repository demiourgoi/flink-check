package es.ucm.fdi.sscheck.flink.poc

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.matcher.{ResultMatchers, ThrownExpectations}
import org.specs2.runner.JUnitRunner

import scala.language.{implicitConversions, reflectiveCalls}
import scala.collection.JavaConverters._

object TimedValue {
  private[this] def tumblingWindowIndex(windowSizeMillis: Long, startTimestamp: Long)(timestamp: Long): Int = {
    val timeOffset = timestamp - startTimestamp
    (timeOffset / windowSizeMillis).toInt
  }

  def tumblingWindows[T](windowSize: Time)(
    data: DataSet[TimedValue[T]]): Iterator[DataSet[TimedValue[T]]] =
    if (data.count() <= 0) Iterator.empty
    else {

      val windowSizeMillis = windowSize.toMilliseconds
      val startTimestamp = data.map{_.timestamp}.reduce(scala.math.min(_, _)).collect().head
      val endTimestamp = data.map{_.timestamp}.reduce(scala.math.max(_, _)).collect().head
      val endingWindowIndex = tumblingWindowIndex(windowSizeMillis, startTimestamp)(endTimestamp)
        //(endTimestamp / windowSize.toMilliseconds).toInt

      Iterator.range(0, endingWindowIndex + 1).map { windowIndex =>
        data.filter{record =>
          tumblingWindowIndex(windowSizeMillis, startTimestamp)(record.timestamp) == windowIndex
        }
      }
  }
}
/** @param timestamp milliseconds since epoch */
case class TimedValue[T](timestamp: Long, value: T)

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

@RunWith(classOf[JUnitRunner])
class WindowRecorderPoC
  extends Specification with ResultMatchers with ThrownExpectations {

  def is =
    sequential ^ s2"""
        where we exercise a test case and store it in a pair of files binaryBar
        and then we read those files $readRecordedWindows
      """

  /* This works fine with event time because [watermarks](https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/event_time.html#event-time-and-watermarks)
  won't find late events, as timestamps are sorted. This wouldn't work so well with unsorted time stamps
  * */
  // expect csv with schema (timestamp millis, int value)
  val inputPDstreamDataPath = getClass.getResource("/timed_int_pdstream_01.csv").getPath
  def eventTimeIntStream(path: String)(implicit env: StreamExecutionEnvironment): DataStream[Int] = {
    env.readTextFile(path)
      .map(TimedInt(_))
      .assignAscendingTimestamps( _.timestamp)
      .map(_.value)
  }

  def discretize(xs: DataStream[Int]): WindowedStream[Int, Int, TimeWindow] =
    xs.keyBy(identity[Int] _)
    .timeWindow(Time.seconds(1))


  def fixture = new {
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate event time
  }

  def subjectAdd(xs: DataStream[Int]): DataStream[Int] = xs.map{_ + 1}
  def binaryBar = {
    val f = fixture
    val env: StreamExecutionEnvironment = f.env

    // we have to get from TimedInt to Int in order to be able to apply
    // the test subject as is: the timestamps are still on the Flink runtime
    val input = env.readTextFile(inputPDstreamDataPath)
      .map(TimedInt(_))
      .assignAscendingTimestamps( _.timestamp)
      .map(_.value)
    val output = subjectAdd(input)

    val timedInput: DataStream[TimedValue[Int]] = input.process(new AddTimestamp())
    val timedOutput = output.process(new AddTimestamp())

    val timedInputOutputFormat = {
      val format = new TypeSerializerOutputFormat[TimedValue[Int]]
      format.setOutputFilePath(new Path( "/tmp/WindowRecorderPoC/inputData"))
      format
    }
    timedInput.writeUsingOutputFormat(timedInputOutputFormat)
    val timedOutputOutputFormat = {
      val format = new TypeSerializerOutputFormat[TimedValue[Int]]
      format.setOutputFilePath(new Path( "/tmp/WindowRecorderPoC/outputData"))
      format
    }
    timedOutput.writeUsingOutputFormat(timedOutputOutputFormat)

//    timedOutput.map{x => s"output($x)"}.print()
//    timedInput.map{x => s"input($x)"}.print()
    env.execute()

    ok
  }

  def readRecordedWindows = {
    val env = ExecutionEnvironment.createLocalEnvironment(3)

    val timedInputInputFormat = new TypeSerializerInputFormat[TimedValue[Int]](
      implicitly[TypeInformation[TimedValue[Int]]])
    val inputFilePath = "/tmp/WindowRecorderPoC/inputData"
    val timedInput = env.readFile(timedInputInputFormat, inputFilePath)
    timedInput.print()
    println(s"timedInput.count ${timedInput.count()}\n\n")

    val timedOutputInputFormat = new TypeSerializerInputFormat[TimedValue[Int]](
      implicitly[TypeInformation[TimedValue[Int]]])
    val timedOutput = env.readFile(timedOutputInputFormat, "/tmp/WindowRecorderPoC/outputData")
    timedOutput.print()
    println(s"timedOutput.count ${timedOutput.count()}")

    println(s"initial timestamp for input = ${timedInput.map{_.timestamp}.reduce(scala.math.min(_, _)).collect().head}")
    println(s"initial timestamp for output = ${timedOutput.map{_.timestamp}.reduce(scala.math.min(_, _)).collect().head}")

    // TODO: add start window time, for the letter
    val outputWindows = TimedValue.tumblingWindows(Time.seconds(1))(timedOutput)
    outputWindows.zipWithIndex.foreach{case (outputWindow, i) =>
      println(s"\noutputWindow #$i")
      outputWindow.print()
    }

    val inputWindows = TimedValue.tumblingWindows(Time.seconds(1))(timedInput)
    inputWindows.zipWithIndex.foreach{case (inputWindow, i) =>
      println(s"\ninputWindow #$i")
      inputWindow.print()
    }

    ok
  }
}

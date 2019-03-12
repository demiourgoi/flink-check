package es.ucm.fdi.sscheck.flink.poc

import scala.language.implicitConversions
import scala.language.reflectiveCalls
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.matcher.{ResultMatchers, ThrownExpectations}
import org.specs2.runner.JUnitRunner

object TimedInt {
  def apply(line: String) = {
    val parts = line.split(",")
    new TimedInt(parts(0).toLong, parts(1).toInt)
  }
}
case class TimedInt(timestamp: Long, value: Int)

@RunWith(classOf[JUnitRunner])
class WindowRecorderPoC
  extends Specification with ResultMatchers with ThrownExpectations {

  def is =
    s2"""
        where $foo
      """

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

  def foo = {
    val f = fixture
    implicit val env: StreamExecutionEnvironment = f.env

    val input = eventTimeIntStream(inputPDstreamDataPath)(env)
    val windows: DataStream[List[Int]] =
      discretize(input).foldWith(Nil:List[Int]){(xs, x) => x::xs}
    // TODO create and delete folder if needed
    // TODO organize windows using https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/connectors/filesystem_sink.html
    // so we can reconstruct each window with a parallel read. Use the timestamps
    // TODO support arbitrary types: avro?, kyro? see Flink serialization
    windows.addSink(new BucketingSink[List[Int]]("/tmp/WindowRecorderPoC"))

    windows.timeWindowAll(Time.seconds(1)).reduce(_ ::: _).print()

    env.execute()
    ok
  }
}

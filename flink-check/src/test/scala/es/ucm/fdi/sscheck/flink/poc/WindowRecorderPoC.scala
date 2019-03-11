package es.ucm.fdi.sscheck.flink.poc

import scala.language.implicitConversions
import scala.language.reflectiveCalls

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

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

  def fixture = new {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate event time
  }

  def foo = {
    val f = fixture
    implicit val env: StreamExecutionEnvironment = f.env

    val input = eventTimeIntStream(inputPDstreamDataPath)(env)
    input.print()

    env.execute()
    ok
  }
}

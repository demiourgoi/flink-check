package es.ucm.fdi.sscheck.flink.demo.pollution

import es.ucm.fdi.sscheck.flink.demo.pollution.Pollution.EmergencyLevel._

import scala.collection.JavaConverters._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.matcher.ResultMatchers
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PollutionTest
  extends Specification
    with ResultMatchers  {

  def is =
    s2"""
        - where pollution is computed correctly $pollution1Ok
      """

  def fixture = new {
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
  }

  def pollution1Ok = {
    val f = fixture

    f.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate event time

    // get input data
    val data : DataStream[Pollution.SensorData] = f.env.readTextFile(getClass.getResource("/sensor.data").getPath)
      .map(Pollution.parsePair(_))
      .assignAscendingTimestamps( _.timestamp )

    val counts : DataStream[(Int, EmergencyLevel)] = Pollution.pollution1(data)
    counts.print()

    val expectedOutput = List((2,Notice), (2,Notice), (2,Notice),
      (1,Alert), (1,Alert), (1,Alert),
      (1,Warning), (1,Notice))
    val output = DataStreamUtils.collect(counts.javaStream).asScala.toList
    output should containTheSameElementsAs(expectedOutput)
  }
}
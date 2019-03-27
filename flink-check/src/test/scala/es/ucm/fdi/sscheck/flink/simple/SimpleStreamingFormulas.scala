package es.ucm.fdi.sscheck.flink.simple

import es.ucm.fdi.sscheck.gen.BatchGen
import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.prop.tl.flink.{DataStreamTLProperty, Parallelism}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalacheck.Arbitrary.arbitrary
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}

@RunWith(classOf[JUnitRunner])
class SimpleStreamingFormulas
  extends Specification with ScalaCheck with DataStreamTLProperty{

  // Sscheck configuration
  override val letterSize = Time.milliseconds(50)
  override val defaultParallelism = Parallelism(4)

  def is =
    sequential ^ s2"""
    Simple demo Specs2 example for ScalaCheck properties with temporal
    formulas on Flink streaming programs
      - Given a stream of integers
        When we filter out negative numbers
        Then we get only numbers greater or equal to zero $filterOutNegativeGetGeqZero
      - where time increments for each batch $timeIncreasesMonotonically
      """
  
  def filterOutNegativeGetGeqZero = {
    type U = DataStreamTLProperty.Letter[Int, Int]
    val numBatches = 10
    val gen = BatchGen.always(BatchGen.ofNtoM(10, 50, arbitrary[Int]),
      numBatches)
    val formula = always(nowTime[U]{ (letter, time) =>
      val (_input, output) = letter
      output should foreachElement {_ >= 0}
    }) during numBatches

    forAllDataStream[Int, Int](
      gen)(
      _.filter{ x => !(x < 0)})(
      formula)
  }.set(minTestsOk = 5).verbose
  //.set(minTestsOk = 50).verbose

  def timeIncreasesMonotonically = {
    type U = DataStreamTLProperty.Letter[Int, Int]
    val numBatches = 10
    val gen = BatchGen.always(BatchGen.ofNtoM(10, 50, arbitrary[Int]))

    val formula = always(nextTime[U]{ (letter, time) =>
      nowTime[U]{ (nextLetter, nextTime) =>
        time.millis <= nextTime.millis
      }
    }) during numBatches-1

    forAllDataStream[Int, Int](
      gen)(
      identity[DataStream[Int]])(
      formula)
  }.set(minTestsOk = 5).verbose//.set(minTestsOk = 10).verbose
}

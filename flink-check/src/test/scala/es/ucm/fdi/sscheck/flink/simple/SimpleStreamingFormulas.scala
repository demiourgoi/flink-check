package es.ucm.fdi.sscheck.flink

import scala.language.reflectiveCalls

import es.ucm.fdi.sscheck.gen.WindowGen
import es.ucm.fdi.sscheck.gen.flink.FlinkGenerators._
import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.prop.tl.flink.FlinkFormula._
import es.ucm.fdi.sscheck.prop.tl.flink.{DataStreamTLProperty, Parallelism}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalacheck.Arbitrary.arbitrary
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}

package object simple {
  case class EventTimeType(value: Int, timestamp: Long)
}

package simple {
  @RunWith(classOf[JUnitRunner])
  class SimpleStreamingFormulas
    extends Specification with ScalaCheck with DataStreamTLProperty with Serializable {

    // Sscheck configuration
    override val defaultParallelism = Parallelism(4)
    override val showNSampleElementsOnEvaluation = 7

    def is =
      sequential ^ s2"""
    Simple demo Specs2 example for ScalaCheck properties with temporal
    formulas on Flink streaming programs
      - Given a stream of integers
        When we filter out negative numbers
        Then we get only numbers greater or equal to zero $filterOutNegativeGetGeqZero
      - where time increments for each batch $timeIncreasesMonotonically
      - where sliding windows for evaluation slide as they should $simpleSlidingWindowEvaluation
      - where we can assign a field for event time according to the generated time $eventTimeAssignedOk
      """

    val genWindowSizeMillis = 50

    def filterOutNegativeGetGeqZero = {
      type U = DataStreamTLProperty.Letter[Int, Int]
      val numBatches = 10
      val gen = tumblingTimeWindows(Time.milliseconds(genWindowSizeMillis)){
        WindowGen.always(WindowGen.ofNtoM(10, 50, arbitrary[Int]), numBatches)
      }
      val formula = always(nowTime[U]{ (letter, time) =>
        val (input, output) = letter
        (output should foreachElement {_.value >= 0}) and
          (output should beSubDataSetOf(input))
      }) during numBatches/2 groupBy TumblingTimeWindows(Time.milliseconds(genWindowSizeMillis*2))

      forAllDataStream[Int, Int](
        gen)(
        _.filter{ x => !(x < 0)})(
        formula)
    }.set(minTestsOk=5, workers=2).verbose

    def timeIncreasesMonotonically = {
      type U = DataStreamTLProperty.Letter[Int, Int]
      val numBatches = 10
      val gen = tumblingTimeWindows(Time.milliseconds(genWindowSizeMillis)){
        WindowGen.always(WindowGen.ofNtoM(10, 50, arbitrary[Int]))
      }

      val formula = always(nextTime[U]{ (letter, time) =>
        nowTime[U]{ (nextLetter, nextTime) =>
          time.millis <= nextTime.millis
        }
      }) during (numBatches-1)/2 groupBy TumblingTimeWindows(Time.milliseconds(genWindowSizeMillis*2))

      forAllDataStream[Int, Int](
        gen)(
        identity[DataStream[Int]])(
        formula)
    }.set(minTestsOk=5, workers=2).verbose

    def simpleSlidingWindowEvaluation = {
      type U = DataStreamTLProperty.Letter[Int, Int]
      val numBatches = 10
      val gen = tumblingTimeWindows(Time.milliseconds(genWindowSizeMillis)){
        WindowGen.always(WindowGen.ofNtoM(10, 50, arbitrary[Int]), numBatches)
      }
      val formula = always(nextTime[U]{ (letter, time) =>
        nowTime[U]{ (nextLetter, nextTime) =>
          nextTime.millis - time.millis === genWindowSizeMillis
        }
      }) during (numBatches-1)/2 groupBy
        SlidingTimeWindows(size=Time.milliseconds(genWindowSizeMillis*2), slide=Time.milliseconds(genWindowSizeMillis))

      forAllDataStream[Int, Int](
        gen)(
        identity[DataStream[Int]])(
        formula)
    }.set(minTestsOk=5, workers=2).verbose

    def eventTimeAssignedOk = {
      type U = DataStreamTLProperty.Letter[EventTimeType, EventTimeType]
      val numBatches = 4
      val gen = eventTimeToFieldAssigner[EventTimeType](ts => x => x.copy(timestamp=ts)){
        tumblingTimeWindows(Time.milliseconds(genWindowSizeMillis)){
          WindowGen.always(WindowGen.ofNtoM(10, 50, arbitrary[Int].map{EventTimeType(_, 0)}), numBatches)
        }
      }
      val formula = always(now[U]{ letter =>
        letter._2 should foreachElement { x =>
          x.timestamp === x.value.timestamp
        }
      }) during numBatches/2 groupBy TumblingTimeWindows(Time.milliseconds(genWindowSizeMillis*2))

      forAllDataStream[EventTimeType, EventTimeType](
        gen)(
        identity[DataStream[EventTimeType]])(
        formula)
    }.set(minTestsOk=5, workers=2).verbose
  }
}

package es.ucm.fdi.sscheck.flink.collaborative

import es.ucm.fdi.sscheck.gen.WindowGen
import es.ucm.fdi.sscheck.gen.flink.FlinkGenerators._
import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import es.ucm.fdi.sscheck.prop.tl.flink.TimedElement
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.prop.tl.flink.FlinkFormula._
import es.ucm.fdi.sscheck.prop.tl.flink.{DataStreamTLProperty, Parallelism}
import es.ucm.fdi.sscheck.flink.demo.collaborative.Harass.{harass_max, HarassmentIncident, DangerLevel}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}


@RunWith(classOf[JUnitRunner])
class Test_harass_max_fail
  extends Specification with ScalaCheck with DataStreamTLProperty{

  // Sscheck configuration
  override val defaultParallelism = Parallelism(4)

  // val letterSize = Time.milliseconds(50)
  val letterSize = Time.seconds(1)
  
  val nTests   = 1
  val nWindows = 1
  val wSize    = 5
  val nWorkers = 1

  def is = sequential ^ s2"""$highDangerNotSafe"""

  // Generator of HarassmentIncident with a zone_id between 0 and num_zones-1, and a 
  // perceived danger between min_danger and max_danger
  def harassmentDataGen( num_zones : Int, min_danger : Double, max_danger : Double) = for {
    zone_id <- Gen.chooseNum[Int](0, num_zones-1)
    danger  <- Gen.chooseNum[Double](min_danger, max_danger)
  } yield HarassmentIncident(zone_id, danger)
  
  // If all generated HarassmentIncidents have values greater than 1 then the 
  // computed danger levels must be different from DangerLevel.Safe
  def highDangerNotSafe = {
    type U = DataStreamTLProperty.Letter[HarassmentIncident, (Int, DangerLevel.DangerLevel)]

    // Generator of 'nWindows' windows (each one of 'letterSize' time) containing 
    // 'wSize' harassment incidents from 10 different zones with perceived dangers
    // in the range [1.1-10.0]
    val gen = tumblingTimeWindows(letterSize){
      WindowGen.always(WindowGen.ofN(wSize, harassmentDataGen(10,0.0,2.0)),
        nWindows)
    }

    // Property to test: in every processed window the danger level of every zone
    // is different from 'Safe'
    val property = always(now[U]{ case (input, output) =>
      output should foreachElement (_.value._2 != DangerLevel.Safe)
    }) during nWindows groupBy TumblingTimeWindows(letterSize)

    forAllDataStream[HarassmentIncident, (Int, DangerLevel.DangerLevel)](
      gen)(
      harass_max)(
      property)
  }.set(minTestsOk = nTests, workers=nWorkers)//.verbose

   
}

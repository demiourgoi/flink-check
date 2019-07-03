package es.ucm.fdi.sscheck.flink.collaborative

import es.ucm.fdi.sscheck.gen.WindowGen
import es.ucm.fdi.sscheck.gen.flink.FlinkGenerators._
import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import es.ucm.fdi.sscheck.prop.tl.flink.TimedElement
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.prop.tl.flink.FlinkFormula._
import es.ucm.fdi.sscheck.prop.tl.flink.{DataStreamTLProperty, Parallelism}
import es.ucm.fdi.sscheck.flink.demo.collaborative.Harass.{harass_max, Incident, DangerLevel}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}


@RunWith(classOf[JUnitRunner])
class Test_harass_max_ok
  extends Specification with ScalaCheck with DataStreamTLProperty{

  // Sscheck configuration
  override val defaultParallelism = Parallelism(4)

  // val letterSize = Time.milliseconds(50)
  val letterSize = Time.hours(1)
  
  val nTests    = 5
  val nWindows  = 1
  val min_wSize = 2
  val max_wSize = 7
  val nWorkers  = 1
  val nZones    = 10

  def is = s2"""$highDangerNotSafe"""

  // Generator of a harassment Incident with a zone_id between 0 and num_zones-1, and a 
  // perceived danger between min_danger and max_danger
  def incidentGen( num_zones : Int, min_danger : Double, max_danger : Double) = for {
    zone_id <- Gen.chooseNum[Int](0, num_zones-1)
    danger  <- Gen.chooseNum[Double](min_danger, max_danger)
  } yield Incident(zone_id, danger)
  
  // If all harassment Incidents have values greater than 1 then the 
  // computed danger levels must be different from DangerLevel.Safe for
  // every zone
  def highDangerNotSafe = {
    type U = DataStreamTLProperty.Letter[Incident, (Int, DangerLevel.DangerLevel)]

    // Generator of 'nWindows' windows (each one of 'letterSize' time) containing 
    // 'wSize' harassment incidents from 'nZones' different zones with perceived 
    // danger in the range [1.1-10.0]
    val gen = tumblingTimeWindows(letterSize){
      WindowGen.always(WindowGen.ofNtoM(min_wSize, max_wSize, incidentGen(nZones,1.1,10.0)),
        nWindows)
    }

    // Property to test: in every processed window the danger level of every zone
    // is different from 'Safe'
    val property = always(now[U]{ case (input, output) =>
      output should foreachElement (_.value._2 != DangerLevel.Safe)
    }) during nWindows groupBy TumblingTimeWindows(letterSize)

    forAllDataStream[Incident, (Int, DangerLevel.DangerLevel)](
      gen)(
      harass_max)(
      property)
  }.set(minTestsOk = nTests, workers=nWorkers)//.verbose

   
}

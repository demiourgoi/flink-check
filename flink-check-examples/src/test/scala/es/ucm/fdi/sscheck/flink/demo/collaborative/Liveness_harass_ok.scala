package es.ucm.fdi.sscheck.flink.collaborative

import es.ucm.fdi.sscheck.gen.WindowGen
import es.ucm.fdi.sscheck.gen.flink.FlinkGenerators._
import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import es.ucm.fdi.sscheck.prop.tl.flink.TimedElement
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.prop.tl.flink.FlinkFormula._
import es.ucm.fdi.sscheck.prop.tl.flink.{DataStreamTLProperty, Parallelism}
import es.ucm.fdi.sscheck.prop.tl.{Solved}
import es.ucm.fdi.sscheck.flink.demo.collaborative.Harass.{harass_max, Incident, DangerLevel}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}


@RunWith(classOf[JUnitRunner])
class Liveness_harass_ok
  extends Specification with ScalaCheck with DataStreamTLProperty{

  // Sscheck configuration
  override val defaultParallelism = Parallelism(4)

  val nTests    = 1
  val nWindows  = 8
  val min_wSize = 1
  val max_wSize = 5
  val nZones    = 10
  val nWorkers  = 1


  def is = s2"""$highestDangerEventuallyExtreme"""

  // Generator of a harassment Incident with a zone_id between 0 and num_zones-1, and a 
  // perceived danger between min_danger and max_danger
  def incidentGen( num_zones : Int, min_danger : Double, max_danger : Double) = for {
    zone_id <- Gen.chooseNum[Int](0, num_zones-1)
    danger  <- Gen.chooseNum[Double](min_danger, max_danger)
  } yield Incident(zone_id, danger)
  
  // If there is an incident with a value greater than 8 then eventually there
  // will be a Extreme danger in that zone
  def highestDangerEventuallyExtreme = {
    type U = DataStreamTLProperty.Letter[Incident, (Int, DangerLevel.DangerLevel)]

    // Generator of 'nWindows' windows (each one of 'windowSize' time) containing 
    // 'wSize' harassment incidents from 'nZones' different zones with perceived 
    // danger in the complete range [0-10.0]
    val gen = tumblingTimeWindows(Time.minutes(30)){
      WindowGen.always(WindowGen.ofNtoM(min_wSize, max_wSize, incidentGen(nZones,0,10)),
        4)
    }

    // Property to test: in every processed window the danger level of every zone
    // is different from 'Safe'
    val property = alwaysF[U] { case (input, output) =>
        val highestDanger = Solved[U] {input.filter(_.value.danger > 8).count() > 0}
        val nowExtreme = Solved[U] {output.filter(_.value._2 == DangerLevel.Extreme).count() > 0}
        val eventuallyExtreme = laterR[U] { case (_,fut_output) =>
          fut_output should existsElement (_.value._2 == DangerLevel.Extreme)
        } during 3
        
        highestDanger ==> (nowExtreme or eventuallyExtreme)
    } during 5 groupBy TumblingTimeWindows(Time.minutes(15))

    forAllDataStream[Incident, (Int, DangerLevel.DangerLevel)](
      gen)(
      harass_max)(
      property)
  }.set(minTestsOk = nTests, workers=nWorkers)//.verbose

   
}

package es.ucm.fdi.sscheck.flink.collaborative

import es.ucm.fdi.sscheck.flink.demo.collaborative.Harass.{DangerLevel, Incident, harass_max}
import es.ucm.fdi.sscheck.gen.WindowGen
import es.ucm.fdi.sscheck.gen.flink.FlinkGenerators._
import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.prop.tl.Solved
import es.ucm.fdi.sscheck.prop.tl.flink.DataStreamTLProperty._
import es.ucm.fdi.sscheck.prop.tl.flink.FlinkFormula._
import es.ucm.fdi.sscheck.prop.tl.flink.{DataStreamTLProperty, Parallelism}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.specs2.execute.AsResult
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}

@RunWith(classOf[JUnitRunner])
class Liveness_harass_ok
  extends Specification with ScalaCheck with DataStreamTLProperty{

  // Sscheck configuration
  override val defaultParallelism = Parallelism(4)

  val nZones    = 10
  val nWorkers  = 5
  val nTests    = 10
  
  val nWindows  = 4 // of 15 minutes
  val winSize   = 50

  def is = s2"""Example liveness property ${highestDangerEventuallyExtreme}"""

  // Generator of a harassment Incident with a zone_id between 0 and num_zones-1, and a 
  // perceived danger between min_danger and max_danger
  def incidentGen(num_zones : Int, min_danger : Double, max_danger : Double)=
    for {
      zone_id <- Gen.chooseNum[Int](0, num_zones-1)
      danger  <- Gen.chooseNum[Double](min_danger, max_danger)
    } yield Incident(zone_id, danger)
  
  // If there is an incident with a value greater than 8 then eventually there
  // will be a Extreme danger in that zone
  def highestDangerEventuallyExtreme = {
    type U = Letter[Incident, (Int, DangerLevel.DangerLevel)]
    
    // Generator of 4 windows of 'genWSize' time containing 'winSize'
    // harassment incidents from 'nZones' different zones with perceived 
    // danger in the complete range [0-10.0]
    val gen = tumblingTimeWindows(Time.minutes(30)){
      WindowGen.always(WindowGen.ofN(winSize, incidentGen(nZones,0.0,10.0)),
        nWindows/2)
    }

    // Property to test: in every processed window the danger level of every zone
    // is different from 'Safe'
    val property = alwaysF[U] { case (input, output) =>
        val extremeZones = input.filter(_.value.danger > 8).map(_.value.zone_id)
        val anyExtremeZones = Solved[U]{ extremeZones should beEmptyDataSet() }
        val nowExtremeZones = output.filter(_.value._2 == DangerLevel.Extreme).map(_.value._1)
                
        val nowExtreme = Solved[U]{ extremeZones should beSubDataSetOf(nowExtremeZones) }
        val eventuallyExtreme = laterR[U] { case (_,fut_output) =>
          val futExtremeZones = fut_output.filter(_.value._2 == DangerLevel.Extreme).map(_.value._1)
          AsResult{ extremeZones should beSubDataSetOf(futExtremeZones) }
        } during 3
        
        anyExtremeZones ==> (nowExtreme or eventuallyExtreme)
    } during (nWindows-3) groupBy TumblingTimeWindows(Time.minutes(15))

    forAllDataStream[Incident, (Int, DangerLevel.DangerLevel)](
      gen)(
      harass_max)(
      property)
  }.set(minTestsOk = nTests, workers=nWorkers)//.verbose
}

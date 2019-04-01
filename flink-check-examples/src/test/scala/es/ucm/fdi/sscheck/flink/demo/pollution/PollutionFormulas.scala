package es.ucm.fdi.sscheck.flink.pollution

import es.ucm.fdi.sscheck.gen.WindowGen
import es.ucm.fdi.sscheck.gen.flink.FlinkGenerators._
import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import es.ucm.fdi.sscheck.prop.tl.flink.TimedElement
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.prop.tl.flink.FlinkFormula._
import es.ucm.fdi.sscheck.prop.tl.flink.{DataStreamTLProperty, Parallelism}
import es.ucm.fdi.sscheck.flink.demo.pollution.Pollution.{pollution1, SensorData, EmergencyLevel}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}


@RunWith(classOf[JUnitRunner])
class PollutionFormulas
  extends Specification with ScalaCheck with DataStreamTLProperty{

  // Sscheck configuration
  override val defaultParallelism = Parallelism(4)

  // val letterSize = Time.milliseconds(50)
  val letterSize = Time.seconds(1)

  def is =
    sequential ^ s2"""
    ScalaCheck properties with temporal formulas on Flink pollution streaming programs
    - pollution1: if all sensors have values greater than 180, then all the generated 
         emergency levels are different from OK $highValuesGetNotOK
    - pollution1: sensors with higher concentration value in a window must be 
         eventually tagged with EmergencyLevel.Alert $highEventuallyAlert
      """      
  // Generator of SensorData with an id between 0 and num_sensors-1, and a 
  // concentration value between min_conc and max_conc
  def sensorDataGen( num_sensors : Int, min_conc : Double, max_conc : Double) = for {
    timestamp     <- Gen.chooseNum[Long](0, 0) // timestamp is useless, force 0
    sensor_id     <- Gen.chooseNum[Int](0, num_sensors-1)
    concentration <- Gen.chooseNum[Double](min_conc, max_conc)
  } yield SensorData(timestamp, sensor_id, concentration)
  
  // If all generated SensorData have values greater that 180, then the generated
  // emergency levels are different from EmergencyLevel.OK
  def highValuesGetNotOK = {
    type U = DataStreamTLProperty.Letter[SensorData, (Int, EmergencyLevel.EmergencyLevel)]
    val numWindows = 5
    // Generates windows of 10-50 measurements from 10 sensors with 
    // concentrations in the range [180.1-1000.0]
    val gen = tumblingTimeWindows(letterSize){
      WindowGen.always(WindowGen.ofNtoM(10, 50, sensorDataGen(10,180.1,1000.0)),
        numWindows)
    }

    // In all processed windows the emergency level is different from OK                              
    val formula = always(now[U]{ letter =>
      val (_input, output) = letter
      output should foreachElement (_.value._2 != EmergencyLevel.OK)
    }) during numWindows groupBy TumblingTimeWindows(letterSize)

    forAllDataStream[SensorData, (Int, EmergencyLevel.EmergencyLevel)](
      gen)(
      pollution1)(
      formula)
  }.set(minTestsOk = 1).verbose

   
  // Those sensors with higher concentration value in a window must be 
  // eventually tagged with EmergencyLevel.Alert
  def highEventuallyAlert = {
    type U = DataStreamTLProperty.Letter[SensorData, (Int, EmergencyLevel.EmergencyLevel)]
    val numWindows = 2
    // Generates windows of 10-50 measurements from 10 sensors with 
    // concentrations in the range [0.0-1000.0]
    val gen = tumblingTimeWindows(letterSize){
      WindowGen.always(WindowGen.ofNtoM(2, 5, sensorDataGen(3,500.0,1000.0)),
        numWindows)
    }

    val formula = alwaysF[U]( { letter => 
      val (input, _) = letter
      val highSensors : Seq[Int] = input.filter( _.value.concentration > 400.0)
                                        .map( _.value.sensor_id)
                                        .collect
      laterR[U] { letter =>
        val (_, output) = letter
        val alertSensors : DataSet[Int] = 
          output.filter(_.value._2 == EmergencyLevel.Alert)
                .map(_.value._1)
        alertSensors should existsElement(highSensors)( (hs:Seq[Int]) => ((x:Int) => hs.contains(12)) )
        // These matcher should fail because there are not sensor ids greater than 10!!
      } on numWindows // FIXME: arbitrary value
    }) during numWindows groupBy TumblingTimeWindows(letterSize)

    forAllDataStream[SensorData, (Int, EmergencyLevel.EmergencyLevel)](
      gen)(
      pollution1)(
      formula)
  }.set(minTestsOk = 1).verbose

}

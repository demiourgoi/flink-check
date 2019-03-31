package es.ucm.fdi.sscheck.flink.pollution

import es.ucm.fdi.sscheck.gen.WindowGen
import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import es.ucm.fdi.sscheck.prop.tl.flink.TimedElement
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.prop.tl.flink.{DataStreamTLProperty, Parallelism}
import es.ucm.fdi.sscheck.flink.demo.pollution.Pollution.{pollution1, SensorData, EmergencyLevel}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}


@RunWith(classOf[JUnitRunner])
class PollutionFormulas
  extends Specification with ScalaCheck with DataStreamTLProperty{

  // Sscheck configuration
  //override val letterSize = Time.milliseconds(50)
  override val letterSize = Time.seconds(1)
  override val defaultParallelism = Parallelism(4)

  def is =
    sequential ^ s2"""
    ScalaCheck properties with temporal formulas on Flink pollution streaming programs
      - pollution1:
          - if all sensors have values greater than 180, then all the generated 
            emergency levels are different from OK $highValuesGetNotOK
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
    val gen = WindowGen.always(WindowGen.ofNtoM(10, 50, sensorDataGen(10,180.1,1000.0)),
                              numWindows)
    // In all processed windows the emergency level is different from OK                              
    val formula = always(now[U]{ letter =>
      val (_input, output) = letter
      //output should foreachElement (_ => false) // Este test deberia fallar!!!!
      output should foreachElement (_.value._2 != EmergencyLevel.OK)
    }) during numWindows

    forAllDataStream[SensorData, (Int, EmergencyLevel.EmergencyLevel)](
      gen)(
      pollution1)(
      formula)
  }.set(minTestsOk = 1).verbose

 
  
  // Those sensors with high concentration values must be eventually be tagged
  // with EmergencyLevel.Alert
  def highEventuallyAlert = {
    type U = DataStreamTLProperty.Letter[SensorData, (Int, EmergencyLevel.EmergencyLevel)]
    val numWindows = 5
    // Generates windows of 10-50 measurements from 10 sensors with 
    // concentrations in the range [0.0-1000.0]
    val gen = WindowGen.always(WindowGen.ofNtoM(2, 5, sensorDataGen(3,0.0,1000.0)),
                              numWindows)

    val formula = alwaysF[U] ({ case (input, _) => 
      type TES = TimedElement[SensorData]
      type TEP = TimedElement[(Int, EmergencyLevel.EmergencyLevel)]
      type TEI = TimedElement[Int]
      val highSensors = input.filter( (x:TES) => x.value.concentration > 400.0)
                             .map( (x:TES) => x.value.sensor_id).collect
      laterR[U] { case (_, output) =>
        val alertSensors = output.filter( (x:TEP) => x.value._2 == EmergencyLevel.OK)
//                                 .map( (x:TEP) => x.value._1)
        alertSensors should foreachElement( (x:TEP) => highSensors.contains(x.value._1))
      } on numWindows // FIXME: arbitrary value
    }) during numWindows

    forAllDataStream[SensorData, (Int, EmergencyLevel.EmergencyLevel)](
      gen)(
      pollution1)(
      formula)
  }.set(minTestsOk = 1).verbose

}

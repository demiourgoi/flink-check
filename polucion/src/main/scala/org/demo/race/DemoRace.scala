package org.demo.race

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.gen.WinGen
import org.specs2.matcher.ResultMatchers
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}
import org.test.Formula._
import org.test.{Formula, Test}
import org.test.ForEachWindow




class DemoRace extends Specification
  with ResultMatchers
  with ScalaCheck
  with Serializable {

  // Spark configuration
  /*override def sparkMaster : String = "local[*]"
  val batchInterval = Duration(500)
  override def batchDuration = batchInterval
  override def defaultParallelism = 4
  override def enableCheckpointing = true*/

  def is =
    sequential ^ s2"""
      - where all the sensor Id must be under the number of sensors $borrar
      """


  //Incluir posibilidad de que haya listas de varios tamaños (para que cada ventana pueda tener un numero distinto de datos)
  def borrar = {
    /*type U = (Int, Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 10
    val numSensor = PollutionGen.genSensorId(3).sample.get
    val data = WinGen.ofN(1, PollutionGen.genDataPol(0,100,numSensor))
    val gen = WinGen.always(data, numWindows)
    val formula : Formula[List[U]] = always { (u : List[U]) =>
      sensorIdLessThan(u, numSensor+1)
    } during numWindows

    println("Running sensorIdsOk")
    val result = Test.test[U](gen, formula, env, 100)
    result.print
    env.execute()
    result.toString*/
    null

  }









}

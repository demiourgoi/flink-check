package org.demo.pollution

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.gen.WinGen
import org.scalacheck.Prop
import org.specs2.matcher.ResultMatchers
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}
import org.test.Formula._
import org.test.{Formula, Test}
import org.demo.pollution.PollutionGen._




class DemoPollution extends Specification
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
      - where all the sensor Id must be under the number of sensors $sensorIdsOk
      - where the polution data is always low $neverPol
      - where the polution data is always high $alwaysPol
      - where we eventually get pollution $eventuallyPol
      - where the alarm must activate $checkAlarm
      - where pollution data is generated randomly $checkGenPol
      """


  //Incluir posibilidad de que haya listas de varios tamaños (para que cada ventana pueda tener un numero distinto de datos)
  def sensorIdsOk = {
    type U = (Int, Int)
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
    result.toString

  }



  def neverPol = {
    type U = (Int, Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 20
    val numSensor = 5
    val maxPol = 20
    val data = WinGen.ofN(1, PollutionGen.genNoPol(maxPol,numSensor))
    val gen = WinGen.always(data, numWindows)
    val formula : Formula[List[U]] = always { (u : List[U]) =>
      checkNoPol(u, maxPol)
    } during numWindows

    println("Running neverPol")
    val result = Test.test[U](gen, formula, env, 100)
    result.print
    env.execute()
    result.toString

  }



  def alwaysPol = {
    type U = (Int, Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 20
    val numSensor = 5
    val maxPol = 20
    val data = WinGen.ofN(1, PollutionGen.genPol(maxPol,numSensor))
    val gen = WinGen.always(data, numWindows)
    val formula : Formula[List[U]] = always { (u : List[U]) =>
      checkPol(u, maxPol)
    } during numWindows

    println("Running alwaysPol")
    val result = Test.test[U](gen, formula, env, 100)
    result.print
    env.execute()
    result.toString

  }



  def eventuallyPol = {
    type U = (Int, Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 20
    val numSensor = 5
    val maxPol = 20
    val data = WinGen.ofN(1, PollutionGen.genDataPol(0, 100,numSensor))
    val gen = WinGen.always(data, numWindows)
    val formula : Formula[List[U]] = later { (u : List[U]) =>
      checkPol(u, maxPol)
    } during numWindows

    println("Running eventuallyPol")
    val result = Test.test[U](gen, formula, env, 100)
    result.print
    env.execute()
    result.toString
  }

  //Hacerla, o bien para que al tener todos los sensores polucion, se compruebe que se activan todas las alarmas,
  //o enfocado a que compruebe uno de los sensores y su alarma
  def checkAlarm = {
    type U = (Int, Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 20
    val numSensor = 5
    val maxPol = 20
    val data = WinGen.ofN(5, PollutionGen.genPol(maxPol,numSensor))
    val gen = WinGen.always(data, numWindows)
    val alwaysPol: Formula[List[U]] = always { (u : List[U]) =>
      checkPol(u, maxPol)
    } during numWindows

    val eventuallyAlarm: Formula[List[U]] = later { (u : List[U]) =>
      PollutionGen.alarmController(u,numSensor,maxPol,3)
    } during numWindows

    val formula = alwaysPol ==> eventuallyAlarm

    println("Running checkAlarm")
    val result = Test.test[U](gen, formula, env, 100)
    result.print
    env.execute()
    result.toString
  }



  def checkGenPol = {
    type U = (Int, Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 10
    val numSensor = 4
    val maxPol = 20
    //Ventanas de 1 solo dato porque nos interesa mirar los datos de polucion de forma individual.
    val data = WinGen.ofN(1, PollutionGen.genDataPol(1,100,numSensor))
    val gen = WinGen.always(data, numWindows)
    val pollution: Formula[List[U]] = (u : List[U]) => checkPol(u, maxPol)
    val noPollution: Formula[List[U]] = (u : List[U]) => checkNoPol(u, maxPol)
    val noPolUntilPol: Formula[List[U]] = (u : List[U]) => noPollution until pollution on numWindows
    val polUntilNoPol: Formula[List[U]] = (u : List[U]) => pollution until noPollution on numWindows
    val untilFormula: Formula[List[U]] = noPolUntilPol or polUntilNoPol
    println("CheckGenPol")
    val result = Test.test[U](gen, untilFormula, env, 100)
    result.print
    env.execute()
    result.toString
  }



  /**Incluyendo alarma*/
  //always(noPol) -> always(alarma=false)
  //always(pol) -> always(alarme=true)
  //noPol and eventuallyPol -> eventually(alarma=true)




}

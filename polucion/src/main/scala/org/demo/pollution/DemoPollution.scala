package org.demo.pollution

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.gen.WinGen
import org.scalacheck.{Gen, Prop}
import org.specs2.matcher.ResultMatchers
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}
import org.test.Formula._
import org.test.{Formula, Test}
import org.demo.pollution.PollutionGen._
import org.apache.flink.api.common.JobExecutionResult
import java.util.concurrent.TimeUnit

import org.test.Test.test


class DemoPollution extends Specification
  with ResultMatchers
  with ScalaCheck
  with Serializable {


  def is =
    sequential ^ s2"""
      - where all the sensor Id must be under the number of sensors $sensorIdsOk
      - where the pollution data is always low $neverPol
      - where the pollution data is always high $alwaysPol
      - where we eventually get pollution (most times) $eventuallyPol
      - where the alarm must activate $checkAlarm
      - where pollution data is generated randomly $checkGenPol
      - where the alarm is on when we get pollution and off when we don't $alarmOn
      - example to test release  $noPolReleasePol
      """



  def sensorIdsOk = {
    val t1 = System.nanoTime


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

    /*val benchmark = env.execute("My Flink Job")
    System.out.println("The job took " + benchmark.getNetRuntime(TimeUnit.SECONDS) + " seconds to execute")*/

    env.execute()

    println("Duration: " +  (System.nanoTime() - t1) / 1e9d)


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


  //Esta prueba puede llegar a dar false si todos los datos generados son menores que 20, o todos
  //son mayores o iguales que 20. No sería un error, ya que el generador genera datos entre 1 y 100,
  //pero ocurre pocas veces.
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



  def checkAlarm = {
    type U = (Int, Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 20
    val numSensor = 1
    val maxPol = 20
    val data = WinGen.ofN(5, PollutionGen.genPol(maxPol,numSensor))
    val gen = WinGen.always(data, numWindows)
    //val poll: Formula[List[U]] = {(u : List[U]) => u.filter( x => x._1 >= maxPol).size == u.size}
    val alwaysPol: Formula[List[U]] = always { (u : List[U]) =>
      checkPol(u, maxPol)
      //poll
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


  def alarmOn = {
    type U = (Int, Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 20
    val numSensor = 1
    val maxPol = 20
    val dataPol = WinGen.ofN(5, PollutionGen.genPol(maxPol,numSensor))
    val dataNoPol = WinGen.ofN(5, PollutionGen.genNoPol(maxPol,numSensor))
    val gen = WinGen.until(dataNoPol, dataPol, numWindows)
    //val poll: Formula[List[U]] = {(u : List[U]) => u.filter( x => x._1 >= maxPol).size == u.size}

    val noPol : Formula[List[U]] = (u : List[U]) => checkNoPol(u, maxPol)
    val pol : Formula[List[U]] = (u : List[U]) => checkPol(u, maxPol)
    val noPolUntilPol : Formula[List[U]] = (u : List[U]) => noPol until pol on numWindows
    val alarm_off : Formula[List[U]] = (u : List[U]) => !alarmController(u,numSensor,maxPol,3)
    val alarm_on : Formula[List[U]] = (u : List[U]) => alarmController(u,numSensor,maxPol,3)
    val noAlarmUntilAlarm : Formula[List[U]] = (u : List[U]) => alarm_off until alarm_on on numWindows
    val formula = noPolUntilPol ==> noAlarmUntilAlarm


    println("Running alarmOn")
    val result = Test.test[U](gen, formula, env, 100)
    result.print
    env.execute()
    result.toString
  }


  def noPolReleasePol = {
    type U = (Int,Int)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 20
    val numSensor = 1
    val maxPol = 20
    val dataPol = WinGen.ofN(1, PollutionGen.genPol(maxPol,numSensor))
    val dataNoPol = WinGen.ofN(1, PollutionGen.genNoPol(maxPol,numSensor))
    val gen = WinGen.release(dataPol, dataNoPol, numWindows)
    val noPol : Formula[List[U]] = (u : List[U]) => checkAnyNoPol(u,maxPol)
    val pol : Formula[List[U]] = (u : List[U]) => checkAnyPol(u,maxPol)
    val formula : Formula[List[U]] = (u : List[U]) =>pol release noPol on numWindows
    val result = Test.test[U](gen, formula, env, 50)
    println("Running noPolReleasePol")
    result.print
    env.execute()
    result.toString
  }

  }

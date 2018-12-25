package org.demo.race

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.gen.WinGen
import org.specs2.matcher.ResultMatchers
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}
import org.test.Formula._
import org.test.{Formula, Not, Test}
import org.demo.race.RaceGen._
import org.scalacheck.Prop.{False, True}




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
      - where the number of runners in the race must always be below the initial number of runners $racesOk
      - where the number of runners in the race must always be below the initial number of runners $checkSpeeds
      - where the number of runners in the race must always be below the initial number of runners $checkBannedRunner
      """


//Puede resultar en 'undecided' si la carrera se termina en menos de 'numWindows' estados
  def racesOk = {
    type U = (String,Int,String)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 5
    val runners = List("Kirby", "Molang", "Piupiu", "Pusheen", "Gudetama")
    val gen = genRace(runners, 50)
    val formula : Formula[List[U]] = always { (u : List[U]) =>
      u.size <= runners.size
    } during numWindows

    println("Running racesOk")
    val result = Test.test[U](gen, formula, env, 50)
    result.print
    env.execute()
    result.toString

  }


  //Devuelve false (failed) en los casos en que nadie se dopa
  //En este caso, si sale undecided es porque la carrera ha acabado antes de 'numWindows' instantes
  //sin que nadie haya sido descalificado, por lo que sabemos que nadie se ha dopado aunque tengamos
  //undecided
  def checkSpeeds= {
    type U = (String,Int, String)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 50
    val runners = List("Kirby", "Molang", "Piupiu", "Pusheen", "Gudetama")
    val gen = genRace(runners, 50)
    val formula : Formula[List[U]] = later { (u : List[U]) =>
      var banned = false
      for(runner <- u) {
        banned = banned || checkSpeed(runner)
      }
      banned
    } during numWindows

    println("Running checkSpeeds")
    val result = Test.test[U](gen, formula, env, 50)
    result.print
    env.execute()
    result.toString
  }

  def checkBannedRunner= {
    type U = (String,Int, String)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 60
    val runners = List("Kirby", "Molang", "Piupiu", "Pusheen", "Gudetama")
    val gen = genRace(runners, 50)
    val runnerSpeedWrong: Formula[List[U]] = (u : List[U]) => checkSpeed(u.filter(r => r._1 == "Piupiu").head)
    val runnerBanned: Formula[List[U]] = (u : List[U]) => isBanned(u.filter(r => r._1 == "Piupiu").head)
    val runnerNotBanned: Formula[List[U]] = Not(runnerBanned)
    val NotBannedUntilBanned: Formula[List[U]] = runnerNotBanned until runnerBanned on numWindows
    val eventuallyDeleted: Formula[List[U]] = later { (u : List[U]) =>
      u.filter(r => r._1 == "Piupiu").isEmpty
    } during numWindows
    val formula : Formula[List[U]] = (runnerSpeedWrong ==> (NotBannedUntilBanned and eventuallyDeleted))
    println("Running checkBannedRunner")
    val result = Test.test[U](gen, formula, env, 50)
    result.print
    env.execute()
    result.toString
  }


//Check speed of one runner

//descalificar corredores






}

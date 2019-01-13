package org.demo.race

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.specs2.matcher.ResultMatchers
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}
import org.test.Formula._
import org.test.{Formula, Not, Test}
import org.demo.race.RaceGen._
import org.scalacheck.Gen



//Clase con tests para probar la funcion test
class DemoRace extends Specification
  with ResultMatchers
  with ScalaCheck
  with Serializable {


  def is =
    sequential ^ s2""" Races demo
      - where the number of runners in the race must always be below the initial number of runners $racesOk
      - checks if a runner eventually exceeds a normal speed $checkSpeeds
      - a runner who exceeds a normal speed must be banned and deleted from the race $checkBannedRunner
      - a banned runner never wins $bannedNeverWin
      - a runner who is never banned doesn't necessarily win $notBannedAlwaysWin
      """


//Puede resultar en 'undecided' si la carrera se termina en menos de 'numWindows' estados
  def racesOk = {
    type U = (String,Int,String, Boolean)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 10
    val runners = List("Kirby", "Molang", "Piupiu", "Pusheen", "Gudetama")
    val gen = genRace(runners, 50,1,10)
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
    type U = (String,Int, String, Boolean)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 50
    val runners = List("Kirby", "Molang", "Piupiu", "Pusheen", "Gudetama")
    val gen = genRace(runners, 50,1,10)
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
    type U = (String, Int, String, Boolean)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 100
    val runners = List("Kirby", "Molang", "Piupiu", "Pusheen", "Gudetama")
    val gen = genRace(runners, 900,9,10)
    val runnerSpeedWrong: Formula[List[U]] = (u: List[U]) => {
      val l = u.filter(r => r._1 == "Piupiu")
      if(l.size > 0) checkSpeed(l.head)
      else false
     }
    val runnerBanned: Formula[List[U]] = (u : List[U]) => {
      val l = u.filter(r => r._1 == "Piupiu")
      if(l.size > 0) isBanned(l.head)
      else false
    }
    val runnerNotBanned: Formula[List[U]] = Not(runnerBanned)
    val NotBannedUntilBanned: Formula[List[U]] = runnerNotBanned until runnerBanned on numWindows
    val eventuallyDeleted: Formula[List[U]] = later { (u : List[U]) =>
      u.filter(r => r._1 == "Piupiu").isEmpty
    } during numWindows
    val formula : Formula[List[U]] = (runnerSpeedWrong ==> (NotBannedUntilBanned and eventuallyDeleted))
    println("Running checkBannedRunner")
    val result = Test.test[U](gen, formula, env, 100)
    result.print
    env.execute()
    result.toString
  }


  def bannedNeverWin = {
    type U = (String,Int, String, Boolean)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 25
    val runners = List("Kirby", "Molang", "Piupiu", "Pusheen", "Gudetama")
    val gen = genRace(runners, 50,10,10)
    val gen2 = nRaces(gen,5)
    val runnerBanned: Formula[List[U]] = (u : List[U]) => {
      val l = u.filter(r => r._1 == "Pusheen")
      if(l.size > 0) isBanned(l.head)
      else false
    }
    val runnerWins: Formula[List[U]] = (u : List[U]) => {
      val l = u.filter(r => r._1 == "Pusheen")
      if(l.size > 0) isWinner(l.head)
      else false
    }
    val neverWins: Formula[List[U]] = always {
      Not(runnerWins)
    } during numWindows
    val formula : Formula[List[U]] = runnerBanned ==> neverWins
    println("Running bannedNeverWin")
    val result = Test.test[U](gen2, formula, env, 50)
    result.print
    env.execute()
    result.toString
  }




  //Debe ser falso
  def notBannedAlwaysWin = {
    type U = (String,Int, String, Boolean)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val numWindows = 30
    val runners = List("Kirby", "Molang", "Piupiu", "Pusheen", "Gudetama")
    val gen = genRace(runners, 50,5,5)
    val gen2 = nRaces(gen,3)
    val runnerBanned: Formula[List[U]] = (u : List[U]) => {
      val l = u.filter(r => r._1 == "Kirby")
      if(l.size > 0) isBanned(l.head)
      else false
    }
    val neverBanned: Formula[List[U]] = always { (u : List[U]) =>
      Not(runnerBanned)
    } during numWindows
    val eventuallyWins: Formula[List[U]] = later {(u : List[U]) =>
      val l = u.filter(r => r._1 == "Kirby")
      if(l.size > 0) isWinner(l.head)
      else false
    } during 10
    val alwaysWins: Formula[List[U]] = always {(u : List[U]) =>
      eventuallyWins
    } during numWindows
    val formula : Formula[List[U]] = (neverBanned ==> alwaysWins)
    println("Running notBannedAlwaysWin")
    val result = Test.test[U](gen2, formula, env, 50)
    result.print
    env.execute()
    result.toString
  }








}

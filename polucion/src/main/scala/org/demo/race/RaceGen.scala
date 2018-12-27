package org.demo.race

import org.gen.{ListStream, WinGen}
import org.scalacheck.{Arbitrary, Gen}
import org.gen.ListStreamConversions._
import org.scalacheck.util.Buildable

import scala.collection.mutable.{ListBuffer, Map}




object RaceGen {

  val maxSpeed = 8

  var previousPos: Map[String, (Int, String)]= Map[String, (Int, String)]()

  var winnerArrived = false
  var winner = List[String]()
  var hola = "Hola"

  //Dopado para velocidad > 8
  //Velocidad entre 1 y 10
  //Incluir posibilidad de que los corredores se enfaden si hay más de N participantes haciendo trampa?
  //Incluir que un corredor tenga mas probabilidad de doparse si pierde mas de N veces?
  //(id, pos, time?)
  //Si (id, pos, t2) - (id,pos,t2) > N -> descalificado/dopado



 //Hacer initrunner para que todos se dopen,  y para que nadie lo haga

  def initRunnerList(id: List[String], min: Int, max: Int): Gen[List[(String, Int, Int, Int, String, Boolean)]] = {
    winnerArrived = false
    previousPos = Map[String, (Int, String)]()
    if (id.isEmpty) for {
    tl <- Gen.const(Nil)
    } yield tl

    else {
      for {
        speedMin <- Gen.choose(min, max)
        speedMax <- Gen.choose(speedMin, max)
      } yield (id.head, speedMin, speedMax, 0, "Authorized", false)::initRunnerList(id.tail, min, max).sample.get
    }
  }


  def genNewPos(initPos: List[(String, Int, Int, Int, String, Boolean)]): Gen[List[(String, Int, Int, Int, String, Boolean)]] = {
    if (initPos.isEmpty) for {
      tl <- Gen.const(Nil)
    } yield tl

    else {
      val runner = initPos.head
      for{
        distance <- Gen.choose(runner._2, runner._3)
      } yield banRunner((runner._1,runner._2,runner._3,runner._4 + distance, runner._5, runner._6))::genNewPos(initPos.tail).sample.get
    }
  }



  def genRace(id: List[String], goal: Int, min: Int, max: Int): Gen[ListStream[(String, Int, String, Boolean)]] = {
    for{
      init <-initRunnerList(id, min, max)
    } yield init.map(x => (x._1, x._4, x._5, x._6))::genRaceAux(init, goal).sample.get
  }


  def genRaceAux(runners: List[(String, Int, Int, Int, String, Boolean)], goal: Int): Gen[ListStream[(String, Int, String, Boolean)]] = {
   /* if(!winnerArrived){
      winnerArrived = getWinner(runners, goal)
    }*/
    val updateGoal = runners.filter(r => r._4 < goal)
    val updateBanned = updateGoal.filter(r => r._5 != "Banned")

    if(updateBanned.isEmpty)for {
      tl <- Gen.const(Nil)
    } yield tl
    else {
      for {
        p <- genNewPos(updateBanned)
      }yield p.map(x => findWinner((x._1, x._4, x._5, x._6),goal))::genRaceAux(p,goal).sample.get
    }
  }


  def banRunner(runner: (String, Int, Int, Int, String, Boolean)): (String, Int, Int, Int, String, Boolean) = {
    if((runner._5 != "Banned") && checkSpeed((runner._1, runner._4, runner._5, runner._6))){
      //println(runner._1 + " banned from competition.")
      (runner._1, runner._2, runner._3, runner._4, "Banned", runner._6)
    }
    else  runner
  }

  def findWinner(runner: (String, Int, String, Boolean), goal: Int): (String, Int, String, Boolean) = {
    if(!winnerArrived && (runner._2 >= goal) && (runner._3 != "Banned")){
      winnerArrived = true
      (runner._1, runner._2, runner._3, true)
    }
    else  runner
  }



  def nRaces(gen: Gen[ListStream[(String, Int, String, Boolean)]], n: Int): Gen[ListStream[(String, Int, String, Boolean)]] ={
    if(n == 0) Gen.const(Nil)
    else if(n == 1) gen
    else if(n == 2) WinGen.concat(gen, gen)
    else WinGen.concat(gen,nRaces(gen, n-1))
  }



  def checkSpeed(runner: (String, Int, String, Boolean)): Boolean = {
    var banned = runner._3 == "Banned"
    if(!previousPos.contains(runner._1)){
      previousPos += runner._1 -> (runner._2, runner._3)
    }
    else{
      if(runner._2 - previousPos(runner._1)._1 > maxSpeed){
        banned = true
        previousPos.update(runner._1, (runner._2, "Banned"))
      }
      else previousPos.update(runner._1, (runner._2, runner._3))
    }
    banned
  }

  def checkAllSpeeds(runners: List[(String, Int, String, Boolean)]): Boolean = {
    var banned = false
    for(runner <- runners) {
      banned = banned || checkSpeed(runner)
    }
    banned
  }




  def getWinner(runners: List[(String, Int, Int, Int, String, Boolean)], goal: Int): Boolean ={
    val winners = runners.filter(_._6)
    //println("Ganadores: " + winners)
   !winners.isEmpty
  }


  def isWinner(runner: (String, Int, String, Boolean)): Boolean ={
    runner._4
  }

  def isBanned(runner: (String, Int, String, Boolean)): Boolean ={
    runner._3 == "Banned"
  }


 def main(args: Array[String]): Unit ={
   val list = List("Kirby", "Molang", "Piupiu", "Pusheen", "Gudetama")

   val gen = genRace(list, 50, 1, 10)
   val u = nRaces(gen, 3).sample.get
   for(l <- u){
     println(l)
   }
 }



}

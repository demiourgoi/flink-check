package org.demo.race

import org.gen.{ListStream, WinGen}
import org.scalacheck.{Arbitrary, Gen}
import org.gen.ListStreamConversions._

import scala.collection.mutable.Map



//Contiene todos los generadores para la demo de carreras
object RaceGen {

  val maxSpeed = 8

  var previousPos: Map[String, (Int, String)]= Map[String, (Int, String)]()

  var winnerArrived = false

  //Inicializa a todos los corredores de una carrera
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


  //Genera un nuevo estado con la posicion de los corredores actualizada
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



  //Genera una nueva carrera
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


  //Descalifica un corredor de la carrera si el corredor ha cometido una ilegalidad
  def banRunner(runner: (String, Int, Int, Int, String, Boolean)): (String, Int, Int, Int, String, Boolean) = {
    if((runner._5 != "Banned") && checkSpeed((runner._1, runner._4, runner._5, runner._6))){
      //println(runner._1 + " banned from competition.")
      (runner._1, runner._2, runner._3, runner._4, "Banned", runner._6)
    }
    else  runner
  }

  //Busca al corredor que haya cruzado la meta primero
  def findWinner(runner: (String, Int, String, Boolean), goal: Int): (String, Int, String, Boolean) = {
    if(!winnerArrived && (runner._2 >= goal) && (runner._3 != "Banned")){
      winnerArrived = true
      (runner._1, runner._2, runner._3, true)
    }
    else  runner
  }


  //Genera n carreras
  def nRaces(gen: Gen[ListStream[(String, Int, String, Boolean)]], n: Int): Gen[ListStream[(String, Int, String, Boolean)]] ={
    if(n == 0) Gen.const(Nil)
    else if(n == 1) gen
    else if(n == 2) WinGen.concat(gen, gen)
    else WinGen.concat(gen,nRaces(gen, n-1))
  }


  //Comprueba que a velocidad de un corredor entra dentro de lo normal
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

  //Comprueba las velocidades de todos los corredores
  def checkAllSpeeds(runners: List[(String, Int, String, Boolean)]): Boolean = {
    var banned = false
    for(runner <- runners) {
      banned = banned || checkSpeed(runner)
    }
    banned
  }


  //Obtiene una lista con los ganadores de una carrera
  //Utilizado solo para depuracion y para visualizar una carrera, pero no en el testing
  def getWinner(runners: List[(String, Int, Int, Int, String, Boolean)], goal: Int): Boolean ={
    val winners = runners.filter(_._6)
    //println("Ganadores: " + winners)
   winners.nonEmpty
  }


  //Comprueba si un corredor es o no ganador
  def isWinner(runner: (String, Int, String, Boolean)): Boolean ={
    runner._4
  }

  //Comprueba si un corredor ha sido descalificado
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

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
 /** def genPos(runner: (String, Int, Int), pos: Int): Gen[(String, Int)] = {
    for {speed <- Gen.choose(runner._2, runner._3)} yield (runner._1, pos + speed)

  }

  /*def genInstant(gen: Gen[List[(String, Int, Int, Int)]]): Gen[List[(String, Int)]] = {
      for {
      runner <- gen
      xs <- genPos(runner,0)
    } yield xs
  }*/
  def genInstante(): Gen[List[(String, Int)]] = {
    Gen.listOfN(5, genPos(("holi", 1, 10), 0))
  }

  //List(list(string,pos)) -> carrera(instante(corredor))
  def genCarrera(gen: Gen[List[(String, Int)]]): Gen[ListStream[(String, Int)]] = {
    for {
      g <- gen
      out <- Gen.listOfN(3, g)
    } yield out
  }


  //Esto seria la inicializacion del corredor, falta tener su carrera.
  def initRunner(id: String): Gen[(String, Int, Int, Int)] = for {
    speedMin <- Gen.choose(1, 10)
    speedMax <- Gen.choose(speedMin, 10)
  } yield (id, speedMin, speedMax, 0)
*/

  def initRunnerList(id: List[String]): Gen[List[(String, Int, Int, Int, String, Boolean)]] = {
    winnerArrived = false
    previousPos = Map[String, (Int, String)]()
    if (id.isEmpty) for {
    tl <- Gen.const(Nil)
    } yield tl

    else {
      for {
        speedMin <- Gen.choose(1, 10)
        speedMax <- Gen.choose(speedMin, 10)
      } yield (id.head, speedMin, speedMax, 0, "Authorized", false)::initRunnerList(id.tail).sample.get
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



  def genRace(id: List[String], goal: Int): Gen[ListStream[(String, Int, String, Boolean)]] = {
    for{
      init <-initRunnerList(id)
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

  /**
  def genList[A:Arbitrary]: Gen[List[A]] = for {
    hd <- Arbitrary.arbitrary[A]
    tl <- Gen.oneOf(nilGen, genList[A])
  } yield hd::tl
  def nilGen: Gen[List[Nothing]] = Gen.const(Nil)



  def genRunnerRace(runner: Gen[(String, Int, Int, Int)], goal: Int): ListBuffer[(String, Int, Int, Int)] = {
    val r = new ListBuffer[(String, Int, Int, Int)]()
    r += runner.sample.get
    var pos = r.head
    var distance = 0
    while (pos._4 < goal) {
      pos = newPos(pos).sample.get
      r += pos
    }
    r
  }

    def newPos(r: (String, Int, Int, Int)) = for{
      distance <- Gen.choose(r._2, r._3)
    } yield (r._1, r._2, r._3, r._4 + distance)


  def raceRunner(id: String): ListBuffer[(String, Int, Int, Int)] = {
    val runner = initRunner(id)
    genRunnerRace(runner,50)
  }

  def race(idRunners: List[String]): List[List[(String, Int)]] = {


    var r = new ListBuffer[(String, Int, Int, Int)]()
    for(id <-idRunners) {
      r ++= raceRunner(id)
    }
    order(r)
  }

  def order(list: ListBuffer[(String, Int, Int, Int)]): List[List[(String,Int)]] ={
    var id = ""
    var cont = 0
    var inOrder = Map[Int, ListBuffer[(String, Int)]]()
    var newEntry = ("", 0)
    var result = new ListBuffer[List[(String,Int)]]()
    var i = 1

    for(runner <- list) {

      if (id != runner._1) {
        id = runner._1
        cont = 1
      }
      if (!inOrder.contains(cont)) {
        inOrder += cont -> new ListBuffer[(String, Int)]
      }

      newEntry = (runner._1, runner._4)
      inOrder.update(cont, inOrder(cont) += newEntry)
      cont += 1
    }

    while(inOrder.contains(i)){
      result += inOrder(i).toList
      i += 1
    }

    result.toList
  }



 def genRace(idRunners: List[String]): Gen[List[List[(String,Int)]]] ={
   for{
     r <- race(idRunners)
   } yield r
 }

  def checkNumberOfRunners(u: List[List[List[(String,Int)]]], numRunners: Int): Boolean = {
    var okay = true
    for(l1 <- u) {
      for (l2 <- l1) {
      okay = okay && l2.size <= numRunners
      }
    }
    okay
  }



  def check(d1: (String, Int), d2: (String, Int)): Unit ={
      d1._2 - d2._2 <= maxSpeed
  }

  def updatePos(d: (String, Int)): Unit ={
    previousPos = d
  }
*/

 def main(args: Array[String]): Unit ={
   val list = List("Kirby", "Molang", "Piupiu", "Pusheen", "Gudetama")


    // println(genRace(list, 50).sample.get)
  /*for(i <- 1 to 3) {
    val r = genRace(list, 50).sample.get
    for (l <- r) {
      println(l)
    }
    println()
  }*/


   val gen = genRace(list, 50)
   val u = nRaces(gen, 3).sample.get
   for(l <- u){
     println(l)
   }
 }



}

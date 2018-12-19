package org.demo.race

import org.scalacheck.Gen

import scala.collection.mutable.ListBuffer

object RaceGen {

  //Dopado para velocidad > 8
  //Velocidad entre 1 y 10

  //Incluir posibilidad de que los corredores se enfaden si hay más de N participantes haciendo trampa?
  //Incluir que un corredor tenga mas probabilidad de doparse si pierde mas de N veces?

//(id, pos, time?)
//Si (id, pos, t2) - (id,pos,t2) > N -> descalificado/dopado




  //Esto seria la inicializacion del corredor, falta tener su carrera.
  def initRunner(id: String): Gen[(String, Int, Int, Int)] = for {
    speedMin <-  Gen.choose(1,10)
    speedMax <- Gen.choose(speedMin,10)
  } yield (id, speedMin, speedMax, 0)




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

  def race(idRunners: List[String]): List[(String, Int, Int, Int)] = {


    var r = new ListBuffer[(String, Int, Int, Int)]()
    for(id <-idRunners) {
      r ++= raceRunner(id)

    }
    r.toList
  }








  def check(d1: (String, Int), d2: (String, Int)): Unit ={

  }


 def main(args: Array[String]): Unit ={
   val list = List("Kirby", "Molang", "Piupiu", "Pusheen", "Gudetama")

     println(race(list))

 }



}

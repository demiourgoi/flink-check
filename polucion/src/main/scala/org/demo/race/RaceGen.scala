package org.demo.race

import org.scalacheck.Gen

object RaceGen {

  //Incluir posibilidad de que los corredores se enfaden si hay más de N participantes haciendo trampa?
  //Incluir que un corredor tenga mas probabilidad de doparse si pierde mas de N veces?

//(id, pos, time?)
//Si (id, pos, t2) - (id,pos,t2) > N -> descalificado/dopado

  def distance(id: String, speedMin: Int, speedlMax: Int): Unit ={
    //Gen(id, pos+random(speedMin, speedMax))
  }

  def race(numRunners: Int): Unit = {
    //for(i<-1 to numRunners){
    //Gen(distance(i,rand,rand)
  //}
  }

  def check(d1: (String, Int), d2: (String, Int)): Unit ={

  }






}

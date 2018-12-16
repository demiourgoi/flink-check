package org.demo.pollution


import org.scalacheck.Gen
import scala.collection.mutable.Map


object PollutionGen {

  var control = Map[Int, Int]()
  var alarm = Map[Int, Boolean]()


  //Tal vez sea necesario tener tuplas tipo (idSensor, listaDatosPolucion, booleanAlarma)


  def genPol(maxPollution: Int, numSensor: Int): Gen[(Int, Int)] = for {
    pol <- Gen.choose(maxPollution, 100)
    sensor <- genSensorId(numSensor)
  } yield (pol, sensor)


  //Generador de datos sin polucion (menores que maxPollution)
  def genNoPol(maxPollution: Int, numSensor: Int): Gen[(Int, Int)] = for {
    pol <- Gen.choose(0, maxPollution - 1)
    sensor <- genSensorId(numSensor)
  } yield (pol, sensor)

  def genDataPol(min: Int, max: Int, numSensor: Int): Gen[(Int, Int)] = for {
    pol <- Gen.choose(min, max - 1)
    sensor <- genSensorId(numSensor)
  } yield (pol, sensor)

  def genSensorId(numSensor: Int): Gen[Int] ={
    Gen.choose(1, numSensor)
  }


  def sensorIdLessThan(list: List[(Int,Int)], numSensor: Int): Boolean = list match {
    case Nil => true
    case l :: ls => {
      if (l._2 <= numSensor) checkNoPol(ls, numSensor) else false
    }
  }

  def checkPol(list: List[(Int, Int)], maxPol: Int): Boolean = list match {
    case Nil => true
    case l::ls => {
      if(l._1 >= maxPol) checkPol(ls, maxPol)
      else false
    }
  }

  def checkNoPol(list: List[(Int, Int)], maxPol: Int): Boolean = list match {
    case Nil => true
    case l::ls => {
      if(l._1 < maxPol) checkNoPol(ls, maxPol)
      else false
    }
  }




  def alarmControl(gen: Gen[(Int, Int)], trigger: Int, maxPol: Int) = for {
    pol <- gen
  } yield updateAlarm(pol, trigger, maxPol)


  //Cambiar genPol a List[(Int,Int)]
  def updateAlarm(pol: (Int,Int), trigger: Int, maxPol: Int) = {

    if (alarm.get(pol._2) == None) {
      alarm += (pol._2 -> false)
      control += (pol._2 -> 0)
    }

    if (pol._1 >= maxPol) {
      if (control(pol._2) != trigger) {
        control(pol._2) = control(pol._2) + 1
      }
    }
    else{
      if (control(pol._2) != 0) {
        control(pol._2) = control(pol._2) - 1
      }
    }

    if (control(pol._2) == 0) {
      alarm(pol._2) = false
    }
    else if(control(pol._2) == trigger) {
      alarm(pol._2) = true
    }

    alarm

  }

  def initControl(numSensor: Int): Map[Int, Int] = {
    var control = Map[Int, Int]()
    for(i<- 1 to numSensor){
      control += i -> 0
    }
    control
  }


  def initAlarm(numSensor: Int): Map[Int, Boolean] = {
    var alarm = Map[Int, Boolean]()
    for(i<- 1 to numSensor){
      alarm += i -> false
    }
    alarm
  }

  def alarmController(data: List[(Int, Int)], numSensor: Int, maxPol: Int, trigger: Int): Boolean ={
    var control = initControl(numSensor)
    var alarm = initAlarm(numSensor) //Map[Int, Boolean]()

    for(d <- data){

      if (d._1 >= maxPol) {
        if (control(d._2) != trigger) {
          control(d._2) = control(d._2) + 1
        }
      }
      else{
        if (control(d._2) != 0) {
          control(d._2) = control(d._2) - 1
        }
      }

      if (control(d._2) == 0) {
        alarm(d._2) = false
      }
      else if(control(d._2) == trigger) {
        alarm(d._2) = true
      }
    }

    var result = false
      for(i<-1 to numSensor){
      result = result || alarm(i)
    }
    result
  }







}

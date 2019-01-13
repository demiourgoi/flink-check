package org.demo.pollution


import org.scalacheck.Gen
import scala.collection.mutable.Map


//Contiene todos los generadores para la demo de polucion
object PollutionGen {

  var control = Map[Int, Int]()
  var alarm = Map[Int, Boolean]()



  //Generador de datos que superan el maximo de polucion
  def genPol(maxPollution: Int, numSensor: Int): Gen[(Int, Int)] = for {
    pol <- Gen.choose(maxPollution, 100)
    sensor <- genSensorId(numSensor)
  } yield (pol, sensor)


  //Generador de datos sin polucion (menores que maxPollution)
  def genNoPol(maxPollution: Int, numSensor: Int): Gen[(Int, Int)] = for {
    pol <- Gen.choose(0, maxPollution - 1)
    sensor <- genSensorId(numSensor)
  } yield (pol, sensor)

  //Generador de datos de polucion entre min y max
  def genDataPol(min: Int, max: Int, numSensor: Int): Gen[(Int, Int)] = for {
    pol <- Gen.choose(min, max - 1)
    sensor <- genSensorId(numSensor)
  } yield (pol, sensor)

  //Generador de identificadores de sensor, entre 1 y numSensor
  def genSensorId(numSensor: Int): Gen[Int] ={
    Gen.choose(1, numSensor)
  }


  //Comprueba que los ids de una lista sean menores o iguales al total de sensores
  def sensorIdLessThan(list: List[(Int,Int)], numSensor: Int): Boolean = list match {
    case Nil => true
    case l :: ls => {
      if (l._2 <= numSensor) sensorIdLessThan(ls, numSensor) else false
    }
  }

  //Comprueba que todos los datos de una lista son de polucion
  def checkPol(list: List[(Int, Int)], maxPol: Int): Boolean = list match {
    case Nil => true
    case l::ls => {
      if(l._1 >= maxPol) checkPol(ls, maxPol)
      else false
    }
  }

  //Comprueba que todos los datos de una lista son de no polucion
  def checkNoPol(list: List[(Int, Int)], maxPol: Int): Boolean = list match {
    case Nil => true
    case l::ls => {
      if(l._1 < maxPol) checkNoPol(ls, maxPol)
      else false
    }
  }

  //Comprueba que haya algun dato de polucion en la lista
  def checkAnyPol(list: List[(Int, Int)], maxPol: Int): Boolean = list match {
    case Nil => true
    case l => l.filter( x => x._1 >= maxPol).size > 0
  }

  //Comprueba que haya algun dato de no polucion en la lista
  def checkAnyNoPol(list: List[(Int, Int)], maxPol: Int): Boolean = list match {
    case Nil => true
    case l => l.filter( x => x._1 < maxPol).size > 0
  }


  //Inicializa la variable que guarda la informacion utilizada para decidir el estado de la alarma
  def initControl(numSensor: Int): Map[Int, Int] = {
    var control = Map[Int, Int]()
    for(i<- 1 to numSensor){
      control += i -> 0
    }
    control
  }


  //Inicializa las alarmas de los sensores
  def initAlarm(numSensor: Int): Map[Int, Boolean] = {
    var alarm = Map[Int, Boolean]()
    for(i<- 1 to numSensor){
      alarm += i -> false
    }
    alarm
  }


  //Actualiza el controllador con la informacion necesario (numero de datos de polucion detectados en cada sensor
  //y el estado de la alarma de cada sensor
  def alarmController(data: List[(Int, Int)], numSensor: Int, maxPol: Int, trigger: Int): Boolean ={
    var control = initControl(numSensor)
    var alarm = initAlarm(numSensor)
    var result = false

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
      for(i<-1 to numSensor){
      result = result || alarm(i)
    }


    result
  }







}

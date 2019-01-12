package org.Examples.pollution

import org.apache.flink.api.common.functions.AggregateFunction



//Contador con el numero de valores mayores o iguales que el maximo nivel de polucion
//Tambien guarda el sensor que ha detectado esos valores
class Counter(init : Int, sensor : Int) {
  var acc = init
  var sensorID = sensor
}



class Count(max : Int) extends AggregateFunction[(Int,Int), Counter, (Int,Int)] {

  //Polucion maxima
  var maximum = max

  //Inicializa el contador
  def createAccumulator() : Counter = new Counter(0,-1)

  //Cuando se mezclan dos contadores debe sumar el numero de valores mayores o iguales que el
  // maximo nivel de polucion de ambos
  def merge(a: Counter, b: Counter): Counter = {
    a.acc += b.acc
    a
  }

  //Cuando le llega un nuevo valor guarda el id del sensor y si
  // el valor es mayor o igual que el maximo, actualiza el contador
  def add(value: (Int, Int), acc: Counter) = {
    acc.sensorID = value._2
    if (value._1 >= maximum) acc.acc += 1
  }

  //El resultado que devuelve es el numero de valores mayores o iguales que el maximo nivel de polucion
  //de la ventana junto con el sensor que los detecto
  def getResult(acc: Counter): (Int, Int) = (acc.acc, acc.sensorID)
}

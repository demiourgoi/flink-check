package org.Examples.pollution

import org.apache.flink.api.common.functions.AggregateFunction


//Acumulador de los valores que va recibiendo, junto al id del sensor que los detecta
class AccumulateValues(init : List[Int], id : Int){
  var l = init
  var sensor = id
}


class AccumulateWindows(rep : Int) extends AggregateFunction[(Int, Int), AccumulateValues, (Int,Int)] {

  //Minimo de repeticiones para cambiar el estado de la alarma
  var repetitions : Int = rep

  //Inicializa el acumulador
  def createAccumulator() : AccumulateValues = new AccumulateValues(List(),-1)

  //En caso de juntar dos acumuladores, acumula en a los valores acumulados en b
  def merge(a: AccumulateValues, b: AccumulateValues): AccumulateValues = {
    a.l = a.l:::b.l
    a
  }

  //Cuando llega un nuevo dato, guarda el id del sensor e
  //incluye el nuevo valor en la lista de valores acumulados
  def add(value: (Int, Int), acc: AccumulateValues) = {
    acc.sensor = value._2
    acc.l = value._1 :: acc.l
  }

  def getResult(acc: AccumulateValues): (Int,Int) = {
    //Si la lista tiene tantos datos como repeticiones necesarias
    if (acc.l.length == repetitions){
      //Se guardan los valores de la lista mayores que 0
      val greaterList = acc.l.filter{x => x > 0}
      //Si la nueva lista tiene tantos datos como repeticiones necesarias
      //devuelve un 2 (indicando que se cumple el requisito para
      //activar la alarma) y el id del sensor
      if (greaterList.length == repetitions) return (2,acc.sensor)
      //Si  la lista queda vacia, es porque no se supero la polucion
      //maxima, por lo que se devuelve un 0 junto al id del sensor
      else if (greaterList.length == 0) return (0,acc.sensor)
      //Para mantener la alarma igual (a falta de tener suficientes datos que indiquen
      //la necesidad de cambiar su estado) se devuelve un 1 y el id del sensor
      else return (1,acc.sensor)
    }
    //Si no se ha llegado aun al numero de repeticiones necesarias, se devuelve un 1 y el id
    //del sensor, pues no se cambia la alarma
    else return (1,acc.sensor)
  }
}
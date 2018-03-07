package org.polucion

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time



object Polucion {
  def main(args: Array[String]) {

    //Establece el entorno de ejecucion
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //Utiliza processing time
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //Obtiene los datos del puerto 9000
    //Un programa Scala inserta valores aleatorios en este puerto
    val text = env.socketTextStream("localhost", 9000, '\n')

    //Transforma los datos leidos a tuplas (polucion, sensor)
    val mapped = text.map { x => (x.split(",")(0).toInt, x.split(",")(1).toInt)}



    //Separa los streams por sensor
    val keyValue = mapped.keyBy(1)

    //Recoge los datos en ventanas de 5 segundos
    val tumblingWindow  = keyValue.timeWindow(Time.seconds(5))

    //Maximo nivel de polucion
    val maxPollution = 20


    //Numero de ventanas a esperar para cambiar el estado de la alarma
    val windows = 3


    //Recogemos los datos en ventanas con capacidad para 3 datos, y contamos el numero de datos que superan la polucion
    //maxima en cada ventana
    //De nuevo separa los flujos por sensor
    val countWindowVal = tumblingWindow.aggregate(new Count(maxPollution)).keyBy(1).countWindow(windows,1)


    //Acumulamos los valores para poder activar y desactivar la alarma en los momentos adecuados,
    //ya que para activarse debe esperar a que se supere el maximo de contaminacion durante
    //'n' ventanas seguidas.
    //Equivalente para desactivarse.
    val accumulateWindow = countWindowVal.aggregate(new AccumulateWindows(windows))


    //Se separan los flujos de datos por sensor.
    //Se aplica un map que mantenga el estado de la alarma (activada o desactivada) y la active
    //o desactive segun los datos que reciba y su estado anterior.
    //Se muestra el resultado
    accumulateWindow.keyBy(1)
      .mapWithState((in: (Int, Int), alarm: Option[Boolean]) =>
        alarm match {
          case Some(c) => if(c && in._1 == 0) ( "Alarma desactivada en sensor " + in._2, Some(false) ) else if (!c && in._1 == 2) ("Alarma activada en sensor " + in._2, Some(true) ) else ("Nada en sensor " + in._2, Some(c))
          case None => if(in._1 == 2) ("Alarma activada en sensor " + in._2, Some(true) ) else ("Nada en sensor " + in._2, Some(false))
        }).print()

    //Ejecutar el programa
    env.execute()


  }

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





}
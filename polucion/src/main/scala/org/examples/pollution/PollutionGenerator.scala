package org.Examples.pollution

import java.io.PrintStream
import java.net.{ServerSocket, Socket}


//Esta clase representa a un sensor. Cada hilo será un sensor distinto que genere sus propios datos de polucion.
class SensorThread(it: Int, id: Int, client: Socket) extends Runnable {
  //Numero de datos a generar
  val iterations = it
  //Identificador del sensor
  val sensor = id

  //Generador de numeros aleatorios
  val r = scala.util.Random

  /*Se inicializan las variables mediante las que se calculara el valor de polucion.
  El generador de numeros aleatorios solo genera rangos de (0,n), por lo que se usara
  start y end para obtener valores aleatorios en el rango (a,b) deseado, y este valor
  definitivo se guardara en polution.
  Estas variables solo son necesarias para manipular los valores aleatorios y así forzar
  un comportamiento determinado en el programa.
   */
  var pollution = 0
  var start = 0
  var end   = 0



  def run {
    try
    {
      //Se crea un stream a partir del socket del cliente para enviar los datos
      val out = new PrintStream(client.getOutputStream)

      //Se abre un bucle para generar los datos
      for(i <- 1 to iterations){

        /*Se modifican los limites del rango de los datos generados para forzar
          que los sensores se activen y desactiven una vez
         */
        if(i < 100) { //Todos los datos superaran la polucion permitida y se activaran las alarmas
          start = 20
          end = 50
        }
        else if((100 <= i) && (i < 200)) { //Ningun dato superara la polucion permitida y se desactivaran las alarmas
          start = 0
          end = 19
        }
        else {
          //La polucion permitida vuelve a sobrepasarse y las alarmas volveran a activarse
          start = 20
          end = 50
        }

        //Calculo de la polucion mediante el numero aleatorio y los limites del rango
        pollution = start + r.nextInt( (end - start) + 1 )
        //Se crea el mensaje a enviar con la polucion y el sensor que la ha detectado
        val message = pollution + "," + sensor + '\n'
        //Se envia el mensaje
        out.print(message)
        Thread.sleep(500)
        //Se muestra el mensaje por consola
        print(message)
        //Se limpia el stream
        out.flush()
      }
    }
    catch
      {
        case e: Exception => println(e.getStackTrace); System.exit(1)
      }
  }

}



//Aqui se abre el servidor y se crean los sensores (threads)
object PollutionGenerator {

  //Numero de datos a generar por sensor
  val iterations = 300
  //Numero de sensores
  val numSensor = 3
  //Lista para guardar los threads y poder esperar a que todos terminen
  var list = List[Thread]()

  def main(args: Array[String]) {
    //Se abre un puerto
    val server = new ServerSocket(9000)
    server.setReuseAddress(true)
    println("Server initialized")
    //Conecta con el cliente
    val client = server.accept
    println("server accepted")

    //Creacion de los sensores (threads)
    1 to numSensor foreach { x =>
      println("Creando sensores...")
      //Hay que enviarles el numero de datos que deben generar, su identificador, y la conexion con el cliente
      val thread = new Thread(new SensorThread(iterations, x, client))
      //Se guarda cada thread en la lista
      list = thread :: list
      //Arranca el thread
      thread.start()
      Thread.sleep(500)
    }
    println("Sensores creados")
    //Espera a que todos los threads acaben
    for(i <- 0 to numSensor-1){
      list(i).join()
    }
    Thread.sleep(10000)
    //Cierra el servidor
    server.close
  }


}

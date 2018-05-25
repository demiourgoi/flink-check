package org.gen

import org.scalacheck.Gen
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows


object WinGen {
  //generador de listas de GlobalWindows
  //val genDataList = Gen.containerOfN[List, GlobalWindows](3,GlobalWindows.create())


  //Generador de count windows
  //size: numero de datos de cada ventana
  //numWindows: numero de ventanas a generar
  //env: el entorno de ejecución para generar el datastream
  def winGen(size: Int, numWindows: Int, env: StreamExecutionEnvironment) = {

    //Genera los datos en tuplas con valores de polucion e ids de sensores aleatorios
    val genPol = for {
      p <- Gen.listOfN((size+1)*numWindows, Gen.choose(1,100)).sample.get
    } yield (p, Gen.choose(1,3).sample.get)

    //Genera el stream a partir del generador
    val text = env.fromCollection(genPol)

    //Divide el stream en ventanas
    text.countWindowAll(size+1)

  }


  def main(args: Array[String]): Unit = {

    //Establece el entorno de ejecucion
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Fold de las count window generadas sólo para comprobar su contenido
    winGen(5,10,env).fold(""){(acc, v) => println(acc)
                                         acc+ v._1}


    env.execute()

  }










}
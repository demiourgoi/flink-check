package org.gen


import org.gen.WinGen._
import org.scalacheck.Gen
import org.apache.flink.streaming.api.scala._

object DemoWinGen {

  val maxPollution = 20
  val numSensor = 3



  def gen = for{
    g <- Gen.choose(1,100)
  } yield g

  //Generador de datos con polucion (mayores o iguales que maxPollution)
  def genPol = for {
    pol <- Gen.choose(maxPollution, 100)
    sensor <- Gen.choose(1, numSensor)
  } yield (pol, sensor)


  //Generador de datos sin polucion (menores que maxPollution)
  def genNoPol: Gen[(Int, Int)] = for {
    pol <- Gen.choose(0, maxPollution - 1)
    sensor <- Gen.choose(1, numSensor)
  } yield (pol, sensor)



  def main(args: Array[String]): Unit = {
    val size =1 //size windows
    val time = 4 //instantes

    val pol = ofN(size,genPol)
    val noPol = ofN(size,genNoPol)

    //val data = always(noPol,time)
    //val data = concat(ofN(2, ofN(1, Gen.const("adios"))), ofN(6, ofN(1, Gen.const("hola"))))
    val data = eventually(noPol, time)

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    toWindowsList(data, env).fold(""){(acc, v) => println(acc+v)
      acc + v}

    env.execute()
  }

}

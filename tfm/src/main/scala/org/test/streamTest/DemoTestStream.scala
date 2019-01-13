package org.test.streamTest

import org.scalacheck.{Gen, Prop}
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.ResultMatchers
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.test.{Formula, Test}
import org.test.Formula._


//Clase con tests para probar la funcion test que recibe un stream
class DemoTestStream extends Specification
  with ScalaCheck
  with ResultMatchers
  with Serializable {

  def is =
    sequential ^ s2"""  TestStream Demo
      - where a simple formula must hold on stream ${testAlwaysStream}
      - the stream must have '1' first and '2' next ${testPollutionStream}
    """


  def testAlwaysStream = {
    val t1 = System.nanoTime
    type U = String
    val times = 8
    val numData = 8
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromCollection(List("hola", "hola", "hola", "hola", "hola", "hola", "hola", "hola"))
    val window = ds.countWindowAll(1)
    val formula : Formula[U] = always { (u : U) =>
      u contains "hola"
    } during times
    val result = Test.testStream[U](window.asInstanceOf[AllWindowedStream[Any, GlobalWindow]], formula, env)
    println("AlwaysStream: ")
    result.print
    env.execute()
    println("Duration: " +  (System.nanoTime() - t1) / 1e9d)
    result.toString
  }




  def testPollutionStream = {
    type U = (Int, Int)
    val times = 100
    val numData = 300
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = PollutionForTestStream.demoPol(env).countWindowAll(1)
    val f1 : Formula[U] = (u : U) => (u._1 == 1)
    val f2 : Formula[U] = (u : U) => (u._1 == 2)
    val formula : Formula[U] = (u : U) => f1 until f2 on times
    val result = Test.testStream[U](stream.asInstanceOf[AllWindowedStream[Any, GlobalWindow]], formula, env)
    println("Polucion: ")
    result.print
    env.execute()
    result.toString
  }


}

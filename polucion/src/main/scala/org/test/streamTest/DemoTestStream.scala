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

class DemoTestStream extends Specification
  with ScalaCheck
  with ResultMatchers
  with Serializable {

  def is =
    sequential ^ s2"""    Simple demo Specs2 for a formula
      - where a simple formula must hold on a list ${testAlwaysStream}
      - where a simple formula must hold on a list ${testPollutionStream}


    """


  def testAlwaysStream = {
    type U = String
    val times = 8
    val numData = 8
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromCollection(List("hola", "hola", "hola", "hola", "hola", "hola", "hola", "hola"))
    val window = ds.countWindowAll(1)
    val formula : Formula[U] = always { (u : U) =>
      u contains "hola"
    } during times
    val result = Test.testStream[U](window.asInstanceOf[AllWindowedStream[Any, GlobalWindow]], formula, env, times, numData)
    println("AlwaysStream: ")
    result.print
    env.execute()
    result.toString
  }




  def testPollutionStream = {
    type U = (Int, Int)
    val times = 10
    val numData = 8
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = PollutionForTestStream.demoPol(env).countWindowAll(1)
    val f1 : Formula[U] = (u : U) => (u._1 == 1)
    val f2 : Formula[U] = (u : U) => (u._1 == 2)
    val formula : Formula[U] = (u : U) => f1 until f2 on times
    val result = Test.testStream[U](stream.asInstanceOf[AllWindowedStream[Any, GlobalWindow]], formula, env, times, numData)
    println("Polucion: ")
    result.print
    env.execute()
    result.toString
  }


}
package org.test

import org.scalacheck.{Gen, Prop}
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.ResultMatchers
import Formula._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.gen.ListStreamConversions._
import org.gen.WinGen
import org.test.Test.test



//Esta clase contiene diversos tests con los que probar la funcion test
class DemoTest extends Specification
  with ScalaCheck
  with ResultMatchers
  with Serializable {

  def is =
    sequential ^ s2""" Test function demo
      - every window must contain at least three "hola" ${testAlways}
      - "hola" must eventually appear in a window ${testEventually}
      - there must be windows containing "hola" until a window contains "adios" ${testUntil}
      - there must be windows containing "hola" until a window contains both "hola" and "adios ${testRelease}

    """



  def testAlways = {
    type U = String
    val times = 8
    val numData = 8
    val numTests = 100
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val genData: Gen[List[List[U]]] = WinGen.always[String](WinGen.ofN[String](5, Gen.const("hola")), numData)
    val formula : Formula[List[U]] = always { (u : List[U]) =>
      checkThree(u)
    } during times
    val result = test[U](genData, formula, env, numTests)
    println("Always: ")
    result.print
    //println(result)
    env.execute()
    result.toString
  }

  def checkThree[U](u:  List[U]): Boolean ={
    var cont = 0
    u.foreach(x => if(x.equals("hola")) cont+=1)
    if(cont >= 3) true
    else false
  }

  def testEventually = {
    type U = String
    val times = 8
    val numData = times
    val numTests = 20
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val genData: Gen[List[List[U]]] = WinGen.eventually[String](WinGen.ofN[String](1, Gen.const("hola")), numData)
    val formula : Formula[List[U]] = later { (u : List[U]) =>
      u contains "hola"
    } during times
    val result = test[U](genData, formula, env, numTests)
    println("Eventually: ")
    result.print
    env.execute()
    result.toString
  }

  def testUntil = {
    type U = String
    val times = 8
    val numData = times
    val numTests = 10
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val genData: Gen[List[List[U]]] = WinGen.until[String](WinGen.ofN[String](1, Gen.const("hola")), WinGen.ofN[String](1, Gen.const("adios")), numData)
    val f1 : Formula[List[U]] = (u : List[U]) => (u contains ("hola"))
    val f2 : Formula[List[U]] = (u : List[U]) => (u contains ("adios"))
    val formula : Formula[List[U]] =  f1 until f2 on times
    val result = test[U](genData, formula, env, numTests)
    println("Until: ")
    result.print
    env.execute()
    result.toString
  }

  def testRelease = {
    type U = String
    val times = 8
    val numData = times
    val numTests = 10
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val genData: Gen[List[List[U]]] = WinGen.release[String](WinGen.ofN[String](1, Gen.const("hola")), WinGen.ofN[String](1, Gen.const("adios")), numData)
    val f1 : Formula[List[U]] = (u : List[U]) => (u contains ("hola"))
    val f2 : Formula[List[U]] = (u : List[U]) => (u contains ("adios"))
    val formula : Formula[List[U]] = f1 release f2 on times
    val result = test[U](genData, formula, env, numTests)
    println("Release: ")
    result.print
    env.execute()
    result.toString
  }


}
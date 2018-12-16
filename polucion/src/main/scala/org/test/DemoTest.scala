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
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.gen.WinGen.{concat, ofN}
import org.pollution.PollutionForTestStream


class DemoTest extends Specification
  with ScalaCheck
  with ResultMatchers
  with Serializable {

  def is =
    sequential ^ s2"""    Simple demo Specs2 for a formula
      - where a simple formula must hold on a list ${testPollutionStream}
      - where a simple formula must hold on a list ${testRelease}
      - where a simple formula must hold on a list ${testUntil}
      - where a simple formula must hold on a list ${testEventually}

    """


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
    print("Polucion: ")
    result.print
    env.execute()
    result.toString
  }

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
    print("Always: ")
    result.print
    env.execute()
    result.toString
  }


  def testAlways = {
    type U = String
    val times = 10
    val numData = 8
    val numTests = 100
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val genData: Gen[List[List[U]]] = WinGen.always[String](WinGen.ofN[String](6, Gen.const("hola")), numData)
    //val data = concat(ofN(2, ofN(1, Gen.const("adios"))), ofN(6, ofN(1, Gen.const("hola"))))
    val formula : Formula[List[U]] = always { (u : List[U]) =>
      checkThree(u)
    } during times
    val result = test[U](genData, formula, env, numTests)
    print("Always: ")
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
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val genData: Gen[List[List[U]]] = WinGen.eventually[String](WinGen.ofN[String](1, Gen.const("hola")), numData)
    val formula : Formula[List[U]] = later { (u : List[U]) =>
      u contains "hola"
    } during times
    val result = test[U](genData, formula, env, times)
    print("Eventually: ")
    result.print
    //println(result)
    env.execute()
    result.toString
  }

  def testUntil = {
    type U = String
    val times = 8
    val numData = times
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val genData: Gen[List[List[U]]] = WinGen.until[String](WinGen.ofN[String](1, Gen.const("hola")), WinGen.ofN[String](1, Gen.const("adios")), numData)
    val f1 : Formula[List[U]] = (u : List[U]) => (u contains ("hola"))
    val f2 : Formula[List[U]] = (u : List[U]) => (u contains ("adios"))
    val formula : Formula[List[U]] = (u : List[U]) => f1 until f2 on times
    val result = test[U](genData, formula, env, 10)
    print("Until: ")
    result.print
    //println(result)
    env.execute()
    result.toString
  }

  def testRelease = {
    type U = String
    val times = 8
    val numData = times
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val genData: Gen[List[List[U]]] = WinGen.release[String](WinGen.ofN[String](1, Gen.const("hola")), WinGen.ofN[String](1, Gen.const("adios")), numData)
    val f1 : Formula[List[U]] = (u : List[U]) => (u contains ("hola"))
    val f2 : Formula[List[U]] = (u : List[U]) => (u contains ("adios"))
    val formula : Formula[List[U]] = (u : List[U]) => f1 release f2 on times
    val result = test[U](genData, formula, env, times)
    print("Release: ")
    result.print
    //println(result)
    env.execute()
    result.toString
  }


  /*
    def prueba2 = {
      //val l_prueba : List[(Int,String)] = List((1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"))
      val l_prueba : List[String] = List("hola","hola","hola","hola","adios","hola","hola")
      type U = String
      val formula : Formula[U] = always { (u : U) =>
        u contains "hola"
      } during 6
      val result = test[String](l_prueba, formula)
      println(result)
      result.toString
    }


    def prueba3 = {
      val l_prueba : List[(Int,String)] = List((1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"))
      type U = (Int,String)
      val formula : Formula[U] = always { (u : U) =>
        u._1 must be_<=(3)
      } during 6
      val result = test[(Int,String)](l_prueba, formula)
      println(result)
      result.toString
    }

    def pruebaEventually = {
      type U = List[String]
      val g_prueba: Gen[List[U]] = WinGen.eventually[String](WinGen.ofN[String](1, Gen.const("hola")), 8)
      val formula : Formula[U] = later[U]{ (u : U) =>
        u contains "hola"
      } during 8
      val result = test[U](g_prueba, formula)
      println(result)
      result.toString
    }

    def pruebaUntil = {
      type U = List[String]
      val g_prueba: Gen[List[U]] = WinGen.until[String](WinGen.ofN[String](1, Gen.const("hola")), WinGen.ofN[String](1, Gen.const("adios")), 8)
      val f1 : Formula[U] = (u : U) => (u contains ("hola"))
      val f2 : Formula[U] = (u : U) => (u contains ("adios"))
      val formula : Formula[U] = (u : U) => f1 until f2 on 8
      val result = test[U](g_prueba, formula)
      println(result)
      result.toString
    }

    def pruebaRelease = {
      type U = List[String]
      val g_prueba: Gen[List[U]] = WinGen.release[String](WinGen.ofN[String](1, Gen.const("hola")), WinGen.ofN[String](1, Gen.const("adios")), 8)
      val f1 : Formula[U] = (u : U) => (u contains ("hola"))
      val f2 : Formula[U] = (u : U) => (u contains ("adios"))
      val formula : Formula[U] = (u : U) => f1 release f2 on 8
      val result = test[U](g_prueba, formula)
      println(result)
      result.toString
    }

    */



}
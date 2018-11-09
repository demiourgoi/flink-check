package org.test


import org.scalacheck.{Gen, Prop}
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.ResultMatchers
import org.scalacheck.Arbitrary.arbitrary
import Formula._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.gen.ListStream
import org.gen.ListStreamConversions._
import org.gen.WinGen
import org.test.Test.test

class Test {

}

object Test {


  def test[U](gen: Gen[List[U]], form : Formula[U]) : Prop.Status = {
    var currFormula = form.nextFormula
    gen.sample.get.foreach( elem =>
      if (currFormula.result.isEmpty){
        currFormula = currFormula.consume(Time(1))(elem)
      }
    )
    return currFormula.result.getOrElse(Prop.Undecided)
  }


  def main(args: Array[String]): Unit = {

  }

}


class Demo extends Specification
  with ScalaCheck
  with ResultMatchers {

  def is =
    sequential ^ s2"""
    Simple demo Specs2 for a formula
      - where a simple formula must hold on a list ${prueba1}
      - where a simple formula must not hold on a list ${prueba2}
      - where a simple formula must not hold on a list of tuples ${prueba3}
    """


  /*def prueba1 = {
    //val l_prueba : List[(Int,String)] = List((1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"),(1,"hola"))
    val l_prueba : List[String] = List("hola","hola","hola","hola","hola","hola","hola")
    type U = String
    val formula : Formula[U] = always { (u : U) =>
      u contains "hola"
    } during 6
    val result = test[String](l_prueba, formula)
    println(result)
    result.toString
  }
  */

  def prueba1 = {
    type U = List[String]
    val g_prueba: Gen[List[U]] = WinGen.always[String](WinGen.ofN[String](8, Gen.const("hola")), 8)
    val formula : Formula[U] = always { (u : U) =>
      u.length == 3
    } during 6
    val result = test[U](g_prueba, formula)
    println(result)
    result.toString
  }

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



}

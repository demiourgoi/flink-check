package org.test


import org.scalacheck.{Gen, Prop}
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.ResultMatchers
import Formula._
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.gen.ListStream
import org.gen.ListStreamConversions._
import org.gen.WinGen
import org.test.Test.test
import org.apache.flink.streaming.api.scala._
import org.gen.WinGen.{concat, ofN}




//VERSION BUENA

class Test {


}

object Test {



  def testList[U](gen: Gen[List[U]], form: Formula[U]): Prop.Status = {
    var currFormula = form.nextFormula
    gen.sample.get.foreach(elem => if (currFormula.result.isEmpty) {
      currFormula = currFormula.consume(Time(1))(elem)
    })
    return currFormula.result.getOrElse(Prop.Undecided)
  }


  def test[U](gen: Gen[ListStream[U]], form: Formula[List[U]], env: StreamExecutionEnvironment, times: Int, numData: Int): DataStream[Prop.Status] = {
    var currFormula = form.nextFormula
    val w = WinGen.toWindowsList(gen, env)
    val formulaCopy : NextFormula[List[U]] = currFormula
    val resultWindows = w.aggregate(new ForEachWindow[U](formulaCopy, times, numData))
    val dataStream: DataStream[Prop.Status] = env.fromCollection(List(Prop.Undecided))
    resultWindows.filter(_._1).map(_._2)
  }


  def main(args: Array[String]): Unit = {

  }

}


//Acumulador de los valores que va recibiendo, junto al id del sensor que los detecta
class WindowResult( r: (Boolean, Prop.Status)){
  var result = r
}


class ForEachWindow[U](formula: => NextFormula[List[U]], times: Int, numData: Int) extends AggregateFunction[List[Any], WindowResult, (Boolean, Prop.Status)] {

  var f = formula
  var cont = 0
  var done = false



  def createAccumulator() : WindowResult = {
    new WindowResult((false,Prop.Undecided))
  }


  def merge(a: WindowResult, b: WindowResult): WindowResult = {
    var okay = a.result._1 && b.result._1
    var res: Prop.Status = Prop.Undecided
    if(a.result._2 == Prop.True && b.result._2 == Prop.True) res = Prop.True
    else if (a.result._2 == Prop.False || b.result._2 == Prop.False) res = Prop.False
    new WindowResult((okay, res))
  }


  def add(data: List[Any], wr: WindowResult) = {
    //println("add")
    /*data.asInstanceOf[List[U]].foreach(elem => if (wr.formula.result.isEmpty) {
      println("Adding " + elem)
      wr.formula = wr.formula.consume(Time(1))(elem)
      println(wr.formula)
    })*/

    //if(cont < times) {
    if (f.result.isEmpty) {
      f = f.consume(Time(1))(data.asInstanceOf[List[U]])
    }
    //}
    cont += 1



  }

  def getResult(wr: WindowResult): (Boolean, Prop.Status) = {

    //println(cont + " - " + f.result.getOrElse(Prop.Undecided) )
      val resul = !f.result.isEmpty && !done
      if(!f.result.isEmpty) done = true
      (resul, f.result.getOrElse(Prop.Undecided))


    /*if((cont == times || cont == numData) && !done){
      done = true
      (true, f.result.getOrElse(Prop.Undecided))
    }
    else (false, f.result.getOrElse(Prop.Undecided))*/


  }


}


class Demo extends Specification
  with ScalaCheck
  with ResultMatchers
  with Serializable {

  def is =
    sequential ^ s2"""
    Simple demo Specs2 for a formula
      - where a simple formula must hold on a list ${pruebaAlways}
      - where a simple formula must hold on a list ${pruebaEventually}
      - where a simple formula must hold on a list ${pruebaUntil}
      - where a simple formula must hold on a list ${pruebaRelease}

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


    def pruebaAlways = {
    type U = List[String]
    val g_prueba: Gen[List[U]] = WinGen.always[String](WinGen.ofN[String](8, Gen.const("hola")), 8)
    val formula : Formula[U] = always { (u : U) =>
      u contains "hola"
    } during 6
    val result = test[U](g_prueba, formula)
    println(result)
    result.toString

  }
  */


  def pruebaAlways = {
    type U = String
    val times = 8
    val numData = 8
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val g_prueba: Gen[List[List[U]]] = WinGen.always[String](WinGen.ofN[String](2, Gen.const("hola")), numData)
    val a = concat(ofN(2, ofN(1, Gen.const("adios"))), ofN(6, ofN(1, Gen.const("hola"))))
    //println(g_prueba.sample.get)
    val formula : Formula[List[U]] = always { (u : List[U]) =>
      u contains "hola"
    } during times
    //println(formula)
    val result = test[U](g_prueba, formula, env, times, numData)
    print("Always: ")
    result.print
    print(result.toString)
    env.execute()
    result.toString
  }

  def pruebaEventually = {
    type U = String
    val times = 8
    val numData = times
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val g_prueba: Gen[List[List[U]]] = WinGen.eventually[String](WinGen.ofN[String](1, Gen.const("hola")), numData)
    val formula : Formula[List[U]] = later { (u : List[U]) =>
      u contains "hola"
    } during times
    val result = test[U](g_prueba, formula, env, times, numData)
    print("Eventually: ")
    result.print
    env.execute()
    result.toString
  }

  def pruebaUntil = {
    type U = String
    val times = 8
    val numData = times
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val g_prueba: Gen[List[List[U]]] = WinGen.until[String](WinGen.ofN[String](1, Gen.const("hola")), WinGen.ofN[String](1, Gen.const("adios")), numData)
    val f1 : Formula[List[U]] = (u : List[U]) => (u contains ("hola"))
    val f2 : Formula[List[U]] = (u : List[U]) => (u contains ("adios"))
    val formula : Formula[List[U]] = (u : List[U]) => f1 until f2 on times
    val result = test[U](g_prueba, formula, env, times, numData)
    print("Until: ")
    result.print
    env.execute()
    result.toString
  }

  def pruebaRelease = {
    type U = String
    val times = 8
    val numData = times
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val g_prueba: Gen[List[List[U]]] = WinGen.release[String](WinGen.ofN[String](1, Gen.const("hola")), WinGen.ofN[String](1, Gen.const("adios")), numData)
    val f1 : Formula[List[U]] = (u : List[U]) => (u contains ("hola"))
    val f2 : Formula[List[U]] = (u : List[U]) => (u contains ("adios"))
    val formula : Formula[List[U]] = (u : List[U]) => f1 release f2 on times
    val result = test[U](g_prueba, formula, env, times, numData)
    print("Release: ")
    result.print
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


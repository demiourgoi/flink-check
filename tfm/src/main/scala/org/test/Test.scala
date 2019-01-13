package org.test


import org.scalacheck.{Gen, Prop}
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.gen.ListStream
import org.gen.WinGen
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.test.streamTest.ForEachStreamElem




//Objeto con todas las funciones test desarrolladas.
//Las que se utilizan en este proyecto son 'test' y 'testAux', y 'testStream' se utiliza en 'DemoTestStream'.
object Test {



  //Evalua la formula fom a la lista generada por gen
  def testList[U](gen: Gen[List[U]], form: Formula[U]): Prop.Status = {
    var currFormula = form.nextFormula
    gen.sample.get.foreach(elem => if (currFormula.result.isEmpty) {
      currFormula = currFormula.consume(Time(1))(elem)
    })
    return currFormula.result.getOrElse(Prop.Undecided)
  }


  //Evalua la formula form al stream recibido
  def testStream[U](stream: AllWindowedStream[Any, GlobalWindow], form: Formula[U], env: StreamExecutionEnvironment): DataStream[Prop.Status] = {
    env.getConfig.disableSysoutLogging()
    var currFormula = form.nextFormula
    val formulaCopy : NextFormula[U] = currFormula
    val resultWindows = stream.aggregate(new ForEachStreamElem[U](formulaCopy))
    resultWindows.filter(_._1).map(_._2)
  }


  //Funcion test que solo permite hacer un test
  def testWithoutLoop[U](gen: Gen[ListStream[U]], form: Formula[List[U]], env: StreamExecutionEnvironment, times: Int): DataStream[Prop.Status] = {
    env.getConfig.disableSysoutLogging()
      var currFormula = form.nextFormula
      val w = WinGen.toWindowsList(gen, env)
      val formulaCopy: NextFormula[List[U]] = currFormula
      val resultWindows = w.aggregate(new ForEachWindow[U](formulaCopy))
      resultWindows.filter(_._1).map(_._2)
  }


  //Funcion test que ejecuta el generador gen tantas veces como se indique en times, y evalua formula a los
  //datos generados cada vez, devolviendo times resultados
  def test[U](gen: Gen[ListStream[U]], form: Formula[List[U]], env: StreamExecutionEnvironment, times: Int): DataStream[String] = {
    env.getConfig.disableSysoutLogging()
      var currFormula = form.nextFormula
      val w = WinGen.toWindowsList(gen, env)
      val formulaCopy: NextFormula[List[U]] = currFormula
      val resultWindows = w.aggregate(new ForEachWindow[U](formulaCopy))
      if(times == 1) {
        val result = resultWindows
        val finalResult = result.filter(_._1).countWindowAll(times).aggregate(new TestFinalResult(times))
        finalResult.filter(_._1).map(_._2)
      }
    else{
        val result = resultWindows.union(testAux(gen, form, env, times - 1))
        val finalResult = result.filter(_._1).countWindowAll(times).aggregate(new TestFinalResult(times))
        finalResult.filter(_._1).map(_._2)
      }
  }

    def testAux[U](gen: Gen[ListStream[U]], form: Formula[List[U]], env: StreamExecutionEnvironment, times: Int): DataStream[(Boolean,Prop.Status)] = {
      env.getConfig.disableSysoutLogging()
      if(times > 1){
        var currFormula = form.nextFormula
        val w = WinGen.toWindowsList(gen, env)
        val formulaCopy: NextFormula[List[U]] = currFormula
        val resultWindows = w.aggregate(new ForEachWindow[U](formulaCopy))
        resultWindows.union(testAux(gen, form, env, times-1))
        }
       else{
        var currFormula = form.nextFormula
        val w = WinGen.toWindowsList(gen, env)
        val formulaCopy: NextFormula[List[U]] = currFormula
        w.aggregate(new ForEachWindow[U](formulaCopy))
        }
      }


}



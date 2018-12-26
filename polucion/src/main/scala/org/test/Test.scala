package org.test


import org.apache.flink.api.java.tuple.Tuple
import org.scalacheck.{Gen, Prop}
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.gen.ListStream
import org.gen.WinGen
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.test.keyedStreamTest.KeyedStreamTest
import org.test.streamTest.ForEachStreamElem


//VERSION BUENA


object Test {



  def testList[U](gen: Gen[List[U]], form: Formula[U]): Prop.Status = {
    var currFormula = form.nextFormula
    gen.sample.get.foreach(elem => if (currFormula.result.isEmpty) {
      currFormula = currFormula.consume(Time(1))(elem)
    })
    return currFormula.result.getOrElse(Prop.Undecided)
  }


  def testStream[U](stream: AllWindowedStream[Any, GlobalWindow], form: Formula[U], env: StreamExecutionEnvironment, times: Int, numData: Int): DataStream[Prop.Status] = {
    env.getConfig.disableSysoutLogging()
    var currFormula = form.nextFormula
    val formulaCopy : NextFormula[U] = currFormula
    val resultWindows = stream.aggregate(new ForEachStreamElem[U](formulaCopy, times, numData))
    //val dataStream: DataStream[Prop.Status] = env.fromCollection(List(Prop.Undecided))
    resultWindows.filter(_._1).map(_._2)
  }


  def testWithoutFor[U](gen: Gen[ListStream[U]], form: Formula[List[U]], env: StreamExecutionEnvironment, times: Int): DataStream[Prop.Status] = {
    env.getConfig.disableSysoutLogging()

      var currFormula = form.nextFormula
      val w = WinGen.toWindowsList(gen, env)
      val formulaCopy: NextFormula[List[U]] = currFormula
      val resultWindows = w.aggregate(new ForEachWindow[U](formulaCopy)) //val dataStream: DataStream[Prop.Status] = env.fromCollection(List(Prop.Undecided))
      //print(resultWindows.filter(_._1).map(_._2).asInstanceOf[Prop.Status])
      resultWindows.filter(_._1).map(_._2)

  }


  def keyedTest[U](stream: KeyedStream[(Any, U), Int], form: Formula[U], env: StreamExecutionEnvironment): DataStream[(Any,Prop.Status)] = {
    env.getConfig.disableSysoutLogging()
    var currFormula = form.nextFormula
    val formulaCopy : NextFormula[U] = currFormula
    val result = stream.flatMap(new KeyedStreamTest[Any,U](currFormula))
    //val dataStream: DataStream[Prop.Status] = env.fromCollection(List(Prop.Undecided))
    result.print()

    env.execute()
    result
  }

  def test[U](gen: Gen[ListStream[U]], form: Formula[List[U]], env: StreamExecutionEnvironment, times: Int): DataStream[String] = {
    env.getConfig.disableSysoutLogging()
    //for(i <- 1 to times) {
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
      //for(i <- 1 to times) {
      if(times > 1){
        var currFormula = form.nextFormula
        val w = WinGen.toWindowsList(gen, env)
        val formulaCopy: NextFormula[List[U]] = currFormula
        val resultWindows = w.aggregate(new ForEachWindow[U](formulaCopy)) //val dataStream: DataStream[Prop.Status] = env.fromCollection(List(Prop.Undecided))
        //print(resultWindows.filter(_._1).map(_._2).asInstanceOf[Prop.Status])
        resultWindows.union(testAux(gen, form, env, times-1))
        }
       else{
        var currFormula = form.nextFormula
        val w = WinGen.toWindowsList(gen, env)
        val formulaCopy: NextFormula[List[U]] = currFormula
        w.aggregate(new ForEachWindow[U](formulaCopy)) //val dataStream: DataStream[Prop.Status] = env.fromCollection(List(Prop.Undecided))
        //print(resultWindows.filter(_._1).map(_._2).asInstanceOf[Prop.Status])

        }
      }


}



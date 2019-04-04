package es.ucm.fdi.sscheck.flink.demo.pollution

import java.util.concurrent.Executors

import org.apache.flink.api.scala._
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.matcher.ResultMatchers

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class ConcurrentActionsTest  extends Specification with ResultMatchers {
  def is =
    s2"""
        - where two sequential calls to collect work ok $doubleCollectSequential
        - where two concurrent calls to collect work
        break ${doubleCollectConcurrent should throwA[NullPointerException]}
      """

  def doubleCollectSequential = {
    val env = ExecutionEnvironment.createLocalEnvironment(3)
    val xs = env.fromCollection(1 to 100).map{_+1}

    println(s"xs.count = ${xs.count}")
    println(s"xs.count = ${xs.count}")
    ok
  }

  def doubleCollectConcurrent = {
    val env = ExecutionEnvironment.createLocalEnvironment(3)
    val xs = env.fromCollection(1 to 100).map{_+1}
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

    val pendingActions = Seq.fill(10)(
      Future { println(s"xs.count = ${xs.count}") }
    )
    val pendingActionsFinished = Future.fold(pendingActions)(Unit){ (u1, u2) =>
      println("pending action finished")
      Unit
    }
    Await.result(pendingActionsFinished, 10 seconds)

    ok
  }
}

/*
Diverse exceptions, non deterministically generated depending on race conditions

java.lang.NullPointerException
	at org.apache.flink.api.java.operators.OperatorTranslation.translate(OperatorTranslation.java:63)
	at org.apache.flink.api.java.operators.OperatorTranslation.translateToPlan(OperatorTranslation.java:52)
	at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:956)
	at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:923)
	at org.apache.flink.api.java.LocalEnvironment.execute(LocalEnvironment.java:85)
	at org.apache.flink.api.java.ExecutionEnvironment.execute(ExecutionEnvironment.java:817)
	at org.apache.flink.api.scala.ExecutionEnvironment.execute(ExecutionEnvironment.scala:525)
	at org.apache.flink.api.scala.DataSet.count(DataSet.scala:719)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply$mcV$sp(ConcurrentActionsTest.scala:38)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply(ConcurrentActionsTest.scala:38)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply(ConcurrentActionsTest.scala:38)

java.lang.NullPointerException
	at org.apache.flink.api.common.JobExecutionResult.getAccumulatorResult(JobExecutionResult.java:90)
	at org.apache.flink.api.scala.DataSet.count(DataSet.scala:720)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply$mcV$sp(ConcurrentActionsTest.scala:38)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply(ConcurrentActionsTest.scala:38)
	at es.ucm.fdi.sscheck.flink.demo.pollution.ConcurrentActionsTest$$anonfun$3$$anonfun$apply$1.apply(ConcurrentActionsTest.scala:38)
* */

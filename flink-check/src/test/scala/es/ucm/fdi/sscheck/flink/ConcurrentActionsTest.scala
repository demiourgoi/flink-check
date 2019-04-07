package es.ucm.fdi.sscheck.flink

import java.util.concurrent.{Executors, ThreadPoolExecutor}

import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import es.ucm.fdi.sscheck.prop.tl.{FormulaParallelism, SequentialFormulaParallelism, TaskSupportFormulaParallelism, Time => SscheckTime}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.runner.RunWith
import org.scalacheck.Prop
import org.slf4j.LoggerFactory
import org.specs2.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.matcher.ResultMatchers

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.{Failure, Try}

@RunWith(classOf[JUnitRunner])
class ConcurrentActionsTest  extends Specification with ResultMatchers {
  def is =
    s2"""
        - where two sequential calls to collect work {doubleCollectSequentialPass should beSuccessful}
        - where two concurrent calls to collect break Flink's OperatorTranslation.translateToPlan,
      and with a race condition {doubleCollectConcurrentFail should beFailedTry}
        - where flink-check matchers triggered from a parallel collection also break Flink's
        OperatorTranslation.translateToPlan, and with a race condition
      {flinkCheckMatchersParCollectionFail should beFailedTry}
       - where flink-check matchers triggered from a sequential collection work
      {flinkCheckMatchersSeqCollectionPass should beSuccessful}
       - where a sscheck formula with high parallelism also break Flink's OperatorTranslation.translateToPlan,
       and with a race condition {sscheckFormulaHighParFail should beFailedTry}
       - where a flink-check formula works {flinkFormulaOkPass should beSuccessful}
      """

  val logger = LoggerFactory.getLogger(getClass)

  def logFailure[A](testName: String)(body: => A): Try[A] =
    Try { body } recoverWith{ case t =>
      logger.warn(s"Expected exception for test ${testName}:", t)
      Failure(t)
    }

  def fixture = new {
    val env = ExecutionEnvironment.createLocalEnvironment(3)
    val xs = env.fromCollection(1 to 100).map{_+1}
  }

  def doubleCollectSequentialPass = {
    val f = fixture

    (1 to 10).foreach{ i =>
      logger.info("{} : xs.count = {}", i, f.xs.count)
    }
    ok
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
  def doubleCollectConcurrentFail = {
    val f = fixture
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

    logFailure("doubleCollectConcurrent"){
      val pendingActions = Seq.fill(10)(
        Future { logger.info(s"xs.count = ${f.xs.count}") }
      )
      val pendingActionsFinished = Future.fold(pendingActions)(Unit){ (u1, u2) =>
        logger.info("pending action finished")
        Unit
      }
      Await.result(pendingActionsFinished, 10 seconds)
    }
  }

  /*
 java.util.ConcurrentModificationException
at org.apache.flink.api.java.operators.OperatorTranslation.translateToPlan(OperatorTranslation.java:51)
at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:956)
at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:923)
at org.apache.flink.api.java.LocalEnvironment.execute(LocalEnvironment.java:85)
at org.apache.flink.api.java.ExecutionEnvironment.execute(ExecutionEnvironment.java:817)
at org.apache.flink.api.scala.ExecutionEnvironment.execute(ExecutionEnvironment.scala:525)
at org.apache.flink.api.scala.DataSet.count(DataSet.scala:719)
at es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers$$anonfun$foreachElementProjection$1.apply(package.scala:17)
at es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers$$anonfun$foreachElementProjection$1.apply(package.scala:14)
at es.ucm.fdi.sscheck.flink.ConcurrentActionsTest$$anonfun$concurrentFlinkMatchers$1.apply(ConcurrentActionsTest.scala:65)
at es.ucm.fdi.sscheck.flink.ConcurrentActionsTest$$anonfun$concurrentFlinkMatchers$1.apply(ConcurrentActionsTest.scala:64)

19/04/06 17:01:39 ERROR ConcurrentActionsTest: concurrentFlinkMatchers
java.lang.NullPointerException
at org.apache.flink.api.java.operators.OperatorTranslation.translate(OperatorTranslation.java:63)
at org.apache.flink.api.java.operators.OperatorTranslation.translateToPlan(OperatorTranslation.java:52)
at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:956)
at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:923)
at org.apache.flink.api.java.LocalEnvironment.execute(LocalEnvironment.java:85)
at org.apache.flink.api.java.ExecutionEnvironment.execute(ExecutionEnvironment.java:817)
at org.apache.flink.api.scala.ExecutionEnvironment.execute(ExecutionEnvironment.scala:525)
at org.apache.flink.api.scala.DataSet.count(DataSet.scala:719)
at es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers$$anonfun$foreachElementProjection$1.apply(package.scala:17)
 * */
  def flinkCheckMatchersParCollectionFail = {
    val f = fixture
    import scala.collection.parallel._
    val counts = (1 to 10).par
    counts.tasksupport =
      new ThreadPoolTaskSupport(Executors.newFixedThreadPool(10).asInstanceOf[ThreadPoolExecutor])

    logFailure("concurrentFlinkMatchers"){
      counts.foreach{_ =>
        f.xs should foreachElement{_ > 0}
      }
    }
  }

  def flinkCheckMatchersSeqCollectionPass = {
    val f = fixture
    val counts = (1 to 10)

    counts.foreach{_ =>
      f.xs should foreachElement{_ > 0}
    }

    ok
  }

  def buildFormula() = {
    import es.ucm.fdi.sscheck.prop.tl.Formula._

    type U = DataSet[Int]
    and(Seq.fill(10){now[U]{_ should foreachElement{_ > 0}}}:_*)
  }

  /*
  *
19/04/06 18:03:10 ERROR ConcurrentActionsTest: sscheckFormulaHighParFail
java.lang.NullPointerException
	at org.apache.flink.api.java.operators.OperatorTranslation.translate(OperatorTranslation.java:63)
	at org.apache.flink.api.java.operators.OperatorTranslation.translateToPlan(OperatorTranslation.java:52)
	at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:956)
	at org.apache.flink.api.java.ExecutionEnvironment.createProgramPlan(ExecutionEnvironment.java:923)
	at org.apache.flink.api.java.LocalEnvironment.execute(LocalEnvironment.java:85)
	at org.apache.flink.api.java.ExecutionEnvironment.execute(ExecutionEnvironment.java:817)
	at org.apache.flink.api.scala.ExecutionEnvironment.execute(ExecutionEnvironment.scala:525)
	at org.apache.flink.api.scala.DataSet.count(DataSet.scala:719)
	at es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers$$anonfun$foreachElementProjection$1.apply(package.scala:17)
	at es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers$$anonfun$foreachElementProjection$1.apply(package.scala:14)
	at org.specs2.matcher.MatchersCreation$$anon$4.apply(MatchersImplicits.scala:211)
	at org.specs2.matcher.Expectable.applyMatcher(Expectable.scala:50)
	at org.specs2.matcher.ShouldExpectations$$anon$2.applyMatcher(ShouldExpectations.scala:24)
	at org.specs2.matcher.ShouldExpectable.should(ShouldExpectable.scala:14)
	at es.ucm.fdi.sscheck.flink.ConcurrentActionsTest$$anonfun$4$$anonfun$apply$17.apply(ConcurrentActionsTest.scala:159)
	at es.ucm.fdi.sscheck.flink.ConcurrentActionsTest$$anonfun$4$$anonfun$apply$17.apply(ConcurrentActionsTest.scala:159)
	at scala.Function1$$anonfun$andThen$1.apply(Function1.scala:52)
	at scala.Function1$$anonfun$andThen$1.apply(Function1.scala:52)
	at es.ucm.fdi.sscheck.prop.tl.BindNext.consume(Formula.scala:391)
	at es.ucm.fdi.sscheck.prop.tl.NextBinaryOp$$anonfun$1.apply(Formula.scala:540)
	at es.ucm.fdi.sscheck.prop.tl.NextBinaryOp$$anonfun$1.apply(Formula.scala:540)
	at scala.collection.parallel.AugmentedIterableIterator$class.map2combiner(RemainsIterator.scala:115)
	at scala.collection.parallel.immutable.ParVector$ParVectorIterator.map2combiner(ParVector.scala:62)
	at scala.collection.parallel.ParIterableLike$Map.leaf(ParIterableLike.scala:1054)
	at scala.collection.parallel.Task$$anonfun$tryLeaf$1.apply$mcV$sp(Tasks.scala:49)
	at scala.collection.parallel.Task$$anonfun$tryLeaf$1.apply(Tasks.scala:48)
	at scala.collection.parallel.Task$$anonfun$tryLeaf$1.apply(Tasks.scala:48)
	at scala.collection.parallel.Task$class.tryLeaf(Tasks.scala:51)
	at scala.collection.parallel.ParIterableLike$Map.tryLeaf(ParIterableLike.scala:1051)
	at scala.collection.parallel.AdaptiveWorkStealingTasks$WrappedTask$class.internal(Tasks.scala:159)
	at scala.collection.parallel.AdaptiveWorkStealingForkJoinTasks$WrappedTask.internal(Tasks.scala:443)
	at scala.collection.parallel.AdaptiveWorkStealingTasks$WrappedTask$class.compute(Tasks.scala:149)
	at scala.collection.parallel.AdaptiveWorkStealingForkJoinTasks$WrappedTask.compute(Tasks.scala:443)
	at scala.concurrent.forkjoin.RecursiveAction.exec(RecursiveAction.java:160)
	at scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)
	at scala.concurrent.forkjoin.ForkJoinTask.doJoin(ForkJoinTask.java:341)
	at scala.concurrent.forkjoin.ForkJoinTask.join(ForkJoinTask.java:673)
	at scala.collection.parallel.ForkJoinTasks$WrappedTask$class.sync(Tasks.scala:378)
	at scala.collection.parallel.AdaptiveWorkStealingForkJoinTasks$WrappedTask.sync(Tasks.scala:443)
	at scala.collection.parallel.ForkJoinTasks$class.executeAndWaitResult(Tasks.scala:426)
	at scala.collection.parallel.ForkJoinTaskSupport.executeAndWaitResult(TaskSupport.scala:56)
	at scala.collection.parallel.ExecutionContextTasks$class.executeAndWaitResult(Tasks.scala:558)
	at scala.collection.parallel.ExecutionContextTaskSupport.executeAndWaitResult(TaskSupport.scala:80)
	at scala.collection.parallel.ParIterableLike$ResultMapping.leaf(ParIterableLike.scala:958)
	at scala.collection.parallel.Task$$anonfun$tryLeaf$1.apply$mcV$sp(Tasks.scala:49)
	at scala.collection.parallel.Task$$anonfun$tryLeaf$1.apply(Tasks.scala:48)
	at scala.collection.parallel.Task$$anonfun$tryLeaf$1.apply(Tasks.scala:48)
	at scala.collection.parallel.Task$class.tryLeaf(Tasks.scala:51)
	at scala.collection.parallel.ParIterableLike$ResultMapping.tryLeaf(ParIterableLike.scala:953)
	at scala.collection.parallel.AdaptiveWorkStealingTasks$WrappedTask$class.compute(Tasks.scala:152)
	at scala.collection.parallel.AdaptiveWorkStealingForkJoinTasks$WrappedTask.compute(Tasks.scala:443)
	at scala.concurrent.forkjoin.RecursiveAction.exec(RecursiveAction.java:160)
	at scala.concurrent.forkjoin.ForkJoinTask.doExec(ForkJoinTask.java:260)
	at scala.concurrent.forkjoin.ForkJoinPool$WorkQueue.runTask(ForkJoinPool.java:1339)
	at scala.concurrent.forkjoin.ForkJoinPool.runWorker(ForkJoinPool.java:1979)
	at scala.concurrent.forkjoin.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:107)
  * */
  def sscheckFormulaHighParFail = {
    import scala.collection.parallel._

    val f = fixture
    implicit val formulaParallelism = TaskSupportFormulaParallelism(
      new ThreadPoolTaskSupport(Executors.newFixedThreadPool(10).asInstanceOf[ThreadPoolExecutor]))
    logFailure("sscheckFormulaHighParFail"){
      val formula = buildFormula()
      formula.nextFormula.consume(SscheckTime(0))(f.xs)
    }
  }

  def flinkFormulaOkPass = {
    import es.ucm.fdi.sscheck.prop.tl.flink.FlinkFormula._

    val f = fixture
    val formula = buildFormula() groupBy TumblingTimeWindows(Time.milliseconds(10))

    val result = formula.nextFormula.consume(SscheckTime(0))(f.xs).result
    result === Some(Prop.True)
  }
}


package es.ucm.fdi.sscheck.matcher.specs2.flink

import scala.language.reflectiveCalls

import org.apache.flink.api.scala._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification
import DataSetMatchers._
import org.specs2.matcher.ResultMatchers

@RunWith(classOf[JUnitRunner])
class FlinkMatchersSpec
  extends Specification with ResultMatchers {

  sequential

  val fixture = new {
    val env = ExecutionEnvironment.createLocalEnvironment(3)
    val emptyDataSet = env.fromCollection[Int](Nil)
    val nonEmptyDataSet = env.fromElements(0)
  }

  "Flink matchers spec" >> {
    "where an existential matcher for a tautology on an empty data set fails" >> {
      val f = fixture
      (f.emptyDataSet should existsElement(Function.const(true))) should beFailing
    }
    "where an existential matcher for a contradiction on an empty data set fails" >> {
      val f = fixture
      (f.emptyDataSet should existsElement(Function.const(false))) should beFailing
    }
    "where an existential matcher for a tautology on a non-empty data set " +
      "succeeds" >> {
      val f = fixture
      f.nonEmptyDataSet should existsElement(Function.const(true))
    }
    "where an existential matcher for a contradiction on a non-empty data set " +
      "fails" >> {
      val f = fixture
      (f.nonEmptyDataSet should existsElement(Function.const(false))) should beFailing
    }
    "where a universal matcher for a tautology on an empty data set succeeds" >> {
      val f = fixture
      f.emptyDataSet should foreachElement(Function.const(true))
    }
    "where a universal matcher for a contradiction on an empty data set succeeds" >> {
      val f = fixture
      f.emptyDataSet should foreachElement(Function.const(false))
    }
    "where a universal matcher for a tautology on a non-empty data set " +
      "succeeds" >> {
      val f = fixture
      f.nonEmptyDataSet should foreachElement(Function.const(true))
    }
    "where a universal matcher for a contradiction on a non-empty data set " +
      "fails" >> {
      val f = fixture
      (f.nonEmptyDataSet should foreachElement(Function.const(false))) should beFailing
    }
    "where an empty data set should match the empty data set matcher" >> {
      val f = fixture
      f.emptyDataSet should beEmptyDataSet()
    }
    "where a non-empty data set should not match the empty data set matcher" >> {
      val f = fixture
      (f.nonEmptyDataSet should beEmptyDataSet()) should beFailing
    }
    "where the minus data set operator works as expected">> {
      val f = fixture
      val xs = f.env.fromCollection(1 to 10)
      val ys = f.env.fromCollection(5 to 15)
      xs.minus(ys).collect() must containTheSameElementsAs(1 to 4)
    }
    "where the beSubDataSetOf data set matcher works as expected">> {
      val f = fixture
      val xs = f.env.fromCollection(1 to 10)
      val ys = f.env.fromCollection(5 to 15)
      val zs = f.env.fromCollection(1 to 5)
      zs should beSubDataSetOf(xs)
      xs should beSubDataSetOf(xs)
      f.emptyDataSet should beSubDataSetOf(xs)
      (xs should beSubDataSetOf(f.emptyDataSet)) should beFailing
      (xs should beSubDataSetOf(ys)) should beFailing
      (ys should beSubDataSetOf(xs)) should beFailing
    }
  }
}
package org.test.streamTest

import org.apache.flink.api.common.functions.AggregateFunction
import org.scalacheck.Prop
import org.test.{NextFormula, Time, WindowResult}


//Evalua la formula para cada elemento del stream
class ForEachStreamElem[U](formula: NextFormula[U]) extends AggregateFunction[Any, WindowResult, (Boolean, Prop.Status)] {

  var f = formula
  var done = false


  def createAccumulator(): WindowResult = {
    new WindowResult((false, Prop.Undecided))
  }


  def merge(a: WindowResult, b: WindowResult): WindowResult = {
    var okay = a.result._1 && b.result._1
    var res: Prop.Status = Prop.Undecided
    if (a.result._2 == Prop.True && b.result._2 == Prop.True) res = Prop.True else if (a.result._2 == Prop.False || b.result._2 == Prop.False) res = Prop.False
    new WindowResult((okay, res))
  }


  def add(data: Any, wr: WindowResult) = {
    println(data)
    if (f.result.isEmpty) {
      f = f.consume(Time(1))(data.asInstanceOf[U])
    }


  }

  def getResult(wr: WindowResult): (Boolean, Prop.Status) = {

    val resul = !f.result.isEmpty && !done
    if (!f.result.isEmpty) done = true
    (resul, f.result.getOrElse(Prop.Undecided))


  }

}
package org.test

import org.apache.flink.api.common.functions.AggregateFunction
import org.scalacheck.Prop

class WindowResult( r: (Boolean, Prop.Status)){
  var result = r
}


class ForEachWindow[U](formula: NextFormula[List[U]]) extends AggregateFunction[List[Any], WindowResult, (Boolean, Prop.Status)] {

  var f = formula
  var cont = 0
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


  def add(data: List[Any], wr: WindowResult) = {
    //println("add")
    /*data.asInstanceOf[List[U]].foreach(elem => if (wr.formula.result.isEmpty) {
      println("Adding " + elem)
      wr.formula = wr.formula.consume(Time(1))(elem)
      println(wr.formula)
    })*/
    //if(cont < times) {
    if (f.result.isEmpty && data!=List(null)) {
      f = f.consume(Time(1))(data.asInstanceOf[List[U]])
    } //}
    if(data == List(null)) done = true
    cont += 1
  }

  def getResult(wr: WindowResult): (Boolean, Prop.Status) = {
    //println(cont + " - " + f.result.getOrElse(Prop.Undecided) )
    /*val resul = !f.result.isEmpty && !done
    if (!f.result.isEmpty) done = true
    (resul, f.result.getOrElse(Prop.Undecided))*/
    if(done) (true, f.result.getOrElse(Prop.Undecided))
    else (false, f.result.getOrElse(Prop.Undecided))
  }
}

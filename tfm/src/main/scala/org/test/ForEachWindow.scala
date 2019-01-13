package org.test

import org.apache.flink.api.common.functions.AggregateFunction
import org.scalacheck.Prop

class WindowResult( r: (Boolean, Prop.Status)){
  var result = r
}

//Evalua la formula 'formula' con cada elemento de cada ventana que va llegando, y devuelve el resultado de cada evaluacion
class ForEachWindow[U](formula: NextFormula[List[U]]) extends AggregateFunction[List[Any], WindowResult, (Boolean, Prop.Status)] {

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


  def add(data: List[Any], wr: WindowResult) = {
    if (f.result.isEmpty && data!=List(null)) {
      f = f.consume(Time(1))(data.asInstanceOf[List[U]])
    }
    if(data == List(null)) done = true
  }

  def getResult(wr: WindowResult): (Boolean, Prop.Status) = {
     if(done) {
      (true, f.result.getOrElse(Prop.Undecided))
    }
    else (false, f.result.getOrElse(Prop.Undecided))
  }
}

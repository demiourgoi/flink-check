package org.test

import org.apache.flink.api.common.functions.AggregateFunction
import org.scalacheck.Prop


//Contador que guarda el numero de aciertos, fallos y pruebas sin decidir que se van recibiendo
class ResultAccum(){
  var (passed, failed, undecided) = (0,0,0)
}


//Cuenta el numero de aciertos, fallos y pruebas sin decidir obtenidas tras el test
class TestFinalResult (times: Int)extends AggregateFunction[(Boolean,Prop.Status), ResultAccum, (Boolean, String)] {

  var cont = 0

  def createAccumulator(): ResultAccum = {
    new ResultAccum()
  }


  def merge(a: ResultAccum, b: ResultAccum): ResultAccum = {
    b
  }


  def add(data: (Boolean, Prop.Status), ra: ResultAccum) = {

    if(data._1){
      if(data._2 == Prop.True) ra.passed +=1
      else if(data._2 == Prop.False) ra.failed +=1
      else if(data._2 == Prop.Undecided) ra.undecided +=1
    }
    cont += 1

  }

  def getResult(ra: ResultAccum): (Boolean, String) = {
    if(cont < times) (false, (ra.passed + " passed, " + ra.failed + " failed, " + ra.undecided + " undecided."))
    else (true, "Test executed: " + ra.passed + " passed, " + ra.failed + " failed, " + ra.undecided + " undecided.")
  }
}



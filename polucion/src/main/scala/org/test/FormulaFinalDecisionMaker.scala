package org.test

import org.apache.flink.api.common.functions.AggregateFunction
import org.scalacheck.Prop


//SIN USO
//La idea de esta funcion era selecionar el ultimo resultado del stream devuelto por la funcion test,
//ya que el ultimo resultado del stream es el resultado final del test.
//Sin embargo, no funciona porque es necesario tener todos los resultados en una misma ventana
//y no es posible saber cuantos resultados se tienen cada vez para poder crear dicha ventana.
class FormulaStreamFinalDecisionMaker () extends AggregateFunction[(Boolean,Prop.Status), ResultAccumulator, Prop.Status] {


  def createAccumulator(): ResultAccumulator = {
    new ResultAccumulator()
  }


  def merge(a: ResultAccumulator, b: ResultAccumulator): ResultAccumulator = {
    b
  }


  def add(data: (Boolean, Prop.Status), wr: ResultAccumulator) = {

    //Si ya es true no hacemos nada porque es el final
    if(!wr.finalProp._1){
      if(data._1){
        wr.finalProp = data
      }
      else{
        wr.finalProp = data
      }
    }

  }

  def getResult(wr: ResultAccumulator): Prop.Status = {
    wr.finalProp._2
  }
}

class ResultAccumulator(){
  var finalProp : (Boolean, Prop.Status) = (false, Prop.Undecided)
}
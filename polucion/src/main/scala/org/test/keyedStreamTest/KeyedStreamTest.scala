package org.test.keyedStreamTest

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.scalacheck.Prop
import org.specs2.{ScalaCheck, Specification}
import org.test.Formula.always
import org.test.{NextFormula, Time}


//Clase que aplica una formula a un KeyedStream
class KeyedStreamTest[Key, U](formula : NextFormula[U]) extends RichFlatMapFunction[(Key,U), (Key,Prop.Status)]{

  private var form: ValueState[NextFormula[U]] = _

  override def flatMap(input: (Key,U), out: Collector[(Key,Prop.Status)]): Unit = {

    //Acceder al ValueState
    val tmpCurrForm = form.value

    //Si no se ha utilizado antes, sera null
    val currentForm = if (tmpCurrForm != null) {
      tmpCurrForm
    } else {
      formula
    }

    val newForm = currentForm.consume(Time(1))(input._2)

    //Actualiza el estado
    form.update(newForm)

    //Si la formula esta resuelta
    if (!newForm.result.getOrElse(Prop.Undecided).equals(Prop.Undecided)) {
      out.collect(input._1, newForm.result.getOrElse(Prop.Undecided))
      form.clear()
    }
  }

  override def open(parameters: Configuration): Unit = {
    form = getRuntimeContext.getState(
      new ValueStateDescriptor[NextFormula[U]]("formula", TypeExtractor.getForClass(classOf[NextFormula[U]]))
    )
  }


}
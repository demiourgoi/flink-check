package org.test


import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.scalacheck.Prop
import org.specs2.{ScalaCheck, Specification}
import org.test.Formula.always


class FormulaWindow[Key, U](formula : NextFormula[U]) extends RichFlatMapFunction[(Key,U), (Key,Prop.Status)]{

  private var form: ValueState[NextFormula[U]] = _

  override def flatMap(input: (Key,U), out: Collector[(Key,Prop.Status)]): Unit = {

    // access the state value
    val tmpCurrForm = form.value

    // If it hasn't been used before, it will be null
    val currentForm = if (tmpCurrForm != null) {
      tmpCurrForm
    } else {
      formula
    }

    // update the count
    val newForm = currentForm.consume(Time(1))(input._2)
    println("valor: " + input)
    println("formula: " + newForm.result.getOrElse(Prop.Undecided))

    // update the state
    form.update(newForm)

    // if the formula is solved
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
package org.test.formulaTrigger

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.specs2.matcher.ResultMatchers
import org.specs2.{ScalaCheck, Specification}
import org.test.Formula
import org.test.Formula.always
import org.test.Formula._


class DemoFormulaTrigger extends Specification
  with ScalaCheck
  with ResultMatchers
  with Serializable {

  def is =
    sequential ^ s2"""
    Simple demo Specs2 for a formula
    - list test  ${testList}
    - Trump Twitter test ${TwitterTrumpTest}
    """



  def testList : String = {
    //Establece el entorno de ejecucion
    type U = String
    val formula : Formula[U] = always { (u : U) =>
      u contains "hola"
    } during 3

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()

    val input = env.fromCollection(List("hola", "hola", "hola", "hola", "hola", "hola", "hola", "adios", "hola", "hola", "hola", "adios", "hola"))

    println("Prueba1")

    val windows = input.windowAll(GlobalWindows.create().asInstanceOf[WindowAssigner[Any, GlobalWindow]])
      .trigger(new FormulaTrigger[U, GlobalWindow](formula.nextFormula))

    val acc = windows.reduce((acc, value )=> acc+ ", " + value)
    acc.print()
    env.execute()

    return acc.toString
  }

  def TwitterTrumpTest : String = {
    //Establece el entorno de ejecucion
    type U = String
    val starting : Formula[U] = { (u: U) => ( u startsWith("..."))}
    val ending : Formula[U] = { (u: U) => ( u endsWith("..."))}
    val strictBegin : Formula[U] = ending and !starting
    val strictEnd : Formula[U] = starting and !ending
    val middleMessage : Formula[U] = starting and ending
    val form : Formula[U] = always { (u : U) =>
      middleMessage
    } during 4
    val findEnd : Formula[U] = form until { (u : U) =>
      strictEnd
    } during 4
    val formula : Formula[U] = strictBegin ==> findEnd

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.disableSysoutLogging()

    val input = env.fromCollection(List("Looking forward to being with the wonderful Bush family at Blair House today. The former First Lady will be coming over to the White House this morning to be given a tour of the Christmas decorations by Melania. The elegance & precision of the last two days have been remarkable!",
      "The negotiations with China have already started. Unless extended, they will end 90 days from the date of our wonderful and very warm dinner with President Xi in Argentina. Bob Lighthizer will be working closely with Steve Mnuchin, Larry Kudlow, Wilbur Ross and Peter Navarro.....",
      "......on seeing whether or not a REAL deal with China is actually possible. If it is, we will get it done. China is supposed to start buying Agricultural product and more immediately. President Xi and I want this deal to happen, and it probably will. But if not remember,......",
      "....I am a Tariff Man. When people or countries come in to raid the great wealth of our Nation, I want them to pay for the privilege of doing so. It will always be the best way to max out our economic power. We are right now taking in $billions in Tariffs. MAKE AMERICA RICH AGAIN",
      ".....But if a fair deal is able to be made with China, one that does all of the many things we know must be finally done, I will happily sign. Let the negotiations begin. MAKE AMERICA GREAT AGAIN!",
      "Could somebody please explain to the Democrats (we need their votes) that our Country losses 250 Billion Dollars a year on illegal immigration, not including the terrible drug flow. Top Border Security, including a Wall, is $25 Billion. Pays for itself in two months. Get it done!",
      "I am glad that my friend @EmmanuelMacron and the protestors in Paris have agreed with the conclusion I reached two years ago. The Paris Agreement is fatally flawed because it raises the price of energy for responsible countries while whitewashing some of the worst polluters....",
      "....in the world. I want clean air and clean water and have been making great strides in improving America’s environment. But American taxpayers – and American workers – shouldn’t pay to clean up others countries’ pollution.",
      "We are either going to have a REAL DEAL with China, or no deal at all - at which point we will be charging major Tariffs against Chinese product being shipped into the United States. Ultimately, I believe, we will be making a deal - either now or into the future....",
      ".....China does not want Tariffs!",
      "“China officially echoed President Donald Trump’s optimism over bilateral trade talks. Chinese officials have begun preparing to restart imports of U.S. Soybeans & Liquified Natural Gas, the first sign confirming the claims of President Donald Trump and the White House that......",
      ".....China had agreed to start “immediately” buying U.S. products.” @business",
      "Very strong signals being sent by China once they returned home from their long trip, including stops, from Argentina. Not to sound naive or anything, but I believe President Xi meant every word of what he said at our long and hopefully historic meeting. ALL subjects discussed!",
      "One of the very exciting things to come out of my meeting with President Xi of China is his promise to me to criminalize the sale of deadly Fentanyl coming into the United States. It will now be considered a “controlled substance.” This could be a game changer on what is.......",
      ".....considered to be the worst and most dangerous, addictive and deadly substance of them all. Last year over 77,000 people died from Fentanyl. If China cracks down on this “horror drug,” using the Death Penalty for distributors and pushers, the results will be incredible!",
      "Looking forward to being with the Bush family. This is not a funeral, this is a day of celebration for a great man who has led a long and distinguished life. He will be missed!",
      "Hopefully OPEC will be keeping oil flows as is, not restricted. The World does not want to see, or need, higher oil prices!"))


    println("Prueba Twitter Trump")

    val windows = input.windowAll(GlobalWindows.create().asInstanceOf[WindowAssigner[Any, GlobalWindow]])
      .trigger(new FormulaTrigger[U, GlobalWindow](formula.nextFormula))

    val acc = windows.reduce((acc, value )=> acc.replaceAll("\\.{2,}", "") + " " + value.replaceAll("\\.{2,}", ""))
    acc.print()
    env.execute()

    return acc.toString
  }
}
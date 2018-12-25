package org

import org.test.formulaTrigger.DemoFormulaTrigger
import org.test.keyedStreamTest.DemoKeyedStreamTest

object Main extends App{

  if(args.length==0){
    println("The program needs an argument, type \"--help\" to get a list of the possible arguments")
  }
  else {
    args(0) match {
      case "--help" => println(getHelp)
      case "-h" => println(getHelp)
      case "-t" => executeTest(args)
      case "--test" => executeTest(args)
      case _ => println(getHelp)
    }
  }



  def getHelp : String = "help"

  def executeTest(args: Array[String]) = {
    if(args.length < 2){
      println("The program needs a test to execute, type \"--help\" to get a list of the possible arguments")
    }
    else{
      args(1) match {
        case "KeyedStreamTest" => specs2.runner.ClassRunner.run(Array(new DemoKeyedStreamTest().getClass.getName))
        case "FormulaTrigger" => specs2.runner.ClassRunner.run(Array(new DemoFormulaTrigger().getClass.getName))
        case _ => println(getHelp)
      }
    }
  }

}
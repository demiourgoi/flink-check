package org

import org.test.formulaTrigger.DemoFormulaTrigger
import org.test.keyedStreamTest.DemoKeyedStreamTest
import org.demo.pollution.DemoPollution
import org.demo.race.DemoRace
import examples.pollution.Pollution
import examples.pollution.PollutionGenerator
import org.test.streamTest.DemoTestStream

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




  def getHelp : String =
    """
      | The possible arguments are:
      | -h | --help to get the help
      | -t | --t <demo> to execute a demo, where <demo> can be:
      |  Pollution : executes the random testing pollution tests
      |  Race : executes the random testing race tests
      |  KeyedStreamTest : runs the test for thee KeyedStream
      |  FormulaTrigger : runs the twitter tests for FormulaTrigger
      |  PollutionFlinkApp : runs the old pollutions alarms tests
      |  StreamTest : applies the test function to the old pollution alarms tests
    """.stripMargin

  def executeTest(args: Array[String]) = {
    if(args.length < 2){
      println("The program needs a test to execute, type \"--help\" to get a list of the possible arguments")
    }
    else{
      args(1) match {
        case "Pollution" => specs2.runner.ClassRunner.run(Array(new DemoPollution().getClass.getName))
        case "Race" => specs2.runner.ClassRunner.run(Array(new DemoRace().getClass.getName))
        case "KeyedStreamTest" => specs2.runner.ClassRunner.run(Array(new DemoKeyedStreamTest().getClass.getName))
        case "FormulaTrigger" => specs2.runner.ClassRunner.run(Array(new DemoFormulaTrigger().getClass.getName))
        case "PollutionFlinkApp" => {
          val thread = new Thread {
            override def run() {
              PollutionGenerator.main(null)
            }
          }
          val thread2 = new Thread {
            override def run() {
              Pollution.main(null)
            }
          }
          thread2.start()
          thread.start()
        }
        case "StreamTest" => {
          val thread = new Thread {
            override def run() {
              PollutionGenerator.main(null)
            }
          }
          val thread2 = new Thread {
            override def run() {
              specs2.runner.ClassRunner.run(Array(new DemoTestStream().getClass.getName))
            }
          }
          thread2.start()
          thread.start()
        }
        case _ => println(getHelp)
      }
    }
  }

}
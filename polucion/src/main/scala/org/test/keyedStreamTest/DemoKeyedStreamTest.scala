package org.test.keyedStreamTest

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalacheck.Prop
import org.specs2.matcher.ResultMatchers
import org.specs2.{ScalaCheck, Specification}
import org.test.Formula
import org.test.Formula._
import org.test.Formula.{always, later}



class DemoKeyedStreamTest extends Specification
  with ScalaCheck
  with ResultMatchers
  with Serializable {

  def is =
    sequential ^ s2"""
    Simple demo Specs2 for a formula
      - where a simple formula must hold on a list ${simpleTest}
      - where Tom is okay but Ana isn't ${patientTest1}
      - where Tom and Ana are fine ${patientTest2}
      - where Tom isn't healthy, and Ana's state is still unknown ${patientTest3}
    """


  def simpleTest : String = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    type U = String
    val formula : Formula[U] = always { (u : U) =>
      u contains "hola"
    } during 3


    println("Simple test")
    env.getConfig.disableSysoutLogging()
    val res = env.fromCollection(List(
      (1, "hola"),
      (2, "hola"),
      (1, "hola"),
      (1, "hola"),
      (1, "hola"),
      (1, "hola"),
      (1, "hola"),
      (1, "hola"),
      (2, "hola"),
      (2, "adios")
    )).keyBy(_._1)
      .flatMap(new KeyedStreamTest[Int,U](formula.nextFormula))

    res.print()

    env.execute()
    return res.toString
  }

  def patientTest1 : String = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    type U = Int
    val normal : Formula[U] = { (u : U) => (3 <= u ) and (u <= 7)}
    val high : Formula[U] = { (u : U) => u > 7}
    val low : Formula[U] = { (u : U) => u < 3 }

    val eventuallyHigh : Formula[U] = later { (u : U) => high } during 6
    val eventuallyLow : Formula[U] = later { (u : U) => low } during 6
    val alwaysNormal : Formula[U] = always{ (u : U) => normal} during 12

    val formula : Formula[U] = {
      alwaysNormal or ( !alwaysNormal ==> ( high and eventuallyLow) or ( low and eventuallyHigh))
    }


    println("Patient test 1")
    env.getConfig.disableSysoutLogging()
    val res = env.fromCollection(List(
      ("Tom", 4),
      ("Tom", 8),
      ("Ana", 2),
      ("Tom", 2),
      ("Ana", 2),
      ("Ana", 2),
      ("Ana", 2),
      ("Ana", 2),
      ("Ana", 2),
      ("Ana", 2),
      ("Ana", 2),
      ("Ana", 9)
    )).keyBy(_._1)
      .flatMap(new KeyedStreamTest[String,U](formula.nextFormula))

    res.print()

    env.execute()
    return res.toString
  }

  def patientTest2 : String = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    type U = Int
    val normal : Formula[U] = { (u : U) => (3 <= u ) and (u <= 7)}
    val high : Formula[U] = { (u : U) => u > 7}
    val low : Formula[U] = { (u : U) => u < 3 }

    val eventuallyHigh : Formula[U] = later { (u : U) => high } during 6
    val eventuallyLow : Formula[U] = later { (u : U) => low } during 6
    val alwaysNormal : Formula[U] = always{ (u : U) => normal} during 12

    val formula : Formula[U] = {
      alwaysNormal or( !alwaysNormal ==> ( high and eventuallyLow) or ( low and eventuallyHigh))
    }

    println("Patient test 2")
    env.getConfig.disableSysoutLogging()
    val res = env.fromCollection(List(
      ("Tom", 4),
      ("Tom", 8),
      ("Ana", 2),
      ("Tom", 4),
      ("Tom", 5),
      ("Tom", 6),
      ("Tom", 5),
      ("Tom", 4),
      ("Tom", 4),
      ("Tom", 4),
      ("Tom", 6),
      ("Tom", 6),
      ("Tom", 6),
      ("Tom", 6),
      ("Tom", 6),
      ("Tom", 6),
      ("Ana", 2),
      ("Ana", 9)
    )).keyBy(_._1)
      .flatMap(new KeyedStreamTest[String,U](formula.nextFormula))

    res.print()

    env.execute()
    return res.toString
  }

  def patientTest3 : String = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    type U = Int
    val normal : Formula[U] = { (u : U) => (3 <= u ) and (u <= 7)}
    val high : Formula[U] = { (u : U) => u > 7}
    val low : Formula[U] = { (u : U) => u < 3 }

    val eventuallyHigh : Formula[U] = later { (u : U) => high } during 6
    val eventuallyLow : Formula[U] = later { (u : U) => low } during 6
    val alwaysNormal : Formula[U] = always{ (u : U) => normal} during 12


    val formula : Formula[U] = {
      alwaysNormal or( !alwaysNormal ==> (high and eventuallyLow) or (low and eventuallyHigh))
    }





   /*val formula : Formula[U] = { (u: U) =>
      (always{ u : U => (3 <= u ) and (u <= 7) } during 12) or  (((_ : U) => u > 7) and (later { (u : U) => u < 3 } during 6)) or (((_ : U) => u < 3) and (later { (u : U) => u > 7 } during 6))
    }*/

    println("Patient test 3")
    env.getConfig.disableSysoutLogging()
    val res = env.fromCollection(List(
      ("Tom", 4),
      ("Tom", 4),
      ("Ana", 2),
      ("Tom", 4),
      ("Tom", 4),
      ("Tom", 4),
      ("Tom", 4),
      ("Tom", 4),
      ("Tom", 4),
      ("Tom", 4),
      ("Tom", 4),
      ("Tom", 4),
      ("Tom", 4),
      ("Tom", 4),
      ("Ana", 5),
      ("Ana", 5),
      ("Ana", 5)
    )).keyBy(_._1)
      .flatMap(new KeyedStreamTest[String,U](formula.nextFormula))

    res.print()

    env.execute()
    return res.toString
  }





}




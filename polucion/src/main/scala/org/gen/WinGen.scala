package org.gen


import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.{PurgingTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

import org.gen.ListStreamConversions._


object WinGen{



  //Devuelve un generador de List con elementos de tipo T generados por gen
  def of[T](gen : => Gen[T]) : Gen[List[T]] = {
    Gen.listOf(gen)
  }

  //Devuelve un generador de List con n elementos de tipo T generados por gen
  def ofN[T](n : Int, gen : Gen[T]) : Gen[List[T]] = {
    Gen.listOfN(n, gen)
  }

  //Devuelve un generador de List con x (n <= x <= m) elementos de tipo T generados por g
  def ofNtoM[T](n : Int, m : Int, gen : Gen[T]) : Gen[List[T]] = {
    for {
      i <- Gen.choose(n, m)
      xs <- Gen.listOfN(i, gen)
    } yield xs
  }

  //devuelve una lista con listas generadas por lg
  def ofNList[T](lg : Gen[List[T]]) : Gen[ListStream[T]] = {
    Gen.listOfN(1,lg)
  }

  //Devuelve una lista de listas con n listas vacias seguidas de lg
  def laterN[A](n : Int, lg : Gen[List[A]]) : Gen[ListStream[A]] = {
    for {
      list <- lg
      blanks = List.fill(n)(List.empty : List[A])
    } yield blanks:::list::Nil
  }





  //Concatena dos generadores de listas en un generador de ListStream
  def concatToListStream[A](lg1 : Gen[List[A]], lg2 : Gen[List[A]]) : Gen[ListStream[A]] = {
    for {
      tail <- lg1
      head <- lg2
    } yield List.fill(1)(head):::tail::Nil

  }

  //Concatena dos generadores de listas en uno
  def concat[A](lg1 : Gen[List[A]], lg2 : Gen[List[A]]) : Gen[List[A]] = {
    for {
      tail <- lg1
      head <- lg2
    } yield head ++ tail
  }

  //Union de dos generadores en uno
  def union[A](gs1 : Gen[ListStream[A]], gs2 : Gen[ListStream[A]]) : Gen[ListStream[A]] = {
    for {
      xs1 <- gs1
      xs2 <- gs2
    } yield zipListStream(xs1,xs2)
  }

  def zipListStream[A](list : ListStream[A], other : ListStream[A]) : ListStream[A] = {
    new ListStream[A](list.toList.zipAll(other.toList, List.empty, List.empty).map(xs12 => xs12._1 ++ xs12._2))
  }





  /**GENERADORES**/

  def until[A](lg1 : Gen[List[A]], lg2 : Gen[List[A]], time: Int) : Gen[ListStream[A]] =
    if (time <= 0)
      Gen.const(List.empty)
    else if (time == 1)
      ofNList(lg2)
    else
      for {
        proofOffset <- Gen.choose(0, time - 1)
        prefix <- always(lg1, proofOffset)
        dsg2Proof <- laterN(proofOffset, lg2)
      } yield prefix.toList ++ dsg2Proof.toList.filter(e => e != List.empty)



  def eventually[A](lg : Gen[List[A]], time: Int) : Gen[ListStream[A]] = {
    if (time <= 0) Gen.const(List.empty)
    else {
      val i = Gen.choose(0, time - 1).sample.get
      laterN(i, lg)
    }
  }


  def always[A](lg : Gen[List[A]], time: Int) : Gen[ListStream[A]] =
    if (time <= 0)
      Gen.const(List.empty)
    else
      Gen.listOfN(time, lg)



  def release[A](lg1 : Gen[List[A]], lg2 : Gen[List[A]], time: Int) : Gen[ListStream[A]] = {
    if (time <= 0)
      Gen.const(List.empty)
    else if (time == 1)
      for {
        isReleased <- arbitrary[Boolean]
        proof <- if (isReleased) (union(ofNList(lg1), ofNList(lg2)))
        else ofNList(lg2)
      } yield proof
    else for {
      isReleased <- arbitrary[Boolean]
      ds <- if (!isReleased)
        always(lg2, time)
      else auxRelease[A](lg1,lg2,time)
    }  yield ds
  }

  def auxRelease[A](lg1 : Gen[List[A]], lg2 : Gen[List[A]], time: Int) : Gen[ListStream[A]] = {
    for {
      proofOffset <- Gen.choose(0, time - 1)
      prefix <- always(lg2, proofOffset)
      ending <- laterN(proofOffset, concat(lg1, lg2))
    } yield prefix.toList ++ ending.toList.filter(e => e != List.empty)
  }





  /**VENTANAS**/
  //Pasa los datos en listas a ventanas
  def toWindowsList[A](data: Gen[ListStream[A]], env: StreamExecutionEnvironment) = {
    val list: List[List[Any]] = data.sample.get.toList
    val stream = env.fromCollection(list:::List(null)::Nil)
    stream.countWindowAll(1)
  }

  //SIN USO
  //La idea era pasar los datos a ventanas haciendo uso de un trigger para que cada ventana
  //tuviera un numero diferente de datos sin necesidad de agruparlos en listas
  def toWindowsTrigger[A](data: Gen[ListStream[A]], env: StreamExecutionEnvironment) = {
    val list: List[List[Any]] = data.sample.get.toList
    val stream = env.fromCollection(list:::List(null)::Nil)
    stream
      .windowAll(GlobalWindows.create().asInstanceOf[WindowAssigner[Any, GlobalWindow]])
      .trigger(PurgingTrigger.of(CustomCountTrigger.of().asInstanceOf[Trigger[Any, GlobalWindow]]))
  }




}


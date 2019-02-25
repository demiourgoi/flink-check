package es.ucm.fdi.sscheck.flink.gen

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.AllWindowedStream
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.triggers.{PurgingTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen

/** Este objeto contiene todos los generadores necesarios para obtener datos aleatorios
  * con los que hacer random testing, incluyendo el paso a ventanas
  */

object WinGen{
  import ListStreamConversions._


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
  def ofNList[T](lg : Gen[List[T]], n: Int) : Gen[ListStream[T]] = {
    Gen.listOfN(n,lg)
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


  //Devuelve un ListStream con un único elemento generado por lg
  def now[A](lg : Gen[List[A]]): Gen[ListStream[A]] = {
    ofNList(lg,1)
  }

  //Devuelve un ListStream con un elemento vacio seguido de un único elemento generado por lg
  def next[A](lg : Gen[List[A]]): Gen[ListStream[A]] = {
    laterN(1, lg)
  }

  //Genera elementos de lg1 hasta generar un elemento de lg2, antes de tener mas de 'time' elementos
  def until[A](lg1 : Gen[List[A]], lg2 : Gen[List[A]], time: Int) : Gen[ListStream[A]] =
    if (time <= 0)
      Gen.const(List.empty)
    else if (time == 1)
      ofNList(lg2, 1)
    else
      for {
        proofOffset <- Gen.choose(0, time - 1)
        prefix <- always(lg1, proofOffset)
        dsg2Proof <- laterN(proofOffset, lg2)
      } yield prefix.toList ++ dsg2Proof.toList.filter(e => e != List.empty)


  //Genera elementos vacios hasta generar un elemento de lg, antes de tener mas de 'time' elementos
  def eventually[A](lg : Gen[List[A]], time: Int) : Gen[ListStream[A]] = {
    if (time <= 0) Gen.const(List.empty)
    else {
      val i = Gen.choose(0, time - 1).sample.get
      laterN(i, lg)
    }
  }


  //Genera 'time' elementos con el generador lg
  def always[A](lg : Gen[List[A]], time: Int) : Gen[ListStream[A]] =
    if (time <= 0)
      Gen.const(List.empty)
    else
      Gen.listOfN(time, lg)



  //Genera elementos de lg2 hasta generar la union de un elemento de lg1 y otro de lg2, antes de tener mas de 'time' elementos
  def release[A](lg1 : Gen[List[A]], lg2 : Gen[List[A]], time: Int) : Gen[ListStream[A]] = {
    if (time <= 0)
      Gen.const(List.empty)
    else if (time == 1)
      for {
        isReleased <- arbitrary[Boolean]
        proof <- if (isReleased) (union(ofNList(lg1,1), ofNList(lg2,1))) else ofNList(lg2,1)
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
      ending <- union(ofNList(lg1,1), ofNList(lg2,1))
    } yield prefix ++ ending
  }




  /**VENTANAS**/
  //Pasa los datos en listas a ventanas
  def toWindowsList[A](data: Gen[ListStream[A]], env: StreamExecutionEnvironment)(
                      implicit ev: TypeInformation[List[A]]):
  AllWindowedStream[List[A],GlobalWindow] = {
    val windowList = data.sample.get.toList
    val stream = env.fromCollection(windowList)
    stream.countWindowAll(1)
  }

//  //SIN USO
//  //La idea era pasar los datos a ventanas haciendo uso de un trigger para que cada ventana
//  //tuviera un numero diferente de datos sin necesidad de agruparlos en listas
//  def toWindowsTrigger[A](data: Gen[ListStream[A]], env: StreamExecutionEnvironment) = {
//    val list: List[List[Any]] = data.sample.get.toList
//    val stream = env.fromCollection(list:::List(null)::Nil)
//    stream
//      .windowAll(GlobalWindows.create().asInstanceOf[WindowAssigner[Any, GlobalWindow]])
//      .trigger(PurgingTrigger.of(CustomCountTrigger.of().asInstanceOf[Trigger[Any, GlobalWindow]]))
//  }
}

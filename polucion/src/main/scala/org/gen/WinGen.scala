package org.gen



import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._





object WinGen{
  val maxPollution = 20
  val numSensor = 3


  //Generador de datos con polucion (mayores o iguales que maxPollution)
  def genPol = for {
    pol <- Gen.choose(maxPollution, 100)
    sensor <- Gen.choose(1, numSensor)
  } yield (pol, sensor)

  //Generador de datos sin polucion (menores que maxPollution)
  def genNoPol = for {
    pol <- Gen.choose(0, maxPollution - 1)
    sensor <- Gen.choose(1, numSensor)
  } yield (pol, sensor)


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
  def ofNList[T](lg : Gen[List[T]]) : Gen[List[List[T]]] = {
    Gen.listOfN(1,lg)
  }

  //Devuelve una lista de listas con n listas vacias seguidas de lg
  def laterN[A](n : Int, lg : Gen[List[A]]) : Gen[List[List[A]]] = {
    for {
      list <- lg
      blanks = List.fill(n)(List.empty : List[A])
    } yield blanks:::list::Nil
  }


  //Concatena dos generadores de listas en uno
  def concatToList[A](lg1 : Gen[List[A]], lg2 : Gen[List[A]]) : Gen[List[List[A]]] = {
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

  def union[A](gs1 : Gen[List[List[A]]], gs2 : Gen[List[List[A]]]) : Gen[List[List[A]]] = {
    for {
      xs1 <- gs1
      xs2 <- gs2
    } yield concat2(xs1,xs2)
  }

  def concat2[A](list : List[List[A]], other : List[List[A]]) : List[List[A]] = {
    list.zipAll(other, List.empty, List.empty).map(xs12 => xs12._1 ++ xs12._2)
  }

  /**GENERADORES*/

  def until[A](lg1 : Gen[List[A]], lg2 : Gen[List[A]], time: Int) : Gen[List[List[A]]] =
      if (time <= 0)
        Gen.const(List.empty)
      else if (time == 1)
        ofNList(lg2)
      else
        for {
          proofOffset <- Gen.choose(0, time - 1)
          prefix <- always(lg1, proofOffset)
          dsg2Proof <- laterN(proofOffset, lg2)
        } yield prefix ++ dsg2Proof.filter(e => e != List.empty)



  def eventually[A](lg : Gen[List[A]], time: Int) : Gen[List[List[A]]] = {
    val i = Gen.choose(0, time - 1).sample.get

    if (time < 0) Gen.const(List.empty)
    else laterN(i, lg)
  }


  def always[A](lg : Gen[List[A]], time: Int) : Gen[List[List[A]]] =
      if (time <= 0) // supporting size == 0 is needed by the calls to always from until and release
        Gen.const(List.empty)
      else
        Gen.listOfN(time, lg)


  def release[A](lg1 : Gen[List[A]], lg2 : Gen[List[A]], time: Int) : Gen[List[List[A]]] =
      if (time <= 0)
        Gen.const(List.empty)
      else if (time == 1) for {
        isReleased <- arbitrary[Boolean]
        proof <- if (isReleased) (union(ofNList(lg1), ofNList(lg2))) else ofNList(lg2)
      } yield proof
      else for {
        isReleased <- arbitrary[Boolean]
        ds <- if (!isReleased)
                always(lg2, time)
              else for {
                proofOffset <- Gen.choose(0, time - 1)
                prefix <- always(lg2, proofOffset)
                ending <- laterN(proofOffset, concat(lg1, lg2))
               } yield prefix ++ ending.filter(e => e != List.empty)
      } yield ds


  //Pasamos los datos en listas a ventanas
  //TODO
  def toWindows[A](data: Gen[List[List[A]]], env: StreamExecutionEnvironment) = {
    val list = data.sample.get
    val size = list.head.length
    //Cambiamos las listas vacias por listas con un valor especial para que posteriormente representen ventanas vacias
    val d = list.map(e => if(e == List.empty) List.fill(size)(-1) else e)
    val stream = env.fromCollection(d.flatten)
    //Divide el stream en ventanas
    stream.countWindowAll(size + 1)
  }



  def main(args: Array[String]): Unit = {
    val size = 1//size windows
    val time = 6//instantes

    val pol = ofN(size,genPol)
    val noPol = ofN(size,genNoPol)

    //println(always(pol, 3).sample.get)
    val a = union(always(ofN(2, 3), 10), until(ofN(2,0), ofN(2,1), 10))
    println(a.sample.get)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    toWindows[(Int,Int)](a, env).fold(""){(acc, v) => println(acc)
                                                        acc + v}



    env.execute()





  }










}
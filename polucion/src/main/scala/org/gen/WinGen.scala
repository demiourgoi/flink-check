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

  /**GENERADORES*/

  def until[A](bg1 : Gen[List[A]], bg2 : Gen[List[A]], time: Int) : Gen[List[List[A]]] =
      if (time <= 0)
        Gen.const(List.empty)
      else if (time == 1)
        ofNList(bg2)
      else
        for {
          proofOffset <- Gen.choose(0, time - 1)
          prefix <- always(bg1, proofOffset)
          dsg2Proof <- laterN(proofOffset, bg2)
        } yield prefix ++ dsg2Proof.filter(e => e != List.empty)



  def eventually[A](bg : Gen[List[A]], time: Int) : Gen[List[List[A]]] = {
    val i = Gen.choose(0, time - 1).sample.get

    if (time < 0) Gen.const(List.empty)
    else laterN(i, bg)
  }


  def always[A](bg : Gen[List[A]], time: Int) : Gen[List[List[A]]] =
      if (time <= 0) // supporting size == 0 is needed by the calls to always from until and release
        Gen.const(List.empty)
      else
        Gen.listOfN(time, bg)


  def release[A](bg1 : Gen[List[A]], bg2 : Gen[List[A]], time: Int) : Gen[List[List[A]]] =
      if (time <= 0)
        Gen.const(List.empty)
      else if (time == 1) for {
        isReleased <- arbitrary[Boolean]
        proof <- if (isReleased) (concatToList(bg1, bg2)) else ofNList(bg2)
      } yield proof
      else for {
        isReleased <- arbitrary[Boolean]
        ds <- if (!isReleased)
                always(bg2, time)
              else for {
                proofOffset <- Gen.choose(0, time - 1)
                prefix <- always(bg2, proofOffset)
                ending <- laterN(proofOffset, concat(bg1, bg2))
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
    val a = always(pol,time)
    println(a.sample.get)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    toWindows[(Int,Int)](a, env).fold(""){(acc, v) => println(acc)
                                                        acc + v}



    env.execute()





  }










}
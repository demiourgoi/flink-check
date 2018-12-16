package org.gen

import org.scalacheck.Gen
import scala.language.implicitConversions



object ListStreamConversions{
  implicit def transformFromGen[A](gen : Gen[List[List[A]]]) : Gen[ListStream[A]] = gen.map(x => new ListStream[A](x))
  implicit def transformToGen[A](gen : Gen[ListStream[A]]) : Gen[List[List[A]]] = gen.map(x => x.toList)
  implicit def transformFromList[A](l : List[List[A]]) : ListStream[A] = new ListStream[A](l)
  implicit def transformToList[A](l : ListStream[A]) : List[List[A]] = l.toList
}


class ListStream[A](list : List[List[A]]) {

  val listStream : List[List[A]] = list


  def toList : List[List[A]] = listStream


  override def toString = {
    list.toString()
  }

}


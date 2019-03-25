package es.ucm.fdi.sscheck.prop.tl

import org.apache.flink.api.scala._

package object flink {
  /** @param timestamp milliseconds since epoch */
  case class TimedElement[T](timestamp: Long, value: T)

  /** Represents a time window with timed values. Note timestamp can be smaller than the earlier
    * element in data, for example in a tumbling window where a new event starts at a regular rate,
    * independently of the actual elements in the window
    * @param timestamp milliseconds since epoch for the start of the window
    * */
  case class TimedWindow[T](timestamp: Long, data: DataSet[TimedElement[T]])

  /** Used to specify a default parallelism for Flink
    * */
  case class Parallelism(val numPartitions : Int)
}
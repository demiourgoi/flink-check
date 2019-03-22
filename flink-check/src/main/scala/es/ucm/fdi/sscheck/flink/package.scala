package es.ucm.fdi.sscheck

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

package object flink {
    // FIXME bad code organization
  object TimedValue {
    private[this] def tumblingWindowIndex(windowSizeMillis: Long, startTimestamp: Long)(timestamp: Long): Int = {
      val timeOffset = timestamp - startTimestamp
      (timeOffset / windowSizeMillis).toInt
    }

    def tumblingWindows[T](windowSize: Time, startTime: Time)
                                            (data: DataSet[TimedValue[T]]): Iterator[TimedWindow[T]] =
      if (data.count() <= 0) Iterator.empty
      else {
        val windowSizeMillis = windowSize.toMilliseconds
        val startTimestamp = startTime.toMilliseconds
        val endTimestamp = data.map{_.timestamp}.reduce(scala.math.max(_, _)).collect().head
        val endingWindowIndex = tumblingWindowIndex(windowSizeMillis, startTimestamp)(endTimestamp)

        Iterator.range(0, endingWindowIndex + 1).map { windowIndex =>
          val windowData = data.filter{record =>
            tumblingWindowIndex(windowSizeMillis, startTimestamp)(record.timestamp) == windowIndex
          }
          TimedWindow(startTimestamp + windowIndex*windowSizeMillis, windowData)
        }
      }
  }
  // FIXME rename to TimedElement: that is Flink's nomenclature
  /** @param timestamp milliseconds since epoch */
  case class TimedValue[T](timestamp: Long, value: T)

  /** Represents a time window with timed values. Note timestamp can be smaller than the earlier
    * element in data, for example in a tumbling window where a new event starts at a regular rate,
    * independently of the actual elements in the window
    * @param timestamp milliseconds since epoch for the start of the window
    * */
  case class TimedWindow[T](timestamp: Long, data: DataSet[TimedValue[T]])
}

package flink {

}
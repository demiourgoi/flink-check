package es.ucm.fdi.sscheck

import es.ucm.fdi.sscheck.prop.tl.flink.{TimedElement, TimedWindow}
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
                                            (data: DataSet[TimedElement[T]]): Iterator[TimedWindow[T]] =
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
}


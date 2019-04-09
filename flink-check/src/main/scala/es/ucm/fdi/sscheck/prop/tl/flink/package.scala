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

package flink {
  import org.apache.flink.streaming.api.windowing.time.Time
  import es.ucm.fdi.sscheck.prop.tl.{Time => SscheckTime}
  import org.scalacheck.Prop

  trait StreamDiscretizer {
    /** @param data data set to split into windows, using the timestamp of TimedElement as time
      *
      * Note: windows are generated until covering the last element. That means that empty windows at the end
      * are ignored. We are not supporting a lastWindowEndTime parameter because that cannot be computed
      * from a `TSeq[A]` without knowing the windowing criteria, as conceptually a window can extend beyond
      * the timestamp of it's latest element
      * */
    def getWindows[T](data: DataSet[TimedElement[T]]): Iterator[TimedWindow[T]]
  }

  object FlinkFormula {
    implicit class SplitterMissingFlinkFormula[T](formula: Formula[T]) extends Serializable {
      def groupBy(discretizer: StreamDiscretizer): FlinkFormula[T] = FlinkFormula(formula, discretizer)
    }

    /** Split data as a series of tumbling windows of size windowSize and starting at startTime
      *
      * @param windowSize Size of the tumbling window
      * @param startTime Start time of the first window. Note, as windows can be empty, we do NOT require
      *                  to have at least one element in data with that time stamp
      *
      * */
    case class TumblingTimeWindows(@transient size: Time,
                                   @transient startTime: Time = Time.milliseconds(0))
      extends StreamDiscretizer {

      private[this] def tumblingWindowIndex(windowSizeMillis: Long, startTimestamp: Long)(timestamp: Long): Int = {
        val timeOffset = timestamp - startTimestamp
        (timeOffset / windowSizeMillis).toInt
      }

      override def getWindows[T](data: DataSet[TimedElement[T]]): Iterator[TimedWindow[T]] =
        if (data.first(1).count() <= 0) Iterator.empty
        else {
          val windowSizeMillis = size.toMilliseconds
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

    case class SlidingTimeWindows(@transient size: Time,
                                  @transient slide: Time,
                                  @transient startTime: Time = Time.milliseconds(0))
      extends StreamDiscretizer {

      override def getWindows[T](data: DataSet[TimedElement[T]]): Iterator[TimedWindow[T]] =
        if (data.first(1).count() <= 0) Iterator.empty
        else {
          val windowSizeMillis = size.toMilliseconds
          val windowSlideMillis = slide.toMilliseconds
          val startTimestamp = startTime.toMilliseconds
          val endTimestamp = data.map{_.timestamp}.reduce(scala.math.max(_, _)).collect().head
          Range.Long(startTimestamp, endTimestamp+1, windowSlideMillis).toIterator.map { windowStart =>
            val windowData = data.filter{ record =>
              /* window start (inclusive) and the windows end (exclusive) */
              record.timestamp >= windowStart && record.timestamp < windowStart + windowSizeMillis
            }
            TimedWindow(windowStart, windowData)
          }
        }
    }

    implicit private[flink] val FormulaParallelism: FormulaParallelism = SequentialFormulaParallelism
  }
  case class FlinkFormula[T](formula: Formula[T], @transient discretizer: StreamDiscretizer) {
    import FlinkFormula.FormulaParallelism

    def nextFormula: FlinkNextFormula[T] = FlinkNextFormula(formula.nextFormula)
  }

  case class FlinkNextFormula[T](formula: NextFormula[T]) {
    import FlinkFormula.FormulaParallelism

    def consume(time: SscheckTime)(atoms: T): FlinkNextFormula[T] =
      FlinkNextFormula(formula.consume(time)(atoms))

    def result : Option[Prop.Status] = formula.result
  }
}
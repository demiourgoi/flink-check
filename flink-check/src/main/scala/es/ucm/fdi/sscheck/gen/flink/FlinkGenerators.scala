package es.ucm.fdi.sscheck.gen.flink

import es.ucm.fdi.sscheck.gen.PStream
import es.ucm.fdi.sscheck.prop.tl.flink.TimedElement
import org.apache.flink.streaming.api.windowing.time.Time
import org.scalacheck.Gen

object FlinkGenerators {

  /** Converts a generator of sequence of sequences into a sequence of TimedElement by flattening the generated sequence
    * of sequences, and assigning a timestamp to each element computed by interpreting each nested sequence as a
    * tumbling window of size windowSize, with the first window starting at startEpoch. Inside each window, the
    * timestamp of each element is a random value between the window start (inclusive) and the windows end (exclusive),
    * obtained using a uniform distribution
    *
    * Elements in the generated sequence will be sorted by timestamp in ascending order.
    *
    * Note: there is no warranty that for each window has a record with the window start time as timestamp.
    * This also allows this method to support empty windows.
    * */
  def tumblingTimeWindow[A](windowSize: Time, startTime: Time = Time.milliseconds(0))
                           (windowsGen: Gen[PStream[A]]): Gen[Seq[TimedElement[A]]] = {

    // "Time-based windows have a start timestamp (inclusive) and an end timestamp (exclusive) that together
    // describe the size of the window."
    // https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/stream/operators/windows.html
    val timestampOffsetGen = Gen.choose(min=0, max=(windowSize.toMilliseconds)-1)
    for {
      windows <- windowsGen
    } yield {
      windows.zipWithIndex.flatMap { case (window, i) =>
        window.map { value =>
          val offset = timestampOffsetGen.sample.getOrElse(0L)
          val timestamp = startTime.toMilliseconds + (i * (windowSize.toMilliseconds)) + offset
          TimedElement(timestamp, value)
        }.sortBy(_.timestamp)
      }
    }
  }

  // FIXME: tumblingTimeWindow as slidingTimeWindow

  def slidingTimeWindow[A](windowSize: Time, windowSlide: Time, startTime: Time = Time.milliseconds(0))
                       (windowsGen: Gen[PStream[A]]): Gen[Seq[TimedElement[A]]] = {
    ???
  }

}

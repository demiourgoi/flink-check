package es.ucm.fdi.sscheck.flink.poc

import java.io.OutputStream

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.serialization.{Encoder, SimpleStringEncoder}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => FlinkTuple2}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.core.memory.DataOutputViewStreamWrapper
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.hadoop.io.IntWritable
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.matcher.{ResultMatchers, ThrownExpectations}
import org.specs2.runner.JUnitRunner

import scala.language.{implicitConversions, reflectiveCalls}
import scala.collection.JavaConverters._


/** Encoder that writes elements T into byte arrays using Flink's serialization. Note
  * the resulting files won't be usable across versions of Flink even different
  * configurations of the same program, as that can change the serialization options. This
  * is useful only for temporal storage of data used by different parts of the same program */
class FlinkBinarySerializationEncoder[T : TypeInformation](config: ExecutionConfig) extends Encoder[T] {
  // see example on https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/serialization/SimpleStringEncoder.java

  val typeSerializer = implicitly[TypeInformation[T]].createSerializer(config)

  override def encode(element: T, stream: OutputStream): Unit = {
    val dataOutputView  = new DataOutputViewStreamWrapper(stream)
    typeSerializer.serialize(element, dataOutputView)
  }
}

case class TimedValue[T](timestamp: Long, value: T)
class AddTimestamp[T] extends ProcessFunction[T, TimedValue[T]] {
  override def processElement(value: T, ctx: ProcessFunction[T, TimedValue[T]]#Context,
                              out: Collector[TimedValue[T]]): Unit = {
    out.collect(TimedValue(ctx.timestamp(), value))
  }
}
object TimedInt {
  def apply(line: String) = {
    val parts = line.split(",")
    new TimedValue(parts(0).toLong, parts(1).toInt)
  }
}

@RunWith(classOf[JUnitRunner])
class WindowRecorderPoC
  extends Specification with ResultMatchers with ThrownExpectations {

  def is =
    s2"""
        - where foo
        - where readRecordedWindows
        - where bar
        - where $binaryBar
      """

  /* This works fine with event time because [watermarks](https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/event_time.html#event-time-and-watermarks)
  won't find late events, as timestamps are sorted. This wouldn't work so well with unsorted time stamps
  * */
  // expect csv with schema (timestamp millis, int value)
  val inputPDstreamDataPath = getClass.getResource("/timed_int_pdstream_01.csv").getPath
  def eventTimeIntStream(path: String)(implicit env: StreamExecutionEnvironment): DataStream[Int] = {
    env.readTextFile(path)
      .map(TimedInt(_))
      .assignAscendingTimestamps( _.timestamp)
      .map(_.value)
  }

  def discretize(xs: DataStream[Int]): WindowedStream[Int, Int, TimeWindow] =
    xs.keyBy(identity[Int] _)
    .timeWindow(Time.seconds(1))


  def fixture = new {
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate event time
  }

  def subjectAdd(xs: DataStream[Int]): DataStream[Int] = xs.map{_ + 1}
  def binaryBar = {
    val f = fixture
    val env: StreamExecutionEnvironment = f.env

    // we have to get from TimedInt to Int in order to be able to apply
    // the test subject as is: the timestamps are still on the Flink runtime
    val input = env.readTextFile(inputPDstreamDataPath)
      .map(TimedInt(_))
      .assignAscendingTimestamps( _.timestamp)
      .map(_.value)
    val output = subjectAdd(input)

    val timedInput: DataStream[TimedValue[Int]] = input.process(new AddTimestamp())
    val timedOutput = output.process(new AddTimestamp())

    val inputSink =  StreamingFileSink
      .forRowFormat(new Path("/tmp/WindowRecorderPoC/input"),
        new FlinkBinarySerializationEncoder[TimedValue[Int]](env.getConfig))
      .build()
    timedInput.addSink(inputSink)

    val ouputSink =  StreamingFileSink
      .forRowFormat(new Path("/tmp/WindowRecorderPoC/output"),
        new FlinkBinarySerializationEncoder[TimedValue[Int]](env.getConfig))
      .build()
    timedOutput.addSink(ouputSink)

//    timedOutput.map{x => s"output($x)"}.print()
//    timedInput.map{x => s"input($x)"}.print()
    env.execute()

    ok
  }

  def readRecordedWindows = {
    val env = ExecutionEnvironment.createLocalEnvironment(3)
    import org.apache.flink.hadoopcompatibility.HadoopInputs
//    val hadoopInput = HadoopInputs.readSequenceFile(classOf[IntWritable], classOf[FlinkWritable[List[Int]]], "/tmp/WindowRecorderPoC/foo")
////    val typeInfo = implicitly[TypeInformation[Tuple2[IntWritable, ]]]
//    val input = env.createInput(hadoopInput)
//    input.print()
////    println(input.count())

    ok
  }
}

package es.ucm.fdi.sscheck.flink.poc

import scala.language.implicitConversions
import scala.language.reflectiveCalls
import scala.collection.JavaConverters._

import java.io.{DataInput, DataOutput}
import java.time.format.DateTimeFormatter

import org.apache.flink.api.java.tuple.{Tuple2 => FlinkTuple2}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.hadoop.io.{IntWritable, BytesWritable => HadoopBytesWritable, Writable => HadoopWritable}

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.{DateTimeBucketAssigner, SimpleVersionedStringSerializer}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
import org.apache.flink.util.Collector
import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.matcher.{ResultMatchers, ThrownExpectations}
import org.specs2.runner.JUnitRunner

object TimedInt {
  def apply(line: String) = {
    val parts = line.split(",")
    new TimedInt(parts(0).toLong, parts(1).toInt)
  }
}
case class TimedInt(timestamp: Long, value: Int)

class HadoopDataInputView
/** A Hadoop Writable that writes using Flink's type information. Note this is not
  * portable across program versions, as Flink serialization is designed for in flight
  * data and can change after a version update. That's ok for us because we will reply
  * the window data from HDFS just after writing it */
class FlinkWritable[T : TypeInformation](var value: T)(config: ExecutionConfig) extends HadoopWritable {
  val typeSerializer = implicitly[TypeInformation[T]].createSerializer(config)
  val bytesWritable = new HadoopBytesWritable()
  val inputDeserializer = new DataInputDeserializer()
  val outputSerializer = new DataOutputSerializer(1)

  def init(): Unit = {}

  override def write(out: DataOutput): Unit = {
    typeSerializer.serialize(value, outputSerializer)
    bytesWritable.write(outputSerializer)
  }

  override def readFields(in: DataInput): Unit = {
    bytesWritable.readFields(in)
    inputDeserializer.setBuffer(bytesWritable.copyBytes())
    value = typeSerializer.deserialize(inputDeserializer)
  }
}

object EventTimeTumblingWindowBucketAssigner {
  // TODO add as constructor parameters for `class EventTimeTumblingWindowBucketAssigner`
  val FormatString = "yyyy-MM-dd--HH-mm-ss--n"
  val ZoneId = java.time.ZoneId.systemDefault()
}
/** A BucketAssigner similar to [[DateTimeBucketAssigner]] but using event time, and assigning
  * each record to a window encoded by the start time of the window, for a sequence of tumbling
  * windows of size windowSize starting at startTime
  */
// Very similar to https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/DateTimeBucketAssigner.java
class EventTimeTumblingWindowBucketAssigner[T](startTime: Time, windowSize: Time)
  extends BucketAssigner[T, String] {

  val dateTimeFormatter = DateTimeFormatter
    .ofPattern(EventTimeTumblingWindowBucketAssigner.FormatString)
    .withZone(EventTimeTumblingWindowBucketAssigner.ZoneId)

  override def getBucketId(element: T, context: BucketAssigner.Context): String = {
//    context.
    ???
  }

  override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
}

case class TimedValue[T](timestamp: Long, value: T)
class AddTimestamp[T] extends ProcessFunction[T, TimedValue[T]] {
  override def processElement(value: T, ctx: ProcessFunction[T, TimedValue[T]]#Context,
                              out: Collector[TimedValue[T]]): Unit = {
    out.collect(TimedValue(ctx.timestamp(), value))
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
  def bar = {
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

    timedOutput.map{x => s"output($x)"}.print()
    timedInput.map{x => s"input($x)"}.print()
    env.execute()

    ok
  }

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

    timedOutput.map{x => s"output($x)"}.print()
    timedInput.map{x => s"input($x)"}.print()
    env.execute()

    ok
  }

  type SeqFileWrap[T] = FlinkTuple2[IntWritable, FlinkWritable[T]]

  def foo = {
    val f = fixture
    implicit val env: StreamExecutionEnvironment = f.env

    val input = eventTimeIntStream(inputPDstreamDataPath)(env)
    val windows: DataStream[List[Int]] =
      discretize(input).foldWith(Nil:List[Int]){(xs, x) => x::xs}
    // TODO create and delete folder if needed
    // TODO organize windows using https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/connectors/filesystem_sink.html
    // so we can reconstruct each window with a parallel read. Use the timestamps
    // TODO support arbitrary types: avro?, kyro? see Flink serialization
//    windows.addSink(new BucketingSink[List[Int]]("/tmp/WindowRecorderPoC"))

//    val seqWindows: DataStream[SeqFileWrap[List[Int]]] =
//      windows.map(new RichMapFunction[List[Int], SeqFileWrap[List[Int]]] {
//        override def map(window: List[Int]) =
//          new FlinkTuple2(new IntWritable(window.hashCode()), // FIXME consider
//            new FlinkWritable(window)(getRuntimeContext.getExecutionConfig))
//      })
//
//    val sink = new BucketingSink[SeqFileWrap[List[Int]]]("/tmp/WindowRecorderPoC")
//    sink.setWriter(new SequenceFileWriter[IntWritable, FlinkWritable[List[Int]]]())
//    sink.setBatchRolloverInterval(1000)
//    seqWindows.addSink(sink)

    val sink = StreamingFileSink
      .forRowFormat(new Path("/tmp/WindowRecorderPoC"),
        new SimpleStringEncoder[List[Int]]("UTF-8"))
//      .withBucketCheckInterval(1000)
//      .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd--HH-mm-ss--LLL"))
        .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd--HH-mm-ss--n"))
      .build()
    windows.addSink(sink)


    windows.timeWindowAll(Time.seconds(1)).reduce(_ ::: _).print()

    env.execute()

    // FIXME double check this is stable

    ok
  }

  def readRecordedWindows = {
    val env = ExecutionEnvironment.createLocalEnvironment(3)
    import org.apache.flink.hadoopcompatibility.HadoopInputs
    val hadoopInput = HadoopInputs.readSequenceFile(classOf[IntWritable], classOf[FlinkWritable[List[Int]]], "/tmp/WindowRecorderPoC/foo")
//    val typeInfo = implicitly[TypeInformation[Tuple2[IntWritable, ]]]
    val input = env.createInput(hadoopInput)
    input.print()
//    println(input.count())

    ok
  }
}

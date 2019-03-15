package es.ucm.fdi.sscheck.flink.poc

import java.io.{DataInput, DataOutput}

import org.apache.flink.api.java.tuple.{Tuple2 => FlinkTuple2}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.hadoop.io.{IntWritable, BytesWritable => HadoopBytesWritable, Writable => HadoopWritable}

import scala.language.implicitConversions
import scala.language.reflectiveCalls
import org.apache.flink.api.scala._
import org.apache.flink.core.memory.{DataInputDeserializer, DataOutputSerializer}
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink
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

@RunWith(classOf[JUnitRunner])
class WindowRecorderPoC
  extends Specification with ResultMatchers with ThrownExpectations {

  def is =
    s2"""
        - where foo
        - where $readRecordedWindows
      """

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

    val seqWindows: DataStream[SeqFileWrap[List[Int]]] =
      windows.map(new RichMapFunction[List[Int], SeqFileWrap[List[Int]]] {
        override def map(window: List[Int]) =
          new FlinkTuple2(new IntWritable(window.hashCode()), // FIXME consider
            new FlinkWritable(window)(getRuntimeContext.getExecutionConfig))
      })

    val sink = new BucketingSink[SeqFileWrap[List[Int]]]("/tmp/WindowRecorderPoC")
    sink.setWriter(new SequenceFileWriter[IntWritable, FlinkWritable[List[Int]]]())
    sink.setBatchRolloverInterval(1000)
    seqWindows.addSink(sink)

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

package es.ucm.fdi.sscheck.prop.tl.flink

import es.ucm.fdi.sscheck.prop.tl.Formula
import org.slf4j.LoggerFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.scalacheck.{Gen, Prop}
import org.scalacheck.util.Pretty

object DataStreamTLProperty {
  @transient private val logger = LoggerFactory.getLogger("DataStreamTLProperty")

  type SSeq[A] = Seq[Seq[A]]
  type SSGen[A] = Gen[SSeq[A]]
}

trait DataStreamTLProperty {
  import DataStreamTLProperty._

  type Letter[In, Out] = (DataSet[TimedElement[In]], DataSet[TimedElement[Out]])

  /** Size of the tumbling windows to be used to discretize both the input
    * and output streams
    * */
  def letterSize : Time

  /** Override for custom configuration
    *
    *  Maximum number of letters (i.e., tumbling windows) that the test case will wait for, 100 by default
    * */
  def maxNumberLettersPerTestCase: Int = 100

  def defaultParallelism: Parallelism = Parallelism(4)

  /** @return a newly created StreamExecutionEnvironment, for which no stream or action has
  *  been defined, and that it's not started
  *  */
  def buildFreshStreamExecutionEnvironment(): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate event time
    env
  }

  /** @return a newly created ExecutionEnvironment, for which no data set or action has
    *  been defined, and that it's not started
    *  */
  def buildFreshExecutionEnvironment(): ExecutionEnvironment = {
    ExecutionEnvironment.createLocalEnvironment(defaultParallelism.numPartitions)
  }

  // 1 in 1 out
  /** @return a ScalaCheck property that is executed by:
    *  - generating an input DataStream with generator. Each generated value is interpreted as a sequence of tumbling
    *  windows of size this.letterSize. Each element in a window is assigned a random timestamp between the start and
    *  the end of the window (using a uniform distribution as provided by ScalaCheck's Gen.choose)
    *  - generating an output DataStream applying testSubject on the input DataStream
    *  - checking formula on a discretization of those DataStream created as a tumbling
    *  window of size this.letterSize
    *
    *  The property is satisfied iff all the test cases satisfy the formula.
    *  A new streaming context is created for each test case to isolate its
    *  execution.
    * */
  def forAllDataStream[In:TypeInformation,Out:TypeInformation](generator: SSGen[Int])
                                                              (testSubject: (DataStream[In]) => DataStream[Out])
                                                              (formula: Formula[Letter[In, Out]])
                                                              (implicit pp1: SSeq[In] => Pretty): Prop = {
    // FIXME
    Prop.forAll(0){_  => true}
  }
}
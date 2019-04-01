package es.ucm.fdi.sscheck.prop.tl.flink

import java.nio.file.{Files, Path => JPath}

import es.ucm.fdi.sscheck.prop.tl.{Formula, NextFormula, Time => SscheckTime}
import es.ucm.fdi.sscheck.{TestCaseId, TestCaseIdCounter}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.scalacheck.util.Pretty
import org.scalacheck.{Gen, Prop}
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._
import scala.util.Properties.lineSeparator

object DataStreamTLProperty {
  type TSeq[A] = Seq[TimedElement[A]]
  type TSGen[A] = Gen[TSeq[A]]
  type Letter[In, Out] = (DataSet[TimedElement[In]], DataSet[TimedElement[Out]])
}

trait DataStreamTLProperty {
  import DataStreamTLProperty._

  @transient private val logger = LoggerFactory.getLogger(DataStreamTLProperty.getClass)

  // FIXME remove
  /** Size of the tumbling windows to be used to discretize both the input
    * and output streams during test case evaluation
    * */
  def letterSize : Time

  /** Override for custom configuration
    *
    *  Maximum number of letters (i.e., tumbling windows) that the test case will wait for, 100 by default
    * */
  def maxNumberLettersPerTestCase: Int = 100

  /** Override for custom configuration */
  def defaultParallelism: Parallelism = Parallelism(4)

  /** How many elements of each window to print during test case evaluation */
  def showNSampleElementsOnEvaluation: Int = 5

  /** If override the returned environment should have event time set.
    *  @return a newly created StreamExecutionEnvironment, for which no stream or action has
    *          been defined, and that it's not started.
    */
  def buildFreshStreamExecutionEnvironment(): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate event time
    env
  }

  /** @return a newly created ExecutionEnvironment, for which no data set or action has
    *  been defined, and that it's not started
    */
  def buildFreshExecutionEnvironment(): ExecutionEnvironment = {
    ExecutionEnvironment.createLocalEnvironment(defaultParallelism.numPartitions)
  }

  // 1 in 1 out
  /** @return a ScalaCheck property that is executed by:
    *  - generating an input DataStream: we generate a sequence of TimedElement with generator, and then we
    *  build a DataStream[In] from it by using the timestamp of each element as its **event time**, and then
    *  discarding the timestamp.
    *  - generating an output DataStream applying testSubject on the input DataStream
    *  - checking formula on a discretization of those DataStream created as a tumbling
    *  window of size this.letterSize
    *
    *  The property is satisfied iff all the test cases satisfy the formula.
    *  A new streaming context is created for each test case to isolate its
    *  execution.
    *
    *  Note: this assumes a stream execution environment configured with event time, which is used to get more
    *  deterministic behaviour, and to place the generated elements on the stream.
    * */
  def forAllDataStream[In : TypeInformation, Out : TypeInformation](generator: TSGen[In])
                                                                   (testSubject: (DataStream[In]) => DataStream[Out])
                                                                   (formula: Formula[Letter[In, Out]])
                                                                   (implicit pp1: TSGen[In] => Pretty): Prop = {
    val testCaseIdCounter = new TestCaseIdCounter

    Prop.forAllNoShrink(generator) { testCase: TSeq[In] =>
      // Setup new test case
      val testCaseId = testCaseIdCounter.nextId()
      val streamEnv = buildFreshStreamExecutionEnvironment()
      val env = buildFreshExecutionEnvironment()
      val testCaseContext = new TestCaseContext[In, Out](
        testCase, testSubject, formula)(
        letterSize, maxNumberLettersPerTestCase, showNSampleElementsOnEvaluation)(
        testCaseId, streamEnv, env)
      logger.warn("Starting execution of test case {}", testCaseId)

      // NOTE: `testCaseContext.run()` is a blocking call
      // TODO: concurrent test case execution, provided concurrent creation and execution
      // of different StreamExecutionEnvironment and ExecutionEnvironment in the same JVM
      // are ok, and Flink has no issues similar to SPARK-2243. In any case that requires
      // setting up a configurable limit to the number of concurrent test cases to avoid
      // starving the host resources; TBD how well this performs on a distributed environment,
      // where it might make sense disabling that limit as the resource manager for the cluster
      // system would handle that; TBD if Flink also implements a resource manager in local
      // cluster mode that makes such concurrent test case execution limit unnecessary
      val testCaseResult = testCaseContext.computeResult()
      logger.warn("Completed execution of test case {} with result {}", testCaseId, testCaseResult)

      testCaseResult match {
        case Prop.True => Prop.passed
        case Prop.Proof => Prop.proved
        case Prop.False => Prop.falsified
        case Prop.Undecided => Prop.passed //Prop.undecided FIXME make configurable
        case Prop.Exception(e) => Prop.exception(e)
      }
    }
  }
}

// FIXME: refactor common fields with Spark's version
object TestCaseContext {
  def createTempDir(testCaseName: String = "flink-check_"): JPath = {
    val tempDir = Files.createTempDirectory(testCaseName)
    tempDir.toFile.deleteOnExit()
    tempDir
  }

  val InputStreamName = "Input"
  val OutputStreamName = "Output"

  object Exercise {
    val logger = LoggerFactory.getLogger(Exercise.getClass)

    // TODO: support parallelism using SplittableIterator
    /**
      * Note: the DataStream is created with [[StreamExecutionEnvironment#fromCollection]] so it is not parallel
      */
    def timedElementsToStream[A : TypeInformation](timedElements: Seq[TimedElement[A]])
                                                  (env: StreamExecutionEnvironment): DataStream[A] = {
      require(env.getStreamTimeCharacteristic == TimeCharacteristic.EventTime,
        logger.error("Event time required for converting a PDStream value into a Flink DataStream"))

      // This works fine with event time because [watermarks](https://ci.apache.org/projects/flink/flink-docs-release-1.7/dev/event_time.html#event-time-and-watermarks)
      // won't find late events, as timestamps are sorted. This wouldn't work so well with unsorted time stamps
      env.fromCollection(timedElements)
        .assignAscendingTimestamps(_.timestamp)
        .map(_.value)
    }

    /** Converts each record into a [[es.ucm.fdi.sscheck.prop.tl.flink.TimedElement]], adding the timestamp provided
      * by [[ProcessFunction#Context]]. That means this will fail if the time characteristic
      * is ProcessingTime. So far this has been tested with event time.
      * */
    class AddTimestamp[A] extends ProcessFunction[A, TimedElement[A]] {
      override def processElement(value: A, ctx: ProcessFunction[A, TimedElement[A]]#Context,
                                  out: Collector[TimedElement[A]]): Unit = {
        out.collect(TimedElement(ctx.timestamp(), value))
      }
    }

    def storeDataStream[A : TypeInformation](stream: DataStream[A])(outputDir: String): Unit = {
      val format = new TypeSerializerOutputFormat[A]
      format.setOutputFilePath(new Path(outputDir))
      stream.writeUsingOutputFormat(format)
    }

    def storeDataStreamWithTimestamps[A : TypeInformation](stream: DataStream[A])(outputDir: String): Unit =
      storeDataStream(stream.process(new AddTimestamp[A]()))(outputDir)
  }

  object Evaluate {
    val logger = LoggerFactory.getLogger(Evaluate.getClass)

    def readRecordedStreamWithTimestamps[T : TypeInformation](path: String)(env: ExecutionEnvironment) = {
      val timedInputInputFormat = new TypeSerializerInputFormat[TimedElement[T]](
        implicitly[TypeInformation[TimedElement[T]]])
      env.readFile(timedInputInputFormat, path)
    }

    private[this] def tumblingWindowIndex(windowSizeMillis: Long, startTimestamp: Long)(timestamp: Long): Int = {
      val timeOffset = timestamp - startTimestamp
      (timeOffset / windowSizeMillis).toInt
    }

    /** Split data as a series of tumbling windows of size windowSize and starting at startTime
      *
      * @param windowSize Size of the tumbling window
      * @param startTime Start time of the first window. Note, as windows can be empty, we do NOT require
      *                  to have at least one element in data with that time stamp
      * @param data data set to split into windows, using the timestamp of TimedElement as time
      *
      * Note: windows are generated until covering the last element. That means that empty windows at the end
      * are ignored. We are not supporting a lastWindowEndTime parameter because that cannot be computed
      * from a `TSeq[A]` without knowing the windowing criteria, as conceptually a window can extend beyond
      * the timestamp of it's latest element
      * */
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

    /** Print some records in a window to get some logging that helps developing tests.
      * */
    def printWindowHead[T](window: TimedWindow[T], streamName: String, showNSampleElements: Int): Unit = {
      val numElements = window.data.count()
      logger.debug(
        s"""Time: ${window.timestamp} - ${streamName} (${numElements} elements)
           |{}
           |...
         """.stripMargin, window.data.first(showNSampleElements).collect().mkString(lineSeparator))
    }
  }
}
/** Runs a test case by
  * 1. Exercise the test case: generate an input DataStream from testCase and an
  * output DataStream applying testSubject to the input, and persist both streams
  * 2. Evaluate the test case exercise: read the persisted input and output streams,
  * and evaluate formulaNext on a discretization of them
  * */
class TestCaseContext[In : TypeInformation, Out : TypeInformation](
  @transient private val testCase: DataStreamTLProperty.TSeq[In],
  @transient private val testSubject: (DataStream[In]) => DataStream[Out],
  @transient private val formula: Formula[DataStreamTLProperty.Letter[In, Out]])(
  @transient val letterSize: Time,
  @transient private val maxNumberLettersPerTestCase: Int,
  @transient val showNSampleElements: Int)(
  @transient private val testCaseId: TestCaseId,
  @transient private val streamEnv: StreamExecutionEnvironment,
  @transient private val env: ExecutionEnvironment)

  extends Serializable {

  import TestCaseContext._

  @transient private val logger = LoggerFactory.getLogger(TestCaseContext.getClass)

  @transient private var result: Option[Prop.Status] = None

  // FIXME: remove when `DataStreamTLProperty.letterSize` is replaced by a param with default in forAllDataStream
  // we could use Time.milliseconds(Instant.now().toEpochMilli) here instead, but Flink
  // doesn't care as long as the timestamps are increasing so the watermark moves smoothly,
  // and starting from 0 leads to timestamps that are usually easier to read (as letterSize
  // is usually a multiple of 10)
  @transient val testCaseStartTime = Time.milliseconds(0)

  private def exerciseTestCase(testCaseInputRecordPath: JPath,
                               testCaseOutputRecordPath: JPath): Unit = {
    logger.info(s"Exercising test case {}: {}", testCaseId, testCase)

    val input = Exercise.timedElementsToStream(testCase)(streamEnv)
    val output = testSubject(input)
    Exercise.storeDataStreamWithTimestamps(input)(testCaseInputRecordPath.toString)
    Exercise.storeDataStreamWithTimestamps(output)(testCaseOutputRecordPath.toString)
    streamEnv.execute()

    logger.info(s"Completed exercise for test case {}", testCaseId)
  }

  private def evaluateTestCase(testCaseInputRecordPath: JPath,
                               testCaseOutputRecordPath: JPath): NextFormula[DataStreamTLProperty.Letter[In, Out]] = {
    logger.info(s"Evaluating test case {}", testCaseId)
    var currFormula = formula.nextFormula

    val timedInput = Evaluate.readRecordedStreamWithTimestamps[In](testCaseInputRecordPath.toString)(env)
    val timedOutput = Evaluate.readRecordedStreamWithTimestamps[Out](testCaseOutputRecordPath.toString)(env)
    val inputWindows = Evaluate.tumblingWindows(letterSize, testCaseStartTime)(timedInput)
    val outputWindows = Evaluate.tumblingWindows(letterSize, testCaseStartTime)(timedOutput)

    // FIXME: delete files at the end
    breakable {
      inputWindows.zip(outputWindows).zipWithIndex.foreach{case ((inputWindow, outputWindow), windowIndex) =>
        val windowStartTimestamp = inputWindow.timestamp
        assume(windowStartTimestamp == outputWindow.timestamp,
        logger.error("Input and output window are not aligned"))
        logger.debug("Checking window #{} with timestamp {}", windowIndex, windowStartTimestamp)
        Evaluate.printWindowHead(inputWindow, InputStreamName, showNSampleElements)
        Evaluate.printWindowHead(outputWindow, OutputStreamName, showNSampleElements)
        val currentLetter = (inputWindow.data, outputWindow.data)
        currFormula = currFormula.consume(SscheckTime(windowStartTimestamp))(currentLetter)
        logger.debug("Current formula result after window #{} is {}", windowIndex, currFormula.result)
        if (currFormula.result.isDefined || windowIndex+1 >= maxNumberLettersPerTestCase) break()
      }
    }

    logger.info(s"Completed evaluation for test case {} with result {}", testCaseId, currFormula.result)
    currFormula
  }

  /** Run the test case and get the result for the final formula.
    * */
  def computeResult(): Prop.Status = this.synchronized {
    result.getOrElse {
      val testCaseInputRecordPath = createTempDir()
      logger.info("Recording input for test case {} on path {}", testCaseId, testCaseInputRecordPath)
      val testCaseOutputRecordPath = createTempDir()
      logger.info("Recording output test case {} on path {}", testCaseId, testCaseOutputRecordPath)

      exerciseTestCase(testCaseInputRecordPath, testCaseOutputRecordPath)
      val finalFormula = evaluateTestCase(testCaseInputRecordPath, testCaseOutputRecordPath)

      result = Some(finalFormula.result.getOrElse(Prop.Undecided))
      result.get
    }
  }
}
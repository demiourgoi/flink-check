package es.ucm.fdi.sscheck.matcher.specs2 {
  import org.apache.flink.api.common.operators.Order
  import org.apache.flink.api.common.typeinfo.TypeInformation
  import org.apache.flink.api.scala._
  import org.apache.flink.util.Collector
  import org.specs2.matcher.Matcher
  import org.specs2.matcher.MatchersImplicits._
  import scala.reflect.ClassTag

  package object flink {

    implicit class FlinkCheckDataSet[T : TypeInformation : ClassTag : Ordering]
      (@transient self: DataSet[T]) extends Serializable {

      /** returns data set with elements in self that are not present in other */
      def minus(other: DataSet[T]): DataSet[T] = minusWithInMemoryPartition(other)

      /* cannot be used in practice, and often leads to "java.lang.OutOfMemoryError: Java heap space"
        * */
      private[this] def minusCoGroup(other: DataSet[T]): DataSet[T] = {
        // based on https://stackoverflow.com/questions/38737194/apache-flink-dataset-difference-subtraction-operation
        self.coGroup(other)
          .where("*")
          .equalTo("*") { (selfVals, otherVals, out: Collector[T]) =>
            val otherValsSet = otherVals.toSet
            selfVals
              .filterNot(otherValsSet.contains)
              .foreach(out.collect)
          }
      }

      /* Computes self minus other using a left outer join.
         This is very slow for local execution environment
       */
      private[this] def minusWithLeftOuterJoin(other: DataSet[T]): DataSet[T] = {
        self.leftOuterJoin(other).where("*").equalTo("*") {
          (thisData, otherData) =>
            if (otherData == null) List(thisData)
            else Nil
        }.flatMap { x => x }
      }

      /* Attempt to compute self minus other using sortPartition. The idea is the following, where
      other is the potential superset:

      - Join the 2 datasets with a transformation such that when ordered
        - we can distinguish occurrences of an element in `data` and `other`
        - after sorting, each element that is both in `other` and `data` appears with an other mark one position to
          the left of the appearance with the `data` mark. When an element is repeated, then occurrences with the same
          mark are together
      - Once sorted we can do a single traversal where occurrences of `other` elements "delete"
        occurrences of the same element with the `data` mark. Also, we delete all `other` occurrences. So if there
        is no element after that then `other` is a superset because it has deleted all elements in `data`

      This is very slow for local execution environment, and leads to errors as follows:

      ```
      19/07/21 18:32:22 ERROR RpcResultPartitionConsumableNotifier: Could not schedule or update consumers at the JobManager.
      Caused by: org.apache.flink.runtime.executiongraph.ExecutionGraphException: Cannot find execution for execution Id ae174e3b992638ef923573d6934024cb.
      ```
      * */
      private[this] def minussWithSortPartition(other: DataSet[T]): DataSet[T] = {
        val selfMarked: DataSet[(T, Boolean)] = self.map((_, true))
        val otherMarked: DataSet[(T, Boolean)] = other.map((_, false))

        // Range vs Hash partitioning in Spark: https://www.edureka.co/blog/demystifying-partitioning-in-spark
        // https://stackoverflow.com/questions/34071445/global-sorting-in-apache-flink/34073087
        val all = selfMarked.union(otherMarked)
          .partitionByHash(0) // so occurrences of the same value in both datasets go to the same partition
          .sortPartition[(T, Boolean)](identity, Order.ASCENDING)
        all.mapPartition[T] { (partitionIter: Iterator[(T, Boolean)], collector: Collector[T]) =>
          var latestOtherOpt: Option[T] = None
          partitionIter.foreach {
            case (otherElem, false) => latestOtherOpt = Some(otherElem)
            case (selfElem, true) =>
              if (latestOtherOpt != Some(selfElem)) collector.collect(selfElem)
          }
        }
      }

      /* Computes self minus other using in memory sorting by partition. This is the same idea as
      minussWithSortPartition but sorting by partition in memory, instead of using `sortPartition`

      This is fast enough for local execution environment, it also works by partition so it might work for distributed
      mode, although it loads the whole partition in memory so it's probably not too good.
      * */
      private[this] def minusWithInMemoryPartition(other: DataSet[T]): DataSet[T] = {
        val selfMarked: DataSet[(T, Boolean)] = self.map((_, true))
        val otherMarked: DataSet[(T, Boolean)] = other.map((_, false))
        val all = selfMarked.union(otherMarked)
          .partitionByHash(0) // so occurrences of the same value in both datasets go to the same partition
        all.mapPartition[T] { (partitionIter: Iterator[(T, Boolean)], collector: Collector[T]) =>
          val sortedPartition = {
            val partition = partitionIter.toArray
            util.Sorting.quickSort(partition)
            partition
          }
          var latestOtherOpt: Option[T] = None
          sortedPartition.foreach {
            case (otherElem, false) => latestOtherOpt = Some(otherElem)
            case (selfElem, true) =>
              if (latestOtherOpt != Some(selfElem)) collector.collect(selfElem)
          }
        }
      }
    }
  }

  package flink {
    object DataSetMatchers {
      /** Number of records to show on failing predicates */
      private val numErrors = 4

      def foreachElementProjection[T, P](projection: T => P)
                                        (predicate: P => Boolean): Matcher[DataSet[T]] = { (data: DataSet[T]) =>
        val failingElements = data.filter{x => ! predicate(projection(x))}.first(numErrors)
        (
          failingElements.count() == 0,
          "all elements fulfil the predicate",
          s"predicate failed for elements ${failingElements.collect().mkString(", ")} ..."
        )
      }

      /** @return a matcher that checks whether predicate holds for all the elements of
        *         a DataSet or not. Doesn't need to be used with TimedValue datasets, but on
        *         a formula will probably be used to have access to the timestamp */
      def foreachElement[T](predicate: T => Boolean): Matcher[DataSet[T]] =
        foreachElementProjection(identity[T])(predicate)

      /** This variant of foreachElement can be useful if we have serialization issues with closures capturing
        * too much */
      def foreachElement[T,C](predicateContext: C)(toPredicate: C => (T => Boolean)): Matcher[DataSet[T]] = {
        val predicate = toPredicate(predicateContext)
        foreachElement(predicate)
      }

      def existsElementProjection[T, P](projection: T => P)
                                       (predicate: P => Boolean): Matcher[DataSet[T]] = { (data: DataSet[T]) =>
        val exampleElements = data.filter{x => predicate(projection(x))}.first(1)
        (
          exampleElements.count() > 0,
          "some element fulfils the predicate",
          "predicate failed for all elements"
        )
      }

      /** @return a matcher that checks whether predicate holds for at least one of the elements of
        *         a DataSet or not. Doesn't need to be used with TimedValue datasets, but on
        *         a formula will probably be used to have access to the timestamp */
      def existsElement[T](predicate: T => Boolean): Matcher[DataSet[T]] =
        existsElementProjection(identity[T])(predicate)

      /** This variant of existsTimedElement can be useful if we have serialization issues with closures capturing
        * too much */
      def existsElement[T,C](predicateContext: C)(toPredicate: C => (T => Boolean)): Matcher[DataSet[T]] = {
        val predicate = toPredicate(predicateContext)
        existsElement(predicate)
      }

      def beEmptyDataSet[T](): Matcher[DataSet[T]] = {
        foreachElement(Function.const(false))
      }


      def beSubDataSetOf[T : TypeInformation : ClassTag : Ordering](other: DataSet[T]): Matcher[DataSet[T]] = {
        (data: DataSet[T]) =>

        val failingElements = new FlinkCheckDataSet(data).minus(other).first(numErrors)

        (
          failingElements.count() == 0,
          "this data set is contained on the other",
          s"these elements of the data set are not contained in the other ${failingElements.collect().mkString(", ")} ..."
        )
      }

    }
  }
}
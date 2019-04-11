package es.ucm.fdi.sscheck.matcher.specs2 {
  import org.apache.flink.api.common.typeinfo.TypeInformation
  import org.apache.flink.api.scala._
  import org.apache.flink.util.Collector
  import org.specs2.matcher.Matcher
  import org.specs2.matcher.MatchersImplicits._
  import scala.reflect.ClassTag

  package object flink {

    implicit class FlinkCheckDataSet[T : TypeInformation](self: DataSet[T]) {
      /** returns data set with elements in xs that are not present in ys
        * NOTE: cannot be used in practice, and often leads to "java.lang.OutOfMemoryError: Java heap space"
        * */
      def minus(other: DataSet[T])(implicit ev: ClassTag[T]): DataSet[T] = {
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

      /** NOTE: This is too slow to be used in practice */
      def beSubDataSetOf[T : TypeInformation : ClassTag](other: DataSet[T]): Matcher[DataSet[T]] = { (data: DataSet[T]) =>
        // TOO slow
        // val failingElements = new FlinkCheckDataSet(data).minus(other).first(numErrors)
        // Still too slow
        val failingElements =
          data.leftOuterJoin(other).where("*").equalTo("*"){
            (thisData, otherData) =>
              if (otherData == null) List(thisData)
              else Nil
          }.flatMap{x => x}.first(numErrors)
        (
          failingElements.count() == 0,
          "this data set is contained on the other",
          s"these elements of the data set are not contained in the other ${failingElements.collect().mkString(", ")} ..."
        )
      }

      // TODO implement Flinks version of sscheck for Spark beEqualAsSetTo based on
      // https://stackoverflow.com/questions/38737194/apache-flink-dataset-difference-subtraction-operationw
    }
  }
}
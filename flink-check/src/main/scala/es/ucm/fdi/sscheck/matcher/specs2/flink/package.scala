package es.ucm.fdi.sscheck.matcher.specs2 {

  package flink {

    import es.ucm.fdi.sscheck.prop.tl.flink.TimedElement
    import org.apache.flink.api.scala._
    import org.specs2.matcher.Matcher
    import org.specs2.matcher.MatchersImplicits._

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

      // TODO implement Flinks version of sscheck for Spark beEqualAsSetTo based on
      // https://stackoverflow.com/questions/38737194/apache-flink-dataset-difference-subtraction-operationw
    }
  }
}
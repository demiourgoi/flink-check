package es.ucm.fdi.sscheck.matcher.specs2 {

  package flink {

    import es.ucm.fdi.sscheck.flink.TimedValue
    import org.apache.flink.api.scala._
    import org.specs2.matcher.Matcher
    import org.specs2.matcher.MatchersImplicits._

    object DataSetMatchers {
      /** Number of records to show on failing predicates */
      private val numErrors = 4

      def foreachElementProjection[T, P](projection: T => P)
                                        (predicate: P => Boolean): Matcher[DataSet[T]] = { (data: DataSet[T]) =>
        val failingElements = data.filter{x => ! predicate(projection(x))}
        (
          failingElements.count() == 0,
          "all elements fulfil the predicate",
          s"predicate failed for elements ${failingElements.first(numErrors).collect().mkString(", ")} ..."
        )
      }

      /** @return a matcher that checks whether predicate holds for all the elements of a DataSet or not. */
      def foreachElement[T](predicate: T => Boolean): Matcher[DataSet[TimedValue[T]]] =
        foreachElementProjection[TimedValue[T], T](_.value)(predicate)

      /** This variant of foreachElement can be useful if we have serialization issues with closures capturing
        * too much */
      // Based on https://erikerlandson.github.io/blog/2015/03/31/hygienic-closures-for-scala-function-serialization/
      def foreachElement[T, C](predicateContext: C)(toPredicate: C => (T => Boolean)): Matcher[DataSet[TimedValue[T]]] = {
        val predicate = toPredicate(predicateContext)
        foreachElement(predicate)
      }

      /** @return a matcher that checks whether predicate holds for all the elements of
        *         a DataSet or not. Doesn't need to be used with TimedValue datasets, but on
        *         a formula will probably be used to have access to the timestamp */
      def foreachTimedElement[T](predicate: T => Boolean): Matcher[DataSet[T]] =
        foreachElementProjection(identity[T])(predicate)

      /** This variant of foreachTimedElement can be useful if we have serialization issues with closures capturing
        * too much */
      def foreachTimedElement[T,C](predicateContext: C)(toPredicate: C => (T => Boolean)): Matcher[DataSet[T]] = {
        val predicate = toPredicate(predicateContext)
        foreachTimedElement(predicate)
      }

      def existsElementProjection[T, P](projection: T => P)
                                       (predicate: P => Boolean): Matcher[DataSet[T]] = { (data: DataSet[T]) =>
        val exampleElements = data.filter{x => predicate(projection(x))}
        (
          exampleElements.count() > 0,
          "some element fulfils the predicate",
          "predicate failed for all elements"
        )
      }

      /** @return a matcher that checks whether predicate holds for at least one of the elements of a DataSet or not.*/
      def existsElement[T](predicate: T => Boolean): Matcher[DataSet[TimedValue[T]]] =
        existsElementProjection[TimedValue[T], T](_.value)(predicate)

      /** This variant of existsElement can be useful if we have serialization issues with closures capturing
        * too much */
      def existsElement[T,C](predicateContext: C)(toPredicate: C => (T => Boolean)): Matcher[DataSet[TimedValue[T]]] = {
        val predicate = toPredicate(predicateContext)
        existsElement(predicate)
      }

      /** @return a matcher that checks whether predicate holds for at least one of the elements of
        *         a DataSet or not. Doesn't need to be used with TimedValue datasets, but on
        *         a formula will probably be used to have access to the timestamp */
      def existsTimedElement[T](predicate: T => Boolean): Matcher[DataSet[T]] =
        existsElementProjection(identity[T])(predicate)

      /** This variant of existsTimedElement can be useful if we have serialization issues with closures capturing
        * too much */
      def existsTimedElement[T,C](predicateContext: C)(toPredicate: C => (T => Boolean)): Matcher[DataSet[T]] = {
        val predicate = toPredicate(predicateContext)
        existsTimedElement(predicate)
      }

      // TODO implement Flinks version of sscheck for Spark beEqualAsSetTo based on
      // https://stackoverflow.com/questions/38737194/apache-flink-dataset-difference-subtraction-operationw
    }
  }
}
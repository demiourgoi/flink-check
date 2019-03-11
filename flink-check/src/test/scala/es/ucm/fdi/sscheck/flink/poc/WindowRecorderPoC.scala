package es.ucm.fdi.sscheck.flink.poc

import org.junit.runner.RunWith
import org.specs2.Specification
import org.specs2.matcher.{ResultMatchers, ThrownExpectations}
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WindowRecorderPoC
  extends Specification with ResultMatchers with ThrownExpectations {

  def is =
    s2"""
        where $foo
      """

  def foo = {
    ok
  }
}

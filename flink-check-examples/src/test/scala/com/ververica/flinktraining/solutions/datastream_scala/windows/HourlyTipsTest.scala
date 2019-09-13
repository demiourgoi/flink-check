/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_scala.windows

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.{ScalaCheck, Specification}
import org.scalacheck.Gen
import org.scalacheck.Arbitrary._
import org.joda.time.DateTime
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.streaming.api.windowing.time.Time
import es.ucm.fdi.sscheck.gen._
import es.ucm.fdi.sscheck.gen.flink.FlinkGenerators._
import es.ucm.fdi.sscheck.prop.tl.Formula._
import es.ucm.fdi.sscheck.prop.tl.flink.DataStreamTLProperty
import es.ucm.fdi.sscheck.gen.PStreamGenConversions._
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare
import com.ververica.flinktraining.solutions.datastream_java.windows._
import es.ucm.fdi.sscheck.matcher.specs2.flink.DataSetMatchers._
import es.ucm.fdi.sscheck.prop.tl.flink.FlinkFormula._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

object TaxiFareGen {
  // Note we'll use the field assigner TaxiFareFieldAssigner to set startTime, as that
  // is the field that determines the event time
  def fare(driverIdGen: Gen[Long] = arbitrary[Long],
           tipGen: Gen[Float] = Gen.posNum[Float]): Gen[TaxiFare] =
    for {
      driverId <- driverIdGen
      tip <- tipGen
    } yield new TaxiFare(0, 0, driverId, new DateTime(0), "", tip, 0F, 0F);

  def eventTimeFieldAssigner(eventTime: Long)(fare: TaxiFare): TaxiFare = {
    fare.startTime = new DateTime(eventTime)
    fare
  }
}

@RunWith(classOf[JUnitRunner])
class HourlyTipsTest
  extends Specification with ScalaCheck with DataStreamTLProperty {

  def is =
    s2"""
      A specification for the HourlyTips example:
        - where if we have $oneDriverWithOneTip_Then_onlyCountThatTip
      """

  type TipCount = Tuple3[java.lang.Long, java.lang.Long, java.lang.Float]

  def oneDriverWithOneTip_Then_onlyCountThatTip = {
    type U = DataStreamTLProperty.Letter[TaxiFare, TipCount]

    val windowSize = Time.minutes(15)
    val numWindows = 4
    val driverId = 42
    val gen = eventTimeToFieldAssigner(TaxiFareGen.eventTimeFieldAssigner){
      tumblingTimeWindows(windowSize) {
        val eventuallyOneTaxi = WindowGen.eventually(
          WindowGen.ofN(1, TaxiFareGen.fare(driverIdGen=Gen.const(driverId))),
          numWindows-1)
        eventuallyOneTaxi ++ WindowGen.laterN(numWindows, WindowGen.ofNtoM(1, 10, TaxiFareGen.fare()))
      }
    }

    val formula = eventuallyR[U]{ case (_, hourlyMax) =>
      hourlyMax.count() === 1 and
        (hourlyMax should foreachElement(driverId){dId => elem => elem.value.f1 == dId })
    } on numWindows groupBy TumblingTimeWindows(windowSize)

    forAllDataStream[TaxiFare, TipCount](
      gen)(
      in =>  new DataStream(HourlyTipsSolution.getHourlyMax(in.javaStream))
    )(formula)
  }.set(minTestsOk = 12, workers = 3).verbose

  // safety: always only 1 max

}


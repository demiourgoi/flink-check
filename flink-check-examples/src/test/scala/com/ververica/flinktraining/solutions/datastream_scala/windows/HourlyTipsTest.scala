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
import es.ucm.fdi.sscheck.gen.BatchGenConversions._
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
    } yield {
      val fare = new TaxiFare()
      fare.driverId = driverId
      fare.tip = tip
      fare
    }

  def eventTimeFieldAssigner(eventTime: Long)(fare: TaxiFare): TaxiFare = {
    fare.startTime = new DateTime(eventTime)
    fare
  }
}

@RunWith(classOf[JUnitRunner])
class HourlyTipsTest
  extends Specification with ScalaCheck  with DataStreamTLProperty with Serializable {

  def is = sequential ^ s2"""
      A specification for the HourlyTips example:
        - where if we have just one driver with one tip then we only count that tip $oneDriverWithOneTip_Then_onlyCountThatTip
        - where tips are correctly summed by hour ${tipsAreSummedByHour(6)}
        - where we always get a single value for the hourly max tip ${alwaysOnlyOneTopHourlyMax(12)}
        - where two drivers correctly alternate as the top tip receiver $driverOneTopUntilDriverTwoTop
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
      in => new DataStream(HourlyTipsSolution.getHourlyMax(in.javaStream))
    )(formula)
  }.set(minTestsOk = 9, workers = 3).verbose

  // getTipsPerHourAndDriver: sum ok iff always the 1 window sum based on driver id is as expected
  def tipsAreSummedByHour(genWindowFactor: Int) = {
    type U = DataStreamTLProperty.Letter[TaxiFare, TipCount]

    val checkWindowSize = Time.hours(1)
    val genWindowSize = Time.milliseconds(checkWindowSize.toMilliseconds / genWindowFactor)
    val numGenWindows = genWindowFactor*4 + 1
    val fareFactor = 10
    val fares = (1 to 10).map{ driverId =>
      val fare = new TaxiFare()
      fare.driverId = driverId
      fare.tip = driverId * fareFactor
      fare
    }
    val gen = eventTimeToFieldAssigner(TaxiFareGen.eventTimeFieldAssigner){
      tumblingTimeWindows(genWindowSize){
        WindowGen.always(Gen.const(fares), numGenWindows)
      }
    }

    val formula = alwaysR[U]{ case (fares, hourlyTips) =>
      hourlyTips should foreachElement{ elem =>
        val tips = elem.value
        tips.f2 === tips.f1 * fareFactor * genWindowFactor
      }
    } during numGenWindows/genWindowFactor groupBy TumblingTimeWindows(checkWindowSize)

    forAllDataStream[TaxiFare, TipCount](
      gen)(
      in => new DataStream(HourlyTipsSolution.getTipsPerHourAndDriver(in.javaStream))
    )(formula)
  }.set(minTestsOk = 9, workers = 3).verbose

  // safety: always only 1 max
  def alwaysOnlyOneTopHourlyMax(genWindowFactor: Int) = {
    type U = DataStreamTLProperty.Letter[TaxiFare, TipCount]

    val checkWindowSize = Time.hours(1)
    val genWindowSize = Time.milliseconds(checkWindowSize.toMilliseconds / genWindowFactor)
    val numWindows = 5 * genWindowFactor
    val gen = eventTimeToFieldAssigner(TaxiFareGen.eventTimeFieldAssigner) {
      tumblingTimeWindows(genWindowSize) {
        WindowGen.always(
          WindowGen.ofNtoM(1, 50, TaxiFareGen.fare()),
          numWindows
        )
      }
    }

    val formula = alwaysR[U]{ case (_, hourlyMax) =>
      hourlyMax.count() === 1
    } during numWindows/genWindowFactor groupBy TumblingTimeWindows(checkWindowSize)

    forAllDataStream[TaxiFare, TipCount](
      gen)(
      in => new DataStream(HourlyTipsSolution.getHourlyMax(in.javaStream))
    )(formula)
    }.set(minTestsOk = 9, workers = 3).verbose

  // testMaxAcrossDrivers with until
  def driverOneTopUntilDriverTwoTop = {
    type U = DataStreamTLProperty.Letter[TaxiFare, TipCount]
    val topHourlyTips = (_ : U)._2

    val windowSize = Time.hours(1)
    val timeout = 6
    def driverTop(driverId: Long): Gen[Window[TaxiFare]] =
      WindowGen.ofN(5, TaxiFareGen.fare(driverIdGen=Gen.const(driverId))) +
        WindowGen.ofN(2, TaxiFareGen.fare(driverIdGen=Gen.const(2-driverId+1)))
    val gen = eventTimeToFieldAssigner(TaxiFareGen.eventTimeFieldAssigner) {
      tumblingTimeWindows(windowSize) {
        WindowGen.until(driverTop(1), driverTop(2), timeout)
      }
    }

    val formula =
      { at(topHourlyTips){_ should foreachElement(_.value.f1 == 1)} } until {
        at(topHourlyTips){_ should foreachElement(_.value.f1 == 2)}
      } on timeout groupBy TumblingTimeWindows(windowSize)

    forAllDataStream[TaxiFare, TipCount](
      gen)(
      in => new DataStream(HourlyTipsSolution.getHourlyMax(in.javaStream))
    )(formula)
  }.set(minTestsOk = 9, workers = 3).verbose
}


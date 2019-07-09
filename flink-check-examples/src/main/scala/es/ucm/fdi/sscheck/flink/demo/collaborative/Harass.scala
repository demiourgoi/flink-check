package es.ucm.fdi.sscheck.flink.demo.collaborative

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object Harass {
  
  // Compute the danger level of every zone based on maximum values
  def harass_max(raw : DataStream[Incident]) : DataStream[(Int, DangerLevel.DangerLevel)] =
    raw.keyBy("zone_id")
       .timeWindow(Time.hours(1))
       .max("danger")
       .map { x => (x.zone_id, DangerLevel(x.danger)) }

  // Enumeration to represent danger levels and the translation of perceived
  // danger to danger level
  object DangerLevel extends Enumeration {
    type DangerLevel = Value
    val Safe, Warning, Danger, Extreme = Value

    def apply(danger : Double) = danger match {
      case x if x <= 1.0 => Safe
      case x if (x > 1.0) && (x <= 5.0) => Warning
      case x if (x > 5.0) && (x <= 8.0) => Danger
      case _ => Extreme
    }
  }
    
  // Class for representing one harassment incident
  // Each incident contains an identifier of the zone where it happens and a
  // value of perceived danger between 0 (safe) and 10 (extremely dangerous)
  case class Incident(zone_id: Int, danger: Double)
}

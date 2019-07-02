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
  /*def main(args: Array[String]): Unit = {
    println("----------------")
    println("-- pollution1 --")
    println("----------------")
    runner[(Int,EmergencyLevel.EmergencyLevel)]("/sensor1.data", pollution1)
      
    println("----------------")
    println("-- pollution2 --")
    println("----------------")
    runner[(Long,EmergencyLevel.EmergencyLevel)]("/sensor2.data", pollution2)
    
    println("----------------")
    println("-- pollution3 --")
    println("----------------")
    runner[(Long,EmergencyLevel.EmergencyLevel,List[Int])]("/sensor3.data", pollution3)
  }

  
  def runner[T](path : String, fun : DataStream[SensorData] => DataStream[T]) : Unit = {
    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate event time

    // get input data and extract timestamps
    val data : DataStream[SensorData] = env.readTextFile(getClass.getResource(path).getPath)
      .map(parsePair(_))
      .assignAscendingTimestamps( _.timestamp )

    val counts : DataStream[T] = fun(data)

    // execute and print result
    counts.print().setParallelism(1)
    env.execute("Pollution example")
  }*/
  
  
  // Compute de danger level of every zone based on maximum values
  def harass_max(raw : DataStream[HarassmentIncident]) : DataStream[(Int, DangerLevel.DangerLevel)] =
    raw.keyBy("zone_id")
       .timeWindow(Time.seconds(1))
       .max("perceived_danger")
       .map { x => (x.zone_id, DangerLevel(x.perceived_danger)) }

  /*
  // Detect global emergency level counting the number of sensors exceeding the limits     
  def pollution2(raw : DataStream[SensorData]) : DataStream[(Long,EmergencyLevel.EmergencyLevel)] = {
    raw.timeWindowAll(Time.seconds(3), Time.seconds(1))
       .apply( (w,v,o) => avg_concentration(w,v,o) )
  }
  
  // Detect global emergency level counting the number of sensors exceeding the limits
  // The level of each sensor is computed every time step as the average value
  // def pollution3(raw : DataStream[SensorData]) : DataStream[(Int, EmergencyLevel.EmergencyLevel)] =
  def pollution3(raw : DataStream[SensorData]) : DataStream[(Long,EmergencyLevel.EmergencyLevel,List[Int])] = {
    raw.map( x => AggSensorData(x.timestamp, x.sensor_id, x.concentration, 1))
       .keyBy("sensor_id")
       .timeWindow(Time.seconds(1))
       .reduce( (a, b) => AggSensorData(Math.max(a.timestamp, b.timestamp), 
                                        a.sensor_id, 
                                        a.concentration_sum + b.concentration_sum, 
                                        a.num_measurements + b.num_measurements) )
       .map( x => SensorData(x.timestamp, x.sensor_id, x.concentration_sum / x.num_measurements) )
       .timeWindowAll(Time.seconds(3), Time.seconds(1))
       .apply( (w,v,o) => compute_level(w,v,o) )
  }

  
  // Each line contains one sensor value in CSV: timestamp, sensor_id, concentration
  def parsePair(s : String) : SensorData = {
    val parts = s.split(",")
    SensorData(parts(0).toLong, parts(1).toInt, parts(2).toDouble)
  }
  
  /* 
   * Function to select the emergency level based on the concentration 
   * registered by each sensor in three consecutive time slots
   * 
   * If 3 or more sensors have a concentration > 180 in 3 consecutive slots => Notice
   * If 3 or more sensors have a concentration > 200 in 3 consecutive slots => Warning
   * If 3 or more sensors have a concentration > 400 in 3 consecutive slots => Alert
   */
  def compute_level(window : TimeWindow, values : Iterable[SensorData], out: Collector[(Long,EmergencyLevel.EmergencyLevel,List[Int])]) : Unit = {
    var map = scala.collection.mutable.Map[Int, List[Double]]()
    for (s <- values) {
      val p : List[Double] = map.getOrElse(s.sensor_id, Nil)
      map.put(s.sensor_id, s.concentration :: p)
    }
    var gt180 : List[Int] = Nil
    var gt200 : List[Int] = Nil
    var gt400 : List[Int] = Nil
    for ((sensor_id,l) <- map) {
      if (l.min > 180 && l.length >= 3)  gt180 = sensor_id :: gt180
      if (l.min > 200 && l.length >= 3)  gt200 = sensor_id :: gt200
      if (l.min > 400 && l.length >= 3)  gt400 = sensor_id :: gt400
    }
    val (level, sensors) : (EmergencyLevel.EmergencyLevel, List[Int]) = selectLevelList(gt180, gt200, gt400)
    out.collect((window.getEnd(), level, sensors))
  }
  
  // Generate the emergency level from the number of sensors exceeding limits,
  // as well as the related sensor_ids
  def selectLevelList(gt180 : List[Int], gt200 : List[Int], gt400 : List[Int]) : (EmergencyLevel.EmergencyLevel, List[Int]) = {
    if (gt400.length >= 3) (EmergencyLevel.Alert, gt400)
    else if (gt200.length >= 3) (EmergencyLevel.Warning, gt200)
    else if (gt180.length >= 3) (EmergencyLevel.Notice, gt180)
    else (EmergencyLevel.OK, Nil)
  }
  
  /* 
   * Function to select the emergency level based on the average concentration 
   * of the sensors in a window. The generated pair contains the timestamp 
   * when the level is computed
   * 
   * If 3 or more sensors have a average concentration > 180 => Notice
   * If 3 or more sensors have a average concentration > 200 => Warning
   * If 3 or more sensors have a average concentration > 400 => Alert
   */
  def avg_concentration(window : TimeWindow, values : Iterable[SensorData], out: Collector[(Long,EmergencyLevel.EmergencyLevel)]) : Unit = {
    var map = scala.collection.mutable.Map[Int, (Double,Int)]()
    for (s <- values) {
      val p : (Double,Int) = map.getOrElse(s.sensor_id, (0.0, 0))
      map.put(s.sensor_id, (p._1 + s.concentration, p._2 + 1))
    }
    var gt180 : Int = 0
    var gt200 : Int = 0
    var gt400 : Int = 0
    for ((_,(sum,n)) <- map) {
      val avg_c : Double = sum/n
      if (avg_c > 180)  gt180 += 1
      if (avg_c > 200)  gt200 += 1
      if (avg_c > 400)  gt400 += 1
    }
    val level : EmergencyLevel.EmergencyLevel = selectLevel(gt180, gt200, gt400)
    out.collect((window.getEnd(), level))
  }
  */
  

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
  case class HarassmentIncident(zone_id: Int, perceived_danger: Double)
  
  
  // Class for representing aggregate sensor datum
  //case class AggSensorData(timestamp: Long, sensor_id: Int, concentration_sum: Double, num_measurements : Int)  
}

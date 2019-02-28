package es.ucm.fdi.sscheck.flink.demo.pollution

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object Pollution {
  def main(args: Array[String]): Unit = {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate event time

    // get input data and extract timestamps
    val data : DataStream[SensorData] = env.readTextFile(getClass.getResource("/sensor.data").getPath)
      .map(parsePair(_))
      .assignAscendingTimestamps( _.timestamp )
    
    //val counts : DataStream[(Int, EmergencyLevel.EmergencyLevel)] = pollution1(data)
    val counts : DataStream[(Long, EmergencyLevel.EmergencyLevel)] = pollution2(data)

    // execute and print result
    counts.print().setParallelism(1)
    
    env.execute("Pollution example")
  }
  
  
  // Detect emergency level of every sensor based on maximum values
  def pollution1(raw : DataStream[SensorData]) : DataStream[(Int, EmergencyLevel.EmergencyLevel)] =
    raw.filter(_.concentration > 180)
       .keyBy("sensor_id")
       .timeWindow(Time.seconds(3), Time.seconds(1))
       .max("concentration")
       .map { x => (x.sensor_id, EmergencyLevel(x.concentration)) }

  // Detect global emergency level counting the number of sensors exceeding the limits     
  def pollution2(raw : DataStream[SensorData]) : DataStream[(Long,EmergencyLevel.EmergencyLevel)] = {
    raw.timeWindowAll(Time.seconds(3), Time.seconds(1))
       .apply( (w,v,o) => avg_concentration(w,v,o) )
  }       
    
  
  // Each line contains one sensor value in CSV: timestamp, sensor_id, concentration
  def parsePair(s : String) : SensorData = {
    val parts = s.split(",")
    SensorData(parts(0).toLong, parts(1).toInt, parts(2).toDouble)
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
  
  // Generate the emergency level from the number of sensors exceeding limits
  def selectLevel(gt180 : Int, gt200 : Int, gt400 : Int) : EmergencyLevel.EmergencyLevel = {
    if (gt400 >= 3) EmergencyLevel.Alert
    else if (gt200 >= 3) EmergencyLevel.Warning
    else if (gt180 >= 3) EmergencyLevel.Notice
    else EmergencyLevel.OK
  }
 
  // Enumeration to represent emergency levels
  object EmergencyLevel extends Enumeration {
    type EmergencyLevel = Value
    val Alert, Warning, Notice, OK = Value

    def apply(pol : Double) = pol match {
      case x if x > 400.0 => Alert
      case x if (x > 200.0) && (x < 400.0) => Warning
      case x if (x > 180.0) && (x <= 200.0) => Notice
      case _ => OK
    }
  }
    
  // Class for representing one sensor datum
  case class SensorData(timestamp: Long, sensor_id: Int, concentration: Double)
  
}

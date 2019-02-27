package es.ucm.fdi.sscheck.flink.demo

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic


object Pollution {
  def main(args: Array[String]): Unit = {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); // activate event time

    // get input data
    val data : DataStream[SensorData] = env.readTextFile(getClass.getResource("/sensor.data").getPath)
      .map(parsePair(_))
      .assignAscendingTimestamps( _.timestamp )
    
    val counts : DataStream[(Int, String)] = pollution1(data)

    // execute and print result
    counts.print()
    
    env.execute("Pollution example")
  }
  
  
  def pollution1(raw : DataStream[SensorData]) : DataStream[(Int, String)] = 
    raw.filter(_.concentration > 180)
       .keyBy("sensor_id")
       .timeWindow(Time.seconds(3), Time.seconds(1))
       .max("concentration")
       .map { x => (x.sensor_id, emergencyLevel(x.concentration)) }
    
  
  // Each line contains one sensor value in CSV: timestamp, sensor_id, concentration
  def parsePair(s : String) : SensorData = {
    val parts = s.split(",")
    SensorData(parts(0).toLong, parts(1).toInt, parts(2).toDouble)
  }
 
  
  def emergencyLevel(pol : Double) : String = 
    pol match {
      case x if x > 400.0 => "Alert"
      case x if (x > 200.0) && (x < 400.0) => "Warning"
      case x if (x > 180.0) && (x <= 200.0) => "Notice"
      case x => "OK"
    }
    
  case class SensorData(timestamp: Long, sensor_id: Int, concentration: Double)
  
}

/*
 * Adapted from the example HourlyTipsSolution at
 * https://github.com/ververica/flink-training-exercises/blob/master/src/main/java/com/ververica/flinktraining/solutions/datastream_java/windows/HourlyTipsSolution.java
 *
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

package com.ververica.flinktraining.solutions.datastream_java.windows;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;


import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Java reference implementation for the "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 */
public class HourlyTipsSolution {
    /** Compute tips per hour for each driver
     *
     * Each output element should be interpreted as
     *      (window end timestamp i.e. hour id,
     *       window key i.e. driver id,
     *       sum of the tips for the fares in the window)
     * */
    public static DataStream<Tuple3<Long, Long, Float>> getTipsPerHourAndDriver(
            DataStream<TaxiFare> fares) {
        return fares
                .keyBy((TaxiFare fare) -> fare.driverId)
                .timeWindow(Time.hours(1))
                .process(new AddTips());
    }

    public static DataStream<Tuple3<Long, Long, Float>> getMaxHourlyTipsFromTipsPerHourAndDriver(
            DataStream<Tuple3<Long, Long, Float>> hourlyTips){
        return hourlyTips
                .timeWindowAll(Time.hours(1))
                .maxBy(2);
    }

    public static DataStream<Tuple3<Long, Long, Float>> getHourlyMax(DataStream<TaxiFare> fares) {
        return getMaxHourlyTipsFromTipsPerHourAndDriver(getTipsPerHourAndDriver(fares));
    }

    /*
     * Wraps the pre-aggregated result into a tuple along with the window's timestamp and key.
     */
    public static class AddTips extends ProcessWindowFunction<
            TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
        @Override
        public void process(Long key, Context context, Iterable<TaxiFare> fares,
                            Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            Float sumOfTips = 0F;
            for (TaxiFare f : fares) {
                sumOfTips += f.tip;
            }
            out.collect(new Tuple3<>(context.window().getEnd(), key, sumOfTips));
        }
    }
}

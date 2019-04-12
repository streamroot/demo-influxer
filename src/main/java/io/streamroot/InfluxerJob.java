package io.streamroot;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.primitives.Ints;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.influxdb.dto.Point;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class InfluxerJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.socketTextStream("netcat", Integer.parseInt(args[0]))
                .map(Ints::tryParse)
                .filter(Objects::nonNull)
                .timeWindowAll(Time.seconds(10))
                .reduce((d1, d2) -> d1 + d2,
                        (AllWindowFunction<Integer, Tuple2<Long, Integer>, TimeWindow>)
                        (window, res, out) -> out.collect(
                                Tuple2.of(window.getEnd(), res.iterator().next())))
                .filter(Objects::nonNull)
                .map(d -> Point.measurement("records")
                        .time(d.f0, TimeUnit.MILLISECONDS) // d.f0 = window.getEnd()
                        .addField("sum", d.f1)             // d.f1 = sum(data) within 10 sec
                        .tag("window", "10-sec")
                        .build())
                .addSink(InfluxSinkV4.builder()
                        .descriptorId("influx-sink")
                        .connUrl("http://influxdb:8086")
                        .user("root")
                        .password("root")
                        .database("data")
                        .batchSize(100)      // batch send every 100 points
                        .batchFreqMs(1000)   // or when 1 second has elapsed
                        .retryFreqMs(5000)
                        .build())
                .setParallelism(1);

		env.execute("influxer");
	}
}

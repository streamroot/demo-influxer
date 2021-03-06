package io.streamroot;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static io.streamroot.Utils.*;
import static org.junit.Assert.*;

public class InfluxSinkV1Test {

    private static InfluxDB influx;

    @BeforeClass
    public static void setup() {
        influx = makeInfluxConn();
        initInflux(influx);
    }

    @AfterClass
    public static void tearDown() {
        influx.close();
    }

    @Test
    public void testWritingData() {
        InfluxSinkV1 sink = new InfluxSinkV1("http://localhost:" + INFLUX_PORT, USER, PASSWORD, DB_DATA);
        sink.open(null);

        sink.invoke(Point.measurement("telemetry")
                .time(Instant.now().toEpochMilli(), TimeUnit.MILLISECONDS)
                .addField("temperature", 42)
                .tag("location", "Paris")
                .build(), null);

        QueryResult res = influx.query(new Query("SELECT * FROM telemetry", DB_DATA));
        QueryResult.Series series = res.getResults()
                .get(0)
                .getSeries()
                .stream()
                .filter(s -> s.getName().equals("telemetry"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No telemetry series"));

        Map<String, Object> data = zip(series.getColumns(), series.getValues().get(0));

        assertEquals("Paris", data.get("location"));
        assertEquals(42.0, data.get("temperature"));
        sink.close();
    }
}
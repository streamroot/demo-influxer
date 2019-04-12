package io.streamroot;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.netcrusher.core.reactor.NioReactor;
import org.netcrusher.tcp.TcpCrusher;
import org.netcrusher.tcp.TcpCrusherBuilder;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.streamroot.Utils.*;
import static org.junit.Assert.assertEquals;

public class InfluxSinkV3Test {

    private static InfluxDB influx;
    private static NioReactor reactor;
    private static TcpCrusher tcpCrusher;

    @BeforeClass
    public static void setup() throws Exception {
        influx = makeInfluxConn();
        initInflux(influx);

        reactor = new NioReactor(10);
        tcpCrusher = TcpCrusherBuilder.builder()
                .withReactor(reactor)
                .withBindAddress("localhost", PROXIED_PORT)
                .withConnectAddress("localhost", INFLUX_PORT)
                .buildAndOpen();
    }

    @AfterClass
    public static void tearDown() {
        tcpCrusher.close();
        reactor.close();
        influx.close();
    }

    @Test
    public void testWritingData() throws Exception {
        InfluxSinkV3 sink = new InfluxSinkV3("http://localhost:" + INFLUX_PORT, USER, PASSWORD, DB_DATA);
        sink.open(null);

        sink.invoke(Point.measurement("telemetry")
                .time(Instant.now().toEpochMilli(), TimeUnit.MILLISECONDS)
                .addField("temperature", 42)
                .tag("location", "Paris")
                .build(), null);

        Thread.sleep(250); // wait for batch to complete (a bit more than batchFreqMs)
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
    }

    @Test
    public void testResilience() throws Exception {
        InfluxSinkV3 sink = new InfluxSinkV3("http://localhost:" + PROXIED_PORT, USER, PASSWORD, DB_DATA);
        sink.open(null);

        // stop proxying to influx
        tcpCrusher.freeze();

        // try writing point
        Instant now = Instant.now();
        sink.invoke(Point.measurement("monitoring")
                .time(now.toEpochMilli(), TimeUnit.MILLISECONDS)
                .addField("warnings", 12)
                .tag("datacenter", "abc")
                .build(), null);
        Thread.sleep(150); // wait for batch to complete (a bit more than batchFreqMs)

        // simulate influx/network failure
        tcpCrusher.close();
        Thread.sleep(300);
        tcpCrusher.open();

        Thread.sleep(200); // wait for retry to complete (a bit more than batchFreqMs + retryFreqMs)
        QueryResult res = influx.query(new Query("SELECT * FROM monitoring", DB_DATA));
        QueryResult.Series series = res.getResults()
                .get(0)
                .getSeries()
                .stream()
                .filter(s -> s.getName().equals("monitoring"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No monitoring series"));

        Map<String, Object> data = zip(series.getColumns(), series.getValues().get(0));

        assertEquals("abc", data.get("datacenter"));
        assertEquals(12.0, data.get("warnings"));
        assertEquals(now, Instant.parse((String) data.get("time")));

        sink.close();
    }
}
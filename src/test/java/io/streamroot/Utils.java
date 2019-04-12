package io.streamroot;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Utils {

    public static final String USER = "root";
    public static final String PASSWORD = "root";
    public static final String DB_DATA = "data";
    public static final int INFLUX_PORT = 8086;
    public static final int PROXIED_PORT = 8085;

    public static InfluxDB makeInfluxConn() {
        return InfluxDBFactory.connect("http://localhost:" + INFLUX_PORT, USER, PASSWORD);
    }

    public static void initInflux(InfluxDB influx) {
        Set<Object> databases = influx.query(new Query("SHOW DATABASES"))
                .getResults()
                .stream()
                .flatMap(r -> r.getSeries().stream())
                .flatMap(s -> s.getValues().stream())
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        if (databases.contains(DB_DATA)) {
            influx.query(new Query("DROP DATABASE " + DB_DATA));
        }

        influx.query(new Query("CREATE DATABASE " + DB_DATA));
    }

    public static Map<String, Object> zip(List<String> keys, List<Object> vals) {
        assert keys.size() == vals.size();
        return IntStream.range(0, keys.size()).boxed()
                .collect(Collectors.toMap(keys::get, vals::get));
    }

    public static List<Map<String, Object>> zipAll(List<String> keys, List<List<Object>> series) {
        return series.stream()
                .map(vals -> zip(keys, vals))
                .collect(Collectors.toList());
    }
}

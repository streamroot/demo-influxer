package io.streamroot;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

public class InfluxSinkV2 extends RichSinkFunction<Point> {

    private static final String RETENTION_POLICY = "";

    private transient InfluxDB influx;

    private final String connUrl;
    private final String user;
    private final String password;
    private final String database;

    public InfluxSinkV2(String connUrl, String user, String password, String database) {
        this.connUrl = connUrl;
        this.user = user;
        this.password = password;
        this.database = database;
    }

    @Override
    public void open(Configuration parameters) {
        influx = InfluxDBFactory.connect(connUrl, user, password);
        influx.enableBatch(BatchOptions.DEFAULTS.flushDuration(100));
    }

    @Override
    public void invoke(Point point, Context context) {
        influx.write(database, RETENTION_POLICY, point);
    }

    @Override
    public void close() {
        influx.close();
    }
}

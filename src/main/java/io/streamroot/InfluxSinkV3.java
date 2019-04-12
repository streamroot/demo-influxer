package io.streamroot;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.function.CheckedSupplier;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.ResponseBody;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.impl.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.moshi.MoshiConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.POST;
import retrofit2.http.Query;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class InfluxSinkV3 extends RichSinkFunction<Point> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxSinkV3.class);
    private static final String RETENTION_POLICY = "";
    private static final MediaType MEDIA_TYPE_STRING = MediaType.parse("text/plain");

    private transient InfluxBatchService influx;
    private transient ListState<Point> checkpointedState;
    private transient ScheduledExecutorService scheduledExec;
    private transient RetryPolicy<Response<ResponseBody>> retryPolicy;

    private final String connUrl;
    private final String user;
    private final String password;
    private final String database;
    private final String descriptorId;
    private final int batchSize;
    private final int batchFreqMs;
    private final int retryFreqMs;
    private final List<Point> bufferedPoints = new ArrayList<>();
    private final AtomicLong retrying = new AtomicLong(0);

    public InfluxSinkV3(String connUrl, String user, String password, String database) {
        this.connUrl = connUrl;
        this.user = user;
        this.password = password;
        this.database = database;
        this.descriptorId = "dummy-descriptor";
        this.batchSize = 2000;
        this.batchFreqMs = 100;
        this.retryFreqMs = 25;
    }

    @Override
    public void open(Configuration parameters) {
        influx = makeBatchService(connUrl);
        retryPolicy = new RetryPolicy<Response<ResponseBody>>()
                .withMaxRetries(-1)
                .handle(IOException.class)
                .handleResultIf((Response<ResponseBody> r) -> {
                    if (!r.isSuccessful()) {
                        String errMessage = "";
                        try (ResponseBody errorBody = r.errorBody()) {
                            if (null != errorBody) errMessage = errorBody.string();
                        } catch (IOException e) {
                            LOG.error("Couldn't read response errorBody: ", e.getMessage());
                        }
                        LOG.error("Error {} from Influx: {}", r.code(), errMessage);
                        return true; // will retry
                    } else {
                        return false; // don't retry
                    }
                })
                .withDelay(Duration.ofMillis(retryFreqMs));
        scheduledExec = Executors.newSingleThreadScheduledExecutor();
        scheduledExec.scheduleAtFixedRate(this::flushPoints, 0, batchFreqMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void invoke(Point point, Context context) {
        bufferedPoints.add(point);
        if (bufferedPoints.size() == batchSize) {
            flushPoints();
        }
    }

    private synchronized void flushPoints() {
        if (bufferedPoints.size() > 0) {
            retrying.getAndSet(0);
            Failsafe.with(retryPolicy).get(batchWrite(bufferedPoints));
            if (retrying.get() > 1) {
                LOG.info("Batch successfully recovered");
            }
            bufferedPoints.clear();
        }
    }

    private CheckedSupplier<Response<ResponseBody>> batchWrite(Iterable<Point> points) {
        return () -> {
            long retryNb = retrying.getAndIncrement();
            if (retryNb > 0) {
                LOG.warn("Retrying batch (" + retryNb + ")");
            }
            return influx.writePoints(
                    user, password, database,
                    RETENTION_POLICY,
                    TimeUtil.toTimePrecision(TimeUnit.NANOSECONDS),
                    InfluxDB.ConsistencyLevel.ONE.value(),
                    RequestBody.create(
                            MEDIA_TYPE_STRING,
                            StreamSupport.stream(points.spliterator(), false)
                                    .map(Point::lineProtocol)
                                    .collect(Collectors.joining("\n"))))
                    .execute();
        };
    }

    @Override
    public synchronized void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Point point : bufferedPoints) {
            checkpointedState.add(point);
        }
    }

    @Override
    public synchronized void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Point> descriptor = new ListStateDescriptor<>(
                descriptorId, TypeInformation.of(Point.class));
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            for (Point point : checkpointedState.get()) {
                bufferedPoints.add(point);
            }
        }
    }

    @Override
    public void close() {
        boolean isStopped = false;
        try {
            isStopped = scheduledExec.awaitTermination(batchFreqMs, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            // slurp
        } finally {
            if(!isStopped) {
                scheduledExec.shutdownNow();
            }
        }
    }

    private static InfluxBatchService makeBatchService(String url) {
        return new Retrofit.Builder()
                .baseUrl(url)
                .client(new OkHttpClient.Builder().build())
                .addConverterFactory(MoshiConverterFactory.create())
                .build()
                .create(InfluxBatchService.class);
    }

    public interface InfluxBatchService {
        String U = "u";
        String P = "p";
        String DB = "db";
        String RP = "rp";
        String PRECISION = "precision";
        String CONSISTENCY = "consistency";

        @POST("/write")
        Call<ResponseBody> writePoints(
                @Query(U) String username,
                @Query(P) String password,
                @Query(DB) String database,
                @Query(RP) String retentionPolicy,
                @Query(PRECISION) String precision,
                @Query(CONSISTENCY) String consistency,
                @Body RequestBody batchPoints);
    }
}

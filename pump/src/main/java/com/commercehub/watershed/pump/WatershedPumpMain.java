package com.commercehub.watershed.pump;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.davidmoten.rx.jdbc.ConnectionProvider;
import com.github.davidmoten.rx.jdbc.Database;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author pmogren
 */
public class WatershedPumpMain {
    private static final Logger log = LoggerFactory.getLogger(WatershedPumpMain.class);
    private static final int RECORDS_PER_CHUNK = 1000;
    private static final NumberFormat NUM_FMT = NumberFormat.getIntegerInstance();
    private static final ObjectMapper objectMapper = new ObjectMapper();


    public static void main(String[] args) {
        java.security.Security.setProperty("networkaddress.cache.ttl" , "60");
        final Long limit = (args.length > 0) ? Long.parseLong(args[0]) : null;
        String stream = "FluxcapPrototype.AppEvent";
        String sql = "SELECT partitionKey, rawData data FROM `commercehub-nonprod-pmogren-collector`.streams.`AppEvent/`";
        if (limit != null) {
            sql = sql + " LIMIT " + limit;
        }

        Database database = connectDatabase();
        KinesisProducerConfiguration kinesisConfig = configureKinesis();
        final Pump pump = new Pump(database, sql, stream, kinesisConfig, Optional.of(addReplayFlags()));
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Destroying pump");
                pump.destroy();
            }
        }, "KPL shutdown hook"));

        Observable<UserRecordResult> results = pump.build();
        Subscription subscription = results.subscribe(
                new Subscriber<UserRecordResult>() {
                    AtomicLong successCount = new AtomicLong();
                    AtomicLong failCount = new AtomicLong();

                    Double startTime;

                    @Override
                    public void onCompleted() {
                        pump.flushSync();
                        log.info("Completed.");
                        stats();
                        pump.destroy();
                        System.exit(0);
                    }

                    private void stats() {
                        double endTime = (double) System.currentTimeMillis();
                        double elapsedTime = (endTime - startTime) / 1000d;
                        log.info("Emitted {} records successfully, along with {} failures, in {} seconds. Overall mean rate {} rec/s. Roughly {} records are pending.",
                                NUM_FMT.format(successCount), NUM_FMT.format(failCount), NUM_FMT.format(elapsedTime),
                                NUM_FMT.format((successCount.get() + failCount.get()) / elapsedTime),
                                NUM_FMT.format(pump.countPending()));
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.error("General failure, aborting.", e);
                        pump.destroy();
                        System.exit(1);
                    }

                    @Override
                    public void onNext(UserRecordResult userRecordResult) {
                        if (startTime == null) {
                            startTime = (double) System.currentTimeMillis();
                            request(RECORDS_PER_CHUNK);
                        }
                        log.trace("Got a Kinesis result.");
                        if (userRecordResult.isSuccessful()) {
                            successCount.incrementAndGet();
                        } else {
                            failCount.incrementAndGet();
                        }
                        long total = successCount.get() + failCount.get();
                        if (total == 1 || total % RECORDS_PER_CHUNK == 0) {
                            stats();
                        }
                        if (total % RECORDS_PER_CHUNK == 0) {
                            request(RECORDS_PER_CHUNK);
                        }
                    }
                });

        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(System.in));
            System.out.println("***** Enter STOP to stop pumping input and flush output, or QUIT to force quit *****");
            for (String line = input.readLine(); true; line = input.readLine()) {
                if ("STOP".equalsIgnoreCase(line)) {
                    log.warn("Stopping by user request...");
                    subscription.unsubscribe();
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            pump.flushSync();
                            System.exit(0);
                        }
                    }).start();
                } else if ("QUIT".equalsIgnoreCase(line)) {
                    log.warn("Quitting by user request...");
                    pump.destroy();
                    System.exit(3);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to stop cleanly", e);
            System.exit(4);
        }

    }

    static Function<byte[], byte[]> addReplayFlags() {
        return new Function<byte[], byte[]>() {
            @Override
            public byte[] apply(byte[] input) {
                try {
                    JsonNode tree = objectMapper.readTree(input);
                    if (JsonNodeType.OBJECT == tree.getNodeType()) {
                        ObjectNode rootObject = (ObjectNode) tree;
                        rootObject.set("replay", BooleanNode.TRUE);
                        rootObject.set("overwrite", BooleanNode.TRUE);
                    }
                    ByteArrayOutputStream output = new ByteArrayOutputStream(input.length + 50);
                    objectMapper.writeValue(output, tree);
                    output.close();
                    return output.toByteArray();
                } catch (Exception e) {
                    log.warn("Failed to add replay flags to record, using original record", e);
                    return input;
                }
            }
        };
    }

    private static KinesisProducerConfiguration configureKinesis() {
        KinesisProducerConfiguration kinesisConfig = new KinesisProducerConfiguration();
        kinesisConfig.setAggregationEnabled(false); //TODO enable KPL aggregation after Filter's KCL is upgraded; it should be a big help.
        kinesisConfig.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());
        kinesisConfig.setRegion(Regions.US_EAST_1.getName());
        kinesisConfig.setRecordTtl(Integer.MAX_VALUE);  //Maybe not the best idea to use MAX_VALUE
        // Pump works more smoothly when shards are not saturated, so 95% is a good maximum rate.
        // May be lowered further to share capacity with running applications.
        kinesisConfig.setRateLimit(50);

        //TODO set more Kinesis Configuration options as appropriate
        return kinesisConfig;
    }

    private static Database connectDatabase() {
        String jdbcUrl = "jdbc:drill:drillbit=localhost:31010";
        String jdbcUsername = "admin";
        String jdbcPassword = "admin";
        String driverClass = "org.apache.drill.jdbc.Driver";
        Database database;
        try {
            log.info("Testing database connection.");
            ConnectionProvider cp = IsolatedConnectionProvider.get(jdbcUrl, jdbcUsername, jdbcPassword, driverClass);
            cp.get().close();
            database = Database.from(cp);
            log.info("Database connectivity verified.");
        } catch (Exception e) {
            log.error("Could not establish a preliminary database connection", e);
            System.exit(2);
            database = null; //thanks javac.
        }
        return database;
    }
}

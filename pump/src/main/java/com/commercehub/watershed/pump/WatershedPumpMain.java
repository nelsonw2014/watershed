package com.commercehub.watershed.pump;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.github.davidmoten.rx.jdbc.ConnectionProvider;
import com.github.davidmoten.rx.jdbc.Database;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author pmogren
 */
public class WatershedPumpMain {
    private static final Logger log = LoggerFactory.getLogger(WatershedPumpMain.class);
    private static final byte[] REPLAY_FLAGS;
    private static final int RECORDS_PER_CHUNK = 1000;
    private static final NumberFormat NUM_FMT = NumberFormat.getIntegerInstance();

    static {
        try {
            REPLAY_FLAGS = "{\"replay\":true,\"overwrite\":true,".getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Your JRE is broken", e);
        }
    }

    public static void main(String[] args) {
        java.security.Security.setProperty("networkaddress.cache.ttl" , "60");
        final Long limit = (args.length > 0) ? Long.parseLong(args[0]) : null;
        String stream = "FluxcapPrototype.AppEvent";
        String sql = "SELECT e.documentPayload.orderId AS partitionKey, CONVERT_TO(e.documentPayload, 'JSON') AS data"
                + " FROM `commercehub-alextest`.app_event.`AppEvent/2015/08/04/` e";
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
        } catch (Exception bs) {
            log.warn("Failed to stop cleanly", bs);
            System.exit(4);
        }

    }

    private static Function<byte[], byte[]> addReplayFlags() {
        return new Function<byte[], byte[]>() {
            @Override
            public byte[] apply(byte[] input) {
                //Note that we intentionally overwrite the first byte from 'input' to remove the old opening brace
                byte[] newBytes = new byte[input.length + REPLAY_FLAGS.length - 1];
                System.arraycopy(input, 0, newBytes, REPLAY_FLAGS.length - 1, input.length);
                System.arraycopy(REPLAY_FLAGS, 0, newBytes, 0, REPLAY_FLAGS.length);
                return newBytes;
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

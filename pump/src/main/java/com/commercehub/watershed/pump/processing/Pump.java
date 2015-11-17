package com.commercehub.watershed.pump.processing;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.StreamDescription;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.commercehub.watershed.pump.SerializingProducer;
import com.github.davidmoten.rx.jdbc.Database;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.observable.ListenableFutureObservable;
import rx.schedulers.Schedulers;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;

/**
 * @author pmogren
 */
public class Pump {
    private static final Logger log = LoggerFactory.getLogger(Pump.class);
    public static final int MAX_RECORDS_PER_SHARD_PER_SECOND = 1000; //Kinesis service limit, at least prior to aggregation

    //TODO produce metrics

    private Database database;
    private String sql;
    private String stream;
    private int shardCount;
    private Optional<? extends Function<byte[], byte[]>> recordTransformer;
    private KinesisProducer kinesisProducer;
    private KinesisProducerConfiguration kinesisConfig;

    /**
     * @param database          Database configuration.
     * @param sql               SQL to query for stream records. Must produce result columns labeled {@code data}, which
     *                          will be retrieved as a byte array, and {@code partitionKey}, which will be retrieved as
     *                          a String.
     *                          Example: {@code SELECT partitionKey, data FROM storage.workspace.table WHERE processDate > '2015-01-01'}
     * @param stream            Name of Kinesis stream to which records will be published.
     * @param kinesisConfig     Kinesis configuration - AWS region, credential provider, buffering, retry, rate limiting, metrics, etc.
     * @param recordTransformer Optional function to transform a stream record before re-publishing it.
     */
    public Pump(Database database, String sql, String stream,
                KinesisProducerConfiguration kinesisConfig,
                Optional<? extends Function<byte[], byte[]>> recordTransformer) {
        this.database = database;
        this.sql = sql;
        this.stream = stream;
        this.recordTransformer = recordTransformer;
        this.kinesisConfig = kinesisConfig;
        this.kinesisProducer = new KinesisProducer(kinesisConfig);
        this.shardCount = countShardsInStream(stream, kinesisConfig);
    }

    private int countShardsInStream(String stream, KinesisProducerConfiguration kinesisConfig) {
        AmazonKinesisClient kinesisClient = new AmazonKinesisClient(kinesisConfig.getCredentialsProvider());
        kinesisClient.setRegion(Region.getRegion(Regions.fromName(kinesisConfig.getRegion())));
        int numShards = 0;
        StreamDescription desc = kinesisClient.describeStream(stream).getStreamDescription();
        int numShardsDescribed = desc.getShards().size();
        numShards += numShardsDescribed;
        while (desc.isHasMoreShards()) {
            desc = kinesisClient.describeStream(stream, desc.getShards().get(numShardsDescribed - 1).getShardId()).getStreamDescription();
        }
        return numShards;
    }

    /**
     * Defines an Observable pipeline to issue a query, transform records, and publish records to Kinesis. Does not
     * actually start pumping until the caller subscribes. The caller should monitor the Subscription for
     * non-recoverable errors by implementing {@code onError}, as well as checking every result for errors if they are
     * to be reported. To cancel pumping, unsubscribe.
     */
    public Observable<UserRecordResult> build() {
        // Can't actually use rxjava-jdbc with Drill at the moment: Drill'subscriber JDBC client is broken wrt PreparedStatements (DRILL-3566)
        // Also not sure whether rxjava-jdbc supports backpressure.
//        Observable<Record> dbRecords = database.select(sql).get(new ResultSetMapper<Record>() {
//            @Override
//            public Record call(ResultSet resultSet) throws SQLException {
//                return new Record().withPartitionKey(resultSet.getString("partitionKey")).
//                        withData(ByteBuffer.wrap(resultSet.getBytes("data")));
//            }
//        });
        Observable<Record> dbRecords = Observable.create(new Observable.OnSubscribe<Record>() {
            @Override
            public void call(final Subscriber<? super Record> subscriber) {
                final Connection connection = getConnection(subscriber);

                final ResultSet resultSet = executeQuery(subscriber, connection);

                JdbcRecordProducer jdbcRecordProducer = new JdbcRecordProducer(subscriber, resultSet, connection);
                subscriber.setProducer(jdbcRecordProducer);

                jdbcRecordProducer.request(shardCount * MAX_RECORDS_PER_SHARD_PER_SECOND * kinesisConfig.getRateLimit() / 200);
            }

            private ResultSet executeQuery(Subscriber<?> subscriber, Connection connection) {
                ResultSet resultSet;
                try {
                    log.info("Executing JDBC query {}", sql);
                    resultSet = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
                    log.info("Got a JDBC ResultSet, streaming results.");
                    resultSet.setFetchSize(Integer.MIN_VALUE);
                } catch (Exception e) {
                    resultSet = null;
                    subscriber.onError(e);
                }
                return resultSet;
            }

            private Connection getConnection(Subscriber<?> s) {
                Connection c;
                try {
                    log.info("Connecting to database...");
                    c = database.getConnectionProvider().get();
                } catch (Exception e) {
                    c = null;
                    s.onError(e);
                }
                return c;
            }

        }).subscribeOn(Schedulers.io());

        Observable<Record> transformedRecords;
        if (recordTransformer.isPresent()) {
            final Function<byte[], byte[]> transformer = recordTransformer.get();
            transformedRecords = dbRecords.map(new Func1<Record, Record>() {
                @Override
                public Record call(Record record) {
                    log.trace("Transforming record");
                    return record.clone().withData(ByteBuffer.wrap(transformer.apply(record.getData().array())));
                }
            });
        } else {
            transformedRecords = dbRecords;
        }

        Observable<UserRecordResult> pubResults = transformedRecords.flatMap(
                new Func1<Record, Observable<UserRecordResult>>() {
                    @Override
                    public Observable<UserRecordResult> call(Record record) {
                        log.debug("Adding record to Kinesis Producer");
                        return ListenableFutureObservable.from(
                                kinesisProducer.addUserRecord(stream, record.getPartitionKey(), record.getData()),
                                Schedulers.io());
                    }
                });

        return pubResults;
    }

    void destroy() {
        if (kinesisProducer != null) {
            kinesisProducer.destroy();
            kinesisProducer = null;
        }
    }

    void flushSync() {
        log.info("Attempting sync flush of about {} records", kinesisProducer.getOutstandingRecordsCount());
        kinesisProducer.flushSync();
    }

    long countPending() {
        return kinesisProducer.getOutstandingRecordsCount();
    }

    private static class JdbcRecordProducer extends SerializingProducer {
        private final Subscriber<? super Record> subscriber;
        private final ResultSet resultSet;
        private final Connection connection;

        public JdbcRecordProducer(Subscriber<? super Record> subscriber, ResultSet resultSet, Connection connection) {
            this.subscriber = subscriber;
            this.resultSet = resultSet;
            this.connection = connection;
        }

        @Override
        protected boolean onItemRequested() {
            boolean keepGoing = true;
            try {
                if (subscriber.isUnsubscribed()) {
                    keepGoing = false;
                    closeConnection();
                } else {
                    if (resultSet.next()) {
                        log.trace("Got a JDBC result record");
                        subscriber.onNext(new Record().withPartitionKey(resultSet.getString("partitionKey")).
                                withData(ByteBuffer.wrap(resultSet.getBytes("data"))));
                    } else {
                        subscriber.onCompleted();
                        keepGoing = false;
                    }
                }
            } catch (Exception e) {
                subscriber.onError(e);
                keepGoing = false;
            }
            return keepGoing;
        }

        private void closeConnection() {
            try {
                connection.close();
            } catch (Exception e) {
                log.warn("Failed to close database connection.", e);
            }
        }
    }
}

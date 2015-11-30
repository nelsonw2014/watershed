package com.commercehub.watershed.pump.processing;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.commercehub.watershed.pump.model.PumpSettings;
import com.commercehub.watershed.pump.service.KinesisService;
import com.google.common.base.Function;
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
import java.sql.SQLException;

/**
 * @author pmogren
 */
public class Pump {
    private static final Logger log = LoggerFactory.getLogger(Pump.class);

    //TODO produce metrics

    private Connection connection;
    private int shardCount;
    private Function<byte[], byte[]> recordTransformer;
    private KinesisProducer kinesisProducer;
    private PumpSettings pumpSettings;
    private KinesisService kinesisService;
    private int maxRecordsPerShardPerSecond; //Kinesis service limit, at least prior to aggregation
    private int producerRateLimit;
    /**
     * @param database          Database configuration.
     * @param sql               SQL to query for stream records. Must produce result columns labeled {@code rawData}, which
     *                          will be retrieved as a byte array, and {@code partitionKey}, which will be retrieved as
     *                          a String.
     *                          Example: {@code SELECT partitionKey, data FROM storage.workspace.table WHERE processDate > '2015-01-01'}
     * @param stream            Name of Kinesis stream to which records will be published.
     * @param kinesisConfig     Kinesis configuration - AWS region, credential provider, buffering, retry, rate limiting, metrics, etc.
     * @param recordTransformer Optional function to transform a stream record before re-publishing it.
     */
    public Pump(
            Connection connection,
            KinesisProducer kinesisProducer,
            KinesisService kinesisService,
            int maxRecordsPerShardPerSecond,
            int producerRateLimit) {

        this.connection = connection;
        this.kinesisProducer = kinesisProducer;
        this.kinesisService = kinesisService;
        this.maxRecordsPerShardPerSecond = maxRecordsPerShardPerSecond;
        this.producerRateLimit = producerRateLimit;
    }

    public Pump with(PumpSettings pumpSettings){
        this.pumpSettings = pumpSettings;
        this.shardCount = kinesisService.countShardsInStream(pumpSettings.getStreamOut());
        return this;
    }

    public Pump with(Function<byte[], byte[]> recordTransformer){
        this.recordTransformer = recordTransformer;
        return this;
    }



    /**
     * Defines an Observable pipeline to issue a query, transform records, and publish records to Kinesis. Does not
     * actually start pumping until the caller subscribes. The caller should monitor the Subscription for
     * non-recoverable errors by implementing {@code onError}, as well as checking every result for errors if they are
     * to be reported. To cancel pumping, unsubscribe.
     */
    public Observable<UserRecordResult> build() {
        // Can't actually use rxjava-jdbc with Drill at the moment: Drill's JDBC client is broken wrt PreparedStatements (DRILL-3566)
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
                subscriber.onStart();
                final ResultSet resultSet = executeQuery(subscriber, connection);

                JdbcRecordProducer jdbcRecordProducer = new JdbcRecordProducer(subscriber, resultSet, connection, pumpSettings);
                subscriber.setProducer(jdbcRecordProducer);

                jdbcRecordProducer.request(shardCount * maxRecordsPerShardPerSecond * producerRateLimit / 200);
            }

            private ResultSet executeQuery(Subscriber<?> subscriber, Connection connection) {
                ResultSet resultSet;
                try {
                    log.info("Executing JDBC query {}", pumpSettings.getQueryIn());
                    resultSet = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).executeQuery(pumpSettings.getQueryIn());
                    log.info("Got a JDBC ResultSet, streaming results.");
                    resultSet.setFetchSize(Integer.MIN_VALUE);
                }
                catch (Exception e) {
                    resultSet = null;
                    subscriber.onError(e);
                }

                return resultSet;
            }
        }).subscribeOn(Schedulers.io());

        Observable<Record> transformedRecords;
        if (recordTransformer != null) {
            transformedRecords = dbRecords.map(new Func1<Record, Record>() {
                @Override
                public Record call(Record record) {
                    log.trace("Transforming record");
                    return record.clone().withData(ByteBuffer.wrap(recordTransformer.apply(record.getData().array())));
                }
            });
        }
        else {
            transformedRecords = dbRecords;
        }

        Observable<UserRecordResult> pubResults = transformedRecords.flatMap(
                new Func1<Record, Observable<UserRecordResult>>() {
                    @Override
                    public Observable<UserRecordResult> call(Record record) {
                        log.debug("Adding record to Kinesis Producer");
                        return ListenableFutureObservable.from(
                                kinesisProducer.addUserRecord(pumpSettings.getStreamOut(), record.getPartitionKey(), record.getData()),
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
        private final PumpSettings pumpSettings;

        public JdbcRecordProducer(Subscriber<? super Record> subscriber, ResultSet resultSet, Connection connection, PumpSettings pumpSettings) {
            this.subscriber = subscriber;
            this.resultSet = resultSet;
            this.connection = connection;
            this.pumpSettings = pumpSettings;
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

                        subscriber.onNext(buildRecord(resultSet, pumpSettings.getPartitionKeyColumn(), pumpSettings.getRawDataColumn()));
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

        private Record buildRecord(ResultSet resultSet, String partitionKeyColumn, String rawDataColumn) throws SQLException{
            Record record = new Record();
            record.withPartitionKey(resultSet.getString(partitionKeyColumn));
            record.withData(ByteBuffer.wrap(resultSet.getBytes(rawDataColumn)));

            return record;
        }
    }
}

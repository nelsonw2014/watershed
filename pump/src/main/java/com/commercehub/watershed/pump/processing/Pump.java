package com.commercehub.watershed.pump.processing;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.commercehub.watershed.pump.model.PumpSettings;
import com.commercehub.watershed.pump.service.KinesisService;
import com.google.common.base.Function;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.observable.ListenableFutureObservable;
import rx.schedulers.Schedulers;

import java.nio.ByteBuffer;
import java.security.InvalidParameterException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Take from the Drill, give to the Kinesis.
 */
public class Pump {
    private static final Logger log = LoggerFactory.getLogger(Pump.class);

    //TODO produce metrics

    private Connection connection;
    private int shardCount;
    private Function<byte[], byte[]> recordTransformer;
    private KinesisProducer kinesisProducer;
    private PumpSettings pumpSettings;
    private int maxRecordsPerShardPerSecond; //Kinesis service limit, at least prior to aggregation
    private int producerRateLimit;

    /**
     *
     * @param connection                    Database connection.
     * @param kinesisProducer               Produces records for Kinesis.
     * @param kinesisService                Communicates with Kinesis to retrieve information about Kinesis streams.
     * @param maxRecordsPerShardPerSecond   The maximum number of records per shard per second that Pump is allowed to handle.
     * @param producerRateLimit             Limits the maximum allowed put rate for a shard, as a percentage of the backend limits.
     */
    @Inject
    public Pump(
            Connection connection,
            KinesisProducer kinesisProducer,
            KinesisService kinesisService,
            @Named("maxRecordsPerShardPerSecond") int maxRecordsPerShardPerSecond,
            @Named("producerRateLimit") int producerRateLimit,
            @Assisted PumpSettings pumpSettings,
            @Assisted Function<byte[], byte[]> recordTransformer) {

        this.connection = connection;
        this.kinesisProducer = kinesisProducer;
        this.maxRecordsPerShardPerSecond = maxRecordsPerShardPerSecond;
        this.producerRateLimit = producerRateLimit;
    /**
     * Include PumpSettings. Pump will not run without this.
     * @param pumpSettings
     * @return this
     */
        this.pumpSettings = pumpSettings;
    /**
     * Optionally set a recordTransformer (used to modify the raw data emitted to Kinesis)
     * @param recordTransformer
     * @return this
     */
        this.recordTransformer = recordTransformer;
        this.shardCount = kinesisService.countShardsInStream(pumpSettings.getStreamOut());
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
        /*
        Observable<Record> dbRecords = database.select(sql).get(new ResultSetMapper<Record>() {
            @Override
            public Record call(ResultSet resultSet) throws SQLException {
                return new Record().withPartitionKey(resultSet.getString("partitionKey")).
                        withData(ByteBuffer.wrap(resultSet.getBytes("data")));
            }
        });
        */

        if(this.pumpSettings == null){
            throw new InvalidParameterException("PumpSettings were not provided.");
        }

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

    /**
     * destroys the kinesis producer, stops emission
     */
    void destroy() {
        if (kinesisProducer != null) {
            kinesisProducer.destroy();
            kinesisProducer = null;
        }
    }

    /**
     * Instructs the kinesisProducer to flush all records and waits until all
     * records are complete (either succeeding or failing).
     */
    void flushSync() {
        log.info("Attempting sync flush of about {} records", kinesisProducer.getOutstandingRecordsCount());
        kinesisProducer.flushSync();
    }

    /**
     *
     * @return outstanding record count that hasn't been emitted yet
     */
    long countPending() {
        return kinesisProducer.getOutstandingRecordsCount();
    }


    /**
     * Producer that takes a ResultSet and turns it into Records for Kinesis emission.
     */
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

        /**
         * @return whether it is okay to continue requesting items
         */
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

        /**
         * closes database connection
         */
        private void closeConnection() {
            try {
                connection.close();
            } catch (Exception e) {
                log.warn("Failed to close database connection.", e);
            }
        }

        /**
         *
         * @param resultSet from the database query
         * @param partitionKeyColumn column to use for the partition key value
         * @param rawDataColumn column to use for the raw data
         * @return
         * @throws SQLException
         */
        private Record buildRecord(ResultSet resultSet, String partitionKeyColumn, String rawDataColumn) throws SQLException{
            Record record = new Record();
            record.withPartitionKey(resultSet.getString(partitionKeyColumn));
            record.withData(ByteBuffer.wrap(resultSet.getBytes(rawDataColumn)));

            return record;
        }
    }
}

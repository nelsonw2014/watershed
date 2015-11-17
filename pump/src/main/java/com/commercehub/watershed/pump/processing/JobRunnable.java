package com.commercehub.watershed.pump.processing;

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.ProcessingStage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.davidmoten.rx.jdbc.Database;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.io.ByteArrayOutputStream;
import java.text.NumberFormat;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public class JobRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(JobRunnable.class);
    private static final int RECORDS_PER_CHUNK = 1000;
    private static final NumberFormat NUM_FMT = NumberFormat.getIntegerInstance();
    private ObjectMapper objectMapper;

    //private Queue<Job> jobQueue;
    private Job job;
    private KinesisProducerConfiguration kinesisProducerConfiguration;
    private Database database;

    /*
    public JobRunnable(Queue<Job> jobQueue, KinesisProducerConfiguration kinesisProducerConfiguration, Database database, ObjectMapper objectMapper){
        this.jobQueue = jobQueue;
        this.kinesisProducerConfiguration = kinesisProducerConfiguration;
        this.database = database;
        this.objectMapper = objectMapper;
    }

    public void run(){
        while(jobQueue.size() > 0) {
            processJob(getNextJobOnQueue());
        }
    }
    */

    public JobRunnable(Job job, KinesisProducerConfiguration kinesisProducerConfiguration, Database database, ObjectMapper objectMapper){
        this.job = job;
        this.kinesisProducerConfiguration = kinesisProducerConfiguration;
        this.database = database;
        this.objectMapper = objectMapper;
    }

    public void run(){
        processJob(job);
    }

    /*
    private Job getNextJobOnQueue(){
        return jobQueue.remove();
    }
    */

    private void processJob(final Job job){


        java.security.Security.setProperty("networkaddress.cache.ttl", "60");
        String stream = job.getPumpSettings().getStreamOut(); //"FluxcapPrototype.AppEvent";
        String sql = job.getPumpSettings().getQueryIn(); //"SELECT partitionKey, rawData data FROM `commercehub-nonprod-pmogren-collector`.streams.`AppEvent/`";

        final Pump pump = new Pump(database, sql, stream, kinesisProducerConfiguration, Optional.of(addReplayFlags(job)));
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Destroying pump (job: {})", job.getJobId());
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
                        log.info("Completed (job: {})", job.getJobId());
                        stats();
                        pump.destroy();
                        job.setStage(ProcessingStage.COMPLETED_SUCCESS);
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
                        job.addProcessingError(e);
                        job.setStage(ProcessingStage.COMPLETED_ERROR);
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

        job.setPumpSubscription(subscription);
    }

    private Function<byte[], byte[]> addReplayFlags(Job job) {
        final BooleanNode replayEnabled = job.getPumpSettings().hasReplayFlag()? BooleanNode.TRUE : BooleanNode.FALSE;
        final BooleanNode overwriteEnabled = job.getPumpSettings().hasOverwriteFlag()? BooleanNode.TRUE : BooleanNode.FALSE;

        return new Function<byte[], byte[]>() {
            @Override
            public byte[] apply(byte[] input) {
                try {
                    JsonNode tree = objectMapper.readTree(input);
                    if (JsonNodeType.OBJECT == tree.getNodeType()) {
                        ObjectNode rootObject = (ObjectNode) tree;
                        rootObject.set("replay", replayEnabled);
                        rootObject.set("overwrite", overwriteEnabled);
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


}

package com.commercehub.watershed.pump.processing;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.ProcessingStage;
import com.commercehub.watershed.pump.model.PumpSettings;
import com.commercehub.watershed.pump.service.TransformerService;
import com.google.common.base.Optional;
import com.google.inject.Provider;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicLong;

public class JobRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(JobRunnable.class);
    private static final NumberFormat NUM_FMT = NumberFormat.getIntegerInstance();

    private Job job;
    private TransformerService transformerService;
    private Provider<Pump> pumpProvider;
    private int numRecordsPerChunk;

    public JobRunnable(
            TransformerService transformerService,
            Provider<Pump> pumpProvider, int numRecordsPerChunk){
        this.transformerService = transformerService;
        this.pumpProvider = pumpProvider;
        this.numRecordsPerChunk = numRecordsPerChunk;
    }

    public JobRunnable withJob(Job job){
        this.job = job;
        return this;
    }

    public void run(){
        if(job == null) return;

        processJob(job);
    }

    private void processJob(final Job job){

        PumpSettings pumpSettings = job.getPumpSettings();
        final Pump pump = pumpProvider.get()
                .with(pumpSettings)
                .with(Optional.of(transformerService.addReplayFlags(pumpSettings.getHasReplayFlag(), pumpSettings.getHasOverwriteFlag())));

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

                    @Override
                    public void onStart(){
                        job.setStage(ProcessingStage.IN_PROGRESS);
                        job.setStartTime(Instant.now().toDateTime());
                        job.setSuccessfulRecordCount(0L);
                        job.setFailureRecordCount(0L);

                        request(numRecordsPerChunk);
                    }

                    @Override
                    public void onCompleted() {
                        pump.flushSync();
                        log.info("Completed (job: {})", job.getJobId());
                        stats();
                        pump.destroy();
                        job.setStage(ProcessingStage.COMPLETED_SUCCESS);
                        job.setCompletionTime(Instant.now().toDateTime());
                    }

                    private void stats() {
                        long pendingRecordCount = pump.countPending();

                        job.setSuccessfulRecordCount(successCount.get());
                        job.setFailureRecordCount(failCount.get());
                        job.setPendingRecordCount(pendingRecordCount);

                        log.info("Emitted {} records successfully, along with {} failures, in {}. Overall mean rate {}. Roughly {} records are pending.",
                                NUM_FMT.format(successCount),
                                NUM_FMT.format(failCount),
                                job.getElapsedTimePretty(),
                                job.getMeanRatePretty(),
                                NUM_FMT.format(pendingRecordCount));
                    }

                    @Override
                    public void onError(Throwable e) {
                        log.error("General failure, aborting.", e);
                        pump.destroy();
                        job.addProcessingError(e);
                        job.setStage(ProcessingStage.COMPLETED_ERROR);
                        job.setCompletionTime(Instant.now().toDateTime());
                    }

                    @Override
                    public void onNext(UserRecordResult userRecordResult) {
                        log.trace("Got a Kinesis result.");
                        if (userRecordResult.isSuccessful()) {
                            successCount.incrementAndGet();
                        } else {
                            failCount.incrementAndGet();
                        }
                        long total = successCount.get() + failCount.get();
                        if (total == 1 || total % numRecordsPerChunk == 0) {
                            stats();
                        }
                        if (total % numRecordsPerChunk == 0) {
                            request(numRecordsPerChunk);
                        }
                    }
                });

        job.setPumpSubscription(subscription);
    }
}

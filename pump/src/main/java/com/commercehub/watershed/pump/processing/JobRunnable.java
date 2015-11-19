package com.commercehub.watershed.pump.processing;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.ProcessingStage;
import com.commercehub.watershed.pump.model.PumpSettings;
import com.commercehub.watershed.pump.service.TransformerService;
import com.google.common.base.Optional;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicLong;

public class JobRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(JobRunnable.class);
    private static final int RECORDS_PER_CHUNK = 1000;
    private static final NumberFormat NUM_FMT = NumberFormat.getIntegerInstance();

    private Job job;
    private TransformerService transformerService;
    private Provider<Pump> pumpProvider;

    public JobRunnable(
            TransformerService transformerService,
            Provider<Pump> pumpProvider){
        this.transformerService = transformerService;
        this.pumpProvider = pumpProvider;
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

        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

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
}

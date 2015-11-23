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
    private Provider<PumpSubscriber> pumpSubscriberProvider;
    private int numRecordsPerChunk;

    public JobRunnable(
            TransformerService transformerService,
            Provider<Pump> pumpProvider,
            Provider<PumpSubscriber> pumpSubscriberProvider){

        this.transformerService = transformerService;
        this.pumpProvider = pumpProvider;
        this.pumpSubscriberProvider = pumpSubscriberProvider;
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
        Subscription subscription = results.subscribe(pumpSubscriberProvider.get().with(job, pump));
        job.setPumpSubscription(subscription);
    }
}

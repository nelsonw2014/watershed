package com.commercehub.watershed.pump.processing;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.PumpSettings;
import com.commercehub.watershed.pump.service.TransformerFunctionFactory;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;


public class JobRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(JobRunnable.class);

    private Job job;
    private TransformerFunctionFactory transformerFunctionFactory;
    private Provider<Pump> pumpProvider;
    private Provider<PumpSubscriber> pumpSubscriberProvider;

    public JobRunnable(
            TransformerFunctionFactory transformerFunctionFactory,
            Provider<Pump> pumpProvider,
            Provider<PumpSubscriber> pumpSubscriberProvider){

        this.transformerFunctionFactory = transformerFunctionFactory;
        this.pumpProvider = pumpProvider;
        this.pumpSubscriberProvider = pumpSubscriberProvider;
    }

    public JobRunnable with(Job job){
        this.job = job;
        return this;
    }

    public void run(){
        if(job == null){
            throw new IllegalStateException("Job cannot be null.");
        }

        PumpSettings pumpSettings = job.getPumpSettings();
        final Pump pump = pumpProvider.get()
                .with(pumpSettings)
                .with(transformerFunctionFactory.getReplayFlagTransformFunction(pumpSettings.getHasReplayFlag(), pumpSettings.getHasOverwriteFlag()));

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

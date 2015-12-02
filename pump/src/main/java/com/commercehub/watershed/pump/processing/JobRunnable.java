package com.commercehub.watershed.pump.processing;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.commercehub.watershed.pump.application.factories.PumpFactory;
import com.commercehub.watershed.pump.application.factories.PumpSubscriberFactory;
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

    private PumpFactory pumpFactory;
    private PumpSubscriberFactory pumpSubscriberFactory;

    public JobRunnable(
            TransformerFunctionFactory transformerFunctionFactory,
            PumpFactory pumpFactory,
            PumpSubscriberFactory pumpSubscriberFactory){

        this.transformerFunctionFactory = transformerFunctionFactory;
        this.pumpFactory = pumpFactory;
        this.pumpSubscriberFactory = pumpSubscriberFactory;
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
        final Pump pump = pumpFactory.create(
                pumpSettings,
                transformerFunctionFactory.getReplayFlagTransformFunction(pumpSettings.getHasReplayFlag(), pumpSettings.getHasOverwriteFlag()));

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Destroying pump (job: {})", job.getJobId());
                pump.destroy();
            }
        }, "KPL shutdown hook"));

        Observable<UserRecordResult> results = pump.build();
        Subscription subscription = results.subscribe(pumpSubscriberFactory.create(job, pump));
        job.setPumpSubscription(subscription);
    }
}

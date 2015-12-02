package com.commercehub.watershed.pump.processing

import com.amazonaws.services.kinesis.producer.UserRecordResult
import com.commercehub.watershed.pump.application.factories.PumpFactory
import com.commercehub.watershed.pump.application.factories.PumpSubscriberFactory
import com.commercehub.watershed.pump.model.Job
import com.commercehub.watershed.pump.model.PumpSettings
import com.commercehub.watershed.pump.service.TransformerFunctionFactory
import com.google.common.base.Function
import com.google.inject.Provider
import rx.Observable
import spock.lang.Specification


public class JobRunnableSpec extends Specification {
    JobRunnable jobRunnable
    Job job
    Pump pump
    PumpSubscriber pumpSubscriber
    TransformerFunctionFactory transformerFunctionFactory
    PumpFactory pumpFactory
    PumpSubscriberFactory pumpSubscriberFactory
    Observable<UserRecordResult> UserRecordResultObservable
    Observable.OnSubscribe onSubscribe


    def setup(){
        job = Mock(Job)
        pump = Mock(Pump)
        pumpSubscriber = Mock(PumpSubscriber)
        transformerFunctionFactory = Mock(TransformerFunctionFactory)
        pumpFactory = Mock(PumpFactory)
        pumpSubscriberFactory = Mock(PumpSubscriberFactory)
        onSubscribe = Mock(Observable.OnSubscribe)
        UserRecordResultObservable = new Observable<UserRecordResult>(onSubscribe)

        jobRunnable = new JobRunnable(transformerFunctionFactory, pumpFactory, pumpSubscriberFactory, job)

        job.getPumpSettings() >> new PumpSettings()
        transformerFunctionFactory.getReplayFlagTransformFunction(_, _) >> Mock(Function)
    }

    def "run creates a Pump, calls build(), and subscribes to it"(){
        when:
        jobRunnable.run()

        then:
        1 * pumpFactory.create(_ as PumpSettings, _ as Function<byte[], byte[]>) >> pump

        then:
        1 * pump.build() >> UserRecordResultObservable

        then:
        1 * pumpSubscriberFactory.create(_ as Job, _ as Pump) >> pumpSubscriber

        then:
        1 * onSubscribe.call(_)
        1 * pumpSubscriber.onStart()
    }

    def "jobRunnable doesn't run without a job"(){
        setup:
        jobRunnable.job = null

        when:
        jobRunnable.run()

        then:
        thrown(IllegalStateException)
        0 * pumpFactory.create(_, _)
        0 * pump.build()
        0 * pumpSubscriberFactory.create(_, _)
        0 * pumpSubscriber.onStart()
    }

}

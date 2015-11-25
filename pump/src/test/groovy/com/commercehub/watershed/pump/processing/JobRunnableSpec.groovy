package com.commercehub.watershed.pump.processing

import com.amazonaws.services.kinesis.producer.UserRecordResult
import com.commercehub.watershed.pump.model.Job
import com.commercehub.watershed.pump.model.PumpSettings
import com.commercehub.watershed.pump.service.TransformerService
import com.google.common.base.Function
import com.google.common.base.Optional
import com.google.inject.Provider
import rx.Observable
import spock.lang.Specification


public class JobRunnableSpec extends Specification {
    JobRunnable jobRunnable
    Job job
    Pump pump
    PumpSubscriber pumpSubscriber
    TransformerService transformerService
    Provider<Pump> pumpProvider
    Provider<PumpSubscriber> pumpSubscriberProvider
    Observable<UserRecordResult> UserRecordResultObservable
    Observable.OnSubscribe onSubscribe


    def setup(){
        job = Mock(Job)
        pump = Mock(Pump)
        pumpSubscriber = Mock(PumpSubscriber)
        transformerService = Mock(TransformerService)
        pumpProvider = Mock(Provider)
        pumpSubscriberProvider = Mock(Provider)
        onSubscribe = Mock(Observable.OnSubscribe)
        UserRecordResultObservable = new Observable<UserRecordResult>(onSubscribe)

        jobRunnable = new JobRunnable(transformerService, pumpProvider, pumpSubscriberProvider)
        jobRunnable.with(job)

        job.getPumpSettings() >> new PumpSettings()
        transformerService.addReplayFlags(_, _) >> Mock(Function)


    }

    def "run creates a Pump, calls build(), and subscribes to it"(){
        when:
        jobRunnable.run()

        then:
        1 * pump.with(_ as Optional<? extends Function<byte[], byte[]>>) >> pump
        1 * pumpProvider.get() >> pump
        1 * pump.with(_ as PumpSettings) >> pump

        then:
        1 * pump.build() >> UserRecordResultObservable

        then:
        1 * pumpSubscriberProvider.get() >> pumpSubscriber
        1 * pumpSubscriber.with(_ as Job, _ as Pump) >> pumpSubscriber

        then:
        1 * onSubscribe.call(_)
    }

}

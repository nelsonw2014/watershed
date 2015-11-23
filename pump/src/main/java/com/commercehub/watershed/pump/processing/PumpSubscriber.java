package com.commercehub.watershed.pump.processing;

import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.commercehub.watershed.pump.model.Job;
import com.commercehub.watershed.pump.model.ProcessingStage;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.text.NumberFormat;
import java.util.concurrent.atomic.AtomicLong;

public class PumpSubscriber extends Subscriber<UserRecordResult> {
    private static final Logger log = LoggerFactory.getLogger(PumpSubscriber.class);
    private static final NumberFormat NUM_FMT = NumberFormat.getIntegerInstance();

    private AtomicLong successCount = new AtomicLong();
    private AtomicLong failCount = new AtomicLong();
    private int numRecordsPerChunk;

    private Job job;
    private Pump pump;

    public PumpSubscriber(int numRecordsPerChunk){
        this.numRecordsPerChunk = numRecordsPerChunk;
    }

    public PumpSubscriber with(Job job, Pump pump){
        this.job = job;
        this.pump = pump;
        return this;
    }

    @Override
    public void onStart(){
        if(job == null || pump == null) return;

        job.setStage(ProcessingStage.IN_PROGRESS);
        job.setStartTime(Instant.now().toDateTime());
        job.setSuccessfulRecordCount(0L);
        job.setFailureRecordCount(0L);

        request(numRecordsPerChunk);
    }

    @Override
    public void onCompleted() {
        if(job == null || pump == null) return;

        pump.flushSync();
        updateStats();
        pump.destroy();

        log.info("Completed (job: {})", job.getJobId());
        job.setStage(ProcessingStage.COMPLETED_SUCCESS);
        job.setCompletionTime(Instant.now().toDateTime());
    }

    public void updateStats() {
        if(job == null || pump == null) return;

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
        if(job == null || pump == null) return;

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
        }
        else {
            failCount.incrementAndGet();
        }

        long total = successCount.get() + failCount.get();
        if (total == 1 || total % numRecordsPerChunk == 0) {
            updateStats();
        }

        if (total % numRecordsPerChunk == 0) {
            request(numRecordsPerChunk);
        }
    }
}

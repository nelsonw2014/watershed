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

/**
 * A Subscriber for Pump that requests records and manages Job statistics
 */
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

    /**
     * Specify which Job and Pump this subscriber is tied to
     * @param job the Job
     * @param pump the Pump
     * @return this
     */
    public PumpSubscriber with(Job job, Pump pump){
        this.job = job;
        this.pump = pump;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onStart(){
        if(job != null){
            job.setStage(ProcessingStage.IN_PROGRESS);
            job.setStartTime(Instant.now().toDateTime());
            job.setSuccessfulRecordCount(0L);
            job.setFailureRecordCount(0L);
        }

        request(numRecordsPerChunk);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onCompleted() {
        if(pump != null){
            pump.flushSync();
            updateStats();
            pump.destroy();
        }

        if(job != null){
            log.info("Completed (job: {})", job.getJobId());
            job.setStage(ProcessingStage.COMPLETED_SUCCESS);
            job.setCompletionTime(Instant.now().toDateTime());
        }
    }

    /**
     * Populates Job with statistics for current state of Pump
     */
    public void updateStats() {
        if(job != null){
            job.setSuccessfulRecordCount(successCount.get());
            job.setFailureRecordCount(failCount.get());

            if(pump != null){
                job.setPendingRecordCount(pump.countPending());
            }
        }

        log.info("Emitted {} records successfully, along with {} failures, in {}. Overall mean rate {}. Roughly {} records are pending.",
                NUM_FMT.format(successCount),
                NUM_FMT.format(failCount),
                (job != null? job.getElapsedTimePretty() : "unknown"),
                (job != null? job.getMeanRatePretty() : "unknown"),
                (pump != null? NUM_FMT.format(pump.countPending()) : "unknown"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onError(Throwable e) {
        log.error("General failure, aborting.", e);

        if(pump != null){
            pump.destroy();
        }

        if(job != null){
            job.addProcessingError(e);
            job.setStage(ProcessingStage.COMPLETED_ERROR);
            job.setCompletionTime(Instant.now().toDateTime());
        }
    }

    /**
     * {@inheritDoc}
     */
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
        if (total == 1) {
            updateStats();
        }

        if (total % numRecordsPerChunk == 0) {
            updateStats();
            request(numRecordsPerChunk);
        }
    }
}

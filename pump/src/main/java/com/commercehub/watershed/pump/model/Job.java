package com.commercehub.watershed.pump.model;

import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import rx.Subscription;

import java.util.ArrayList;
import java.util.List;

public class Job {
    String jobId;
    PumpSettings pumpSettings;
    List<Throwable> processingErrors;
    Subscription pumpSubscription;

    Long successfulRecordCount;
    Long failureRecordCount;
    Long pendingRecordCount;

    DateTime startTime;
    DateTime completionTime;

    ProcessingStage stage = ProcessingStage.NOT_STARTED;

    PeriodFormatter formatter = new PeriodFormatterBuilder()
            .appendHours().appendSuffix(" hours, ")
            .appendMinutes().appendSuffix(" minutes, ")
            .appendSeconds().appendSuffix(".")
            .appendMillis3Digit().appendSuffix(" seconds")
            .printZeroNever()
            .toFormatter();

    public Job(String jobId, PumpSettings pumpSettings){
        this.jobId = jobId;
        this.pumpSettings = pumpSettings;
        this.processingErrors = new ArrayList<>();
    }

    public String getJobId() {
        return jobId;
    }

    public PumpSettings getPumpSettings() {
        return pumpSettings;
    }

    public ProcessingStage getStage() {
        return stage;
    }

    public void setStage(ProcessingStage stage) {
        this.stage = stage;
    }

    public List<Throwable> getProcessingErrors() {
        return processingErrors;
    }

    public void addProcessingError(Throwable processingError) {
        processingErrors.add(processingError);
    }

    public Subscription getPumpSubscription() {
        return pumpSubscription;
    }

    public void setPumpSubscription(Subscription pumpSubscription) {
        this.pumpSubscription = pumpSubscription;
    }

    public Long getSuccessfulRecordCount() {
        return successfulRecordCount;
    }

    public void setSuccessfulRecordCount(Long successfulRecordCount) {
        this.successfulRecordCount = successfulRecordCount;
    }

    public Long getFailureRecordCount() {
        return failureRecordCount;
    }

    public void setFailureRecordCount(Long failureRecordCount) {
        this.failureRecordCount = failureRecordCount;
    }

    public DateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    public DateTime getCompletionTime() {
        return completionTime;
    }

    public void setCompletionTime(DateTime completionTime) {
        this.completionTime = completionTime;
    }

    public Long getElapsedTime() {
        if(completionTime == null){
            return startTime != null? System.currentTimeMillis() - startTime.getMillis() : 0L;
        }

        return completionTime.getMillis() - startTime.getMillis();
    }

    public String getElapsedTimePretty() {
        return formatter.print(new Period(getElapsedTime() > 0? getElapsedTime().longValue() : 0L));
    }

    public Long getPendingRecordCount() {
        return pendingRecordCount;
    }

    public void setPendingRecordCount(Long pendingRecordCount) {
        this.pendingRecordCount = pendingRecordCount;
    }

    public Double getMeanRate() {
        return getElapsedTime() > 0? (successfulRecordCount + failureRecordCount) / (getElapsedTime() / 1000d) : null;
    }

    public String getMeanRatePretty() {
        return getElapsedTime() > 0 && getMeanRate() != null? String.format("%.1f rec/s", getMeanRate()) : "∞ rec/s";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Job job = (Job) o;

        if (!jobId.equals(job.jobId)) return false;
        if (!pumpSettings.equals(job.pumpSettings)) return false;
        if (processingErrors != null ? !processingErrors.equals(job.processingErrors) : job.processingErrors != null)
            return false;
        if (pumpSubscription != null ? !pumpSubscription.equals(job.pumpSubscription) : job.pumpSubscription != null)
            return false;

        return stage == job.stage;

    }

    @Override
    public int hashCode() {
        int result = jobId.hashCode();
        result = 31 * result + pumpSettings.hashCode();
        result = 31 * result + (processingErrors != null ? processingErrors.hashCode() : 0);
        result = 31 * result + (pumpSubscription != null ? pumpSubscription.hashCode() : 0);
        result = 31 * result + stage.hashCode();
        return result;
    }
}

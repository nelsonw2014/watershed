package com.commercehub.watershed.pump.model;

import rx.Subscription;

import java.util.ArrayList;
import java.util.List;

public class Job {
    String jobId;
    PumpSettings pumpSettings;
    List<Throwable> processingErrors;
    Subscription pumpSubscription;

    Integer totalRecordCount;
    Integer currentRecordCount;
    Integer successfulRecordCount;
    Integer failureRecordCount;

    ProcessingStage stage = ProcessingStage.NOT_STARTED;

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

    public Integer getTotalRecordCount() {
        return totalRecordCount;
    }

    public void setTotalRecordCount(Integer totalRecordCount) {
        this.totalRecordCount = totalRecordCount;
    }

    public Integer getCurrentRecordCount() {
        return currentRecordCount;
    }

    public void setCurrentRecordCount(Integer currentRecordCount) {
        this.currentRecordCount = currentRecordCount;
    }

    public Integer getSuccessfulRecordCount() {
        return successfulRecordCount;
    }

    public void setSuccessfulRecordCount(Integer successfulRecordCount) {
        this.successfulRecordCount = successfulRecordCount;
    }

    public Integer getFailureRecordCount() {
        return failureRecordCount;
    }

    public void setFailureRecordCount(Integer failureRecordCount) {
        this.failureRecordCount = failureRecordCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Job job = (Job) o;

        if (!jobId.equals(job.jobId)) return false;
        if (!pumpSettings.equals(job.pumpSettings)) return false;
        if (!processingErrors.equals(job.processingErrors)) return false;
        if (pumpSubscription != null ? !pumpSubscription.equals(job.pumpSubscription) : job.pumpSubscription != null)
            return false;
        if (totalRecordCount != null ? !totalRecordCount.equals(job.totalRecordCount) : job.totalRecordCount != null)
            return false;
        if (currentRecordCount != null ? !currentRecordCount.equals(job.currentRecordCount) : job.currentRecordCount != null)
            return false;
        if (successfulRecordCount != null ? !successfulRecordCount.equals(job.successfulRecordCount) : job.successfulRecordCount != null)
            return false;
        if (failureRecordCount != null ? !failureRecordCount.equals(job.failureRecordCount) : job.failureRecordCount != null)
            return false;
        return stage == job.stage;

    }

    @Override
    public int hashCode() {
        int result = jobId.hashCode();
        result = 31 * result + pumpSettings.hashCode();
        result = 31 * result + processingErrors.hashCode();
        result = 31 * result + (pumpSubscription != null ? pumpSubscription.hashCode() : 0);
        result = 31 * result + (totalRecordCount != null ? totalRecordCount.hashCode() : 0);
        result = 31 * result + (currentRecordCount != null ? currentRecordCount.hashCode() : 0);
        result = 31 * result + (successfulRecordCount != null ? successfulRecordCount.hashCode() : 0);
        result = 31 * result + (failureRecordCount != null ? failureRecordCount.hashCode() : 0);
        result = 31 * result + stage.hashCode();
        return result;
    }
}

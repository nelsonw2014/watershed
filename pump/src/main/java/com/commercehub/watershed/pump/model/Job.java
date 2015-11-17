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
}

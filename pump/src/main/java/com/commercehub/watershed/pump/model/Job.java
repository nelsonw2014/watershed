package com.commercehub.watershed.pump.model;

public abstract class Job {
    String jobId;
    JobSettings jobSettings;

    Integer numTotalRecords;
    ProcessingStage stage = ProcessingStage.NOT_STARTED;

    public Job(String jobId, JobSettings jobSettings){
        this.jobId = jobId;
        this.jobSettings = jobSettings;
    }

    public String getJobId() {
        return jobId;
    }

    public JobSettings getJobSettings() {
        return jobSettings;
    }

    public Integer getNumTotalRecords() {
        return numTotalRecords;
    }

    public void setNumTotalRecords(Integer numTotalRecords) {
        this.numTotalRecords = numTotalRecords;
    }

    public ProcessingStage getStage() {
        return stage;
    }

    public void setStage(ProcessingStage stage) {
        this.stage = stage;
    }
}

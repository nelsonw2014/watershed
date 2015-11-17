package com.commercehub.watershed.pump.model;

public class JobSettings {
    private String query;
    private Boolean hasOverwriteFlag;
    private JobType jobType;

    public JobSettings(String query, Boolean hasOverwriteFlag, JobType jobType) {
        this.query = query;
        this.hasOverwriteFlag = hasOverwriteFlag;
        this.jobType = jobType;
    }

    public String getQuery() {
        return query;
    }

    public Boolean hasOverwriteFlag() {
        return hasOverwriteFlag;
    }

    public JobType getJobType() {
        return jobType;
    }
}

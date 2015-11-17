package com.commercehub.watershed.pump.model;

public class QueryAndEmitJob extends Job{
    Integer currentRecord;

    public QueryAndEmitJob(String jobId, JobSettings jobSettings) {
        super(jobId, jobSettings);
    }

    public Integer getCurrentRecord() {
        return currentRecord;
    }

    public void setCurrentRecord(Integer currentRecord) {
        this.currentRecord = currentRecord;
    }

}

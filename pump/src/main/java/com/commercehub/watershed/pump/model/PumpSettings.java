package com.commercehub.watershed.pump.model;

import javax.validation.constraints.NotNull;

public class PumpSettings {
    @NotNull
    private String queryIn;

    @NotNull
    private Boolean hasOverwriteFlag = false;

    @NotNull
    private Boolean hasReplayFlag = false;

    @NotNull
    private String streamOut;

    @NotNull
    private String rawDataColumn = "rawData";

    @NotNull
    private String partitionKeyColumn = "partitionKey";

    public String getQueryIn() {
        return queryIn;
    }

    public void setQueryIn(String queryIn) {
        this.queryIn = queryIn;
    }

    public Boolean getHasOverwriteFlag() {
        return hasOverwriteFlag;
    }

    public void setHasOverwriteFlag(Boolean hasOverwriteFlag) {
        this.hasOverwriteFlag = hasOverwriteFlag;
    }

    public Boolean getHasReplayFlag() {
        return hasReplayFlag;
    }

    public void setHasReplayFlag(Boolean hasReplayFlag) {
        this.hasReplayFlag = hasReplayFlag;
    }

    public String getStreamOut() {
        return streamOut;
    }

    public void setStreamOut(String streamOut) {
        this.streamOut = streamOut;
    }

    public String getRawDataColumn() {
        return rawDataColumn;
    }

    public void setRawDataColumn(String rawDataColumn) {
        this.rawDataColumn = rawDataColumn;
    }

    public String getPartitionKeyColumn() {
        return partitionKeyColumn;
    }

    public void setPartitionKeyColumn(String partitionKeyColumn) {
        this.partitionKeyColumn = partitionKeyColumn;
    }
}

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PumpSettings that = (PumpSettings) o;

        if (!queryIn.equals(that.queryIn)) return false;
        if (!hasOverwriteFlag.equals(that.hasOverwriteFlag)) return false;
        if (!hasReplayFlag.equals(that.hasReplayFlag)) return false;
        if (!streamOut.equals(that.streamOut)) return false;
        if (!rawDataColumn.equals(that.rawDataColumn)) return false;
        return partitionKeyColumn.equals(that.partitionKeyColumn);

    }

    @Override
    public int hashCode() {
        int result = queryIn.hashCode();
        result = 31 * result + hasOverwriteFlag.hashCode();
        result = 31 * result + hasReplayFlag.hashCode();
        result = 31 * result + streamOut.hashCode();
        result = 31 * result + rawDataColumn.hashCode();
        result = 31 * result + partitionKeyColumn.hashCode();
        return result;
    }
}

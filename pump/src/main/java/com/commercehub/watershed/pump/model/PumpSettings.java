package com.commercehub.watershed.pump.model;

public class PumpSettings {
    private String queryIn;
    private Boolean hasOverwriteFlag;
    private Boolean hasReplayFlag;
    private String streamOut;

    public PumpSettings(String queryIn, Boolean hasOverwriteFlag, Boolean hasReplayFlag, String streamOut) {
        this.queryIn = queryIn;
        this.hasOverwriteFlag = hasOverwriteFlag;
        this.hasReplayFlag = hasReplayFlag;
        this.streamOut = streamOut;
    }

    public String getQueryIn() {
        return queryIn;
    }

    public Boolean hasOverwriteFlag() {
        return hasOverwriteFlag;
    }

    public Boolean hasReplayFlag() {
        return hasReplayFlag;
    }

    public String getStreamOut() {
        return streamOut;
    }
}

package com.commercehub.watershed.pump.model;

public class PreviewSettings {
    private String queryIn;
    private Integer previewCount;

    public PreviewSettings(String queryIn, Integer previewCount) {
        this.queryIn = queryIn;
        this.previewCount = previewCount;
    }

    public String getQueryIn() {
        return queryIn;
    }

    public Integer getPreviewCount() {
        return previewCount;
    }
}

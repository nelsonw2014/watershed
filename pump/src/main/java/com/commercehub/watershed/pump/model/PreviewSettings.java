package com.commercehub.watershed.pump.model;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class PreviewSettings {
    @NotNull
    private String queryIn;

    @NotNull
    @Min(0)
    private Integer previewCount;

    public String getQueryIn() {
        return queryIn;
    }

    public void setQueryIn(String queryIn) {
        this.queryIn = queryIn;
    }

    public Integer getPreviewCount() {
        return previewCount;
    }

    public void setPreviewCount(Integer previewCount) {
        this.previewCount = previewCount;
    }
}

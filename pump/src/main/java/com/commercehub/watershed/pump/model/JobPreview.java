package com.commercehub.watershed.pump.model;

import java.util.List;
import java.util.Map;

public class JobPreview {
    private Integer count;
    private List<Map<String, String>> rows;

    public JobPreview(Integer count, List<Map<String, String>> rows) {
        this.count = count;
        this.rows = rows;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public List<Map<String, String>> getRows() {
        return rows;
    }

    public void setRows(List<Map<String, String>> rows) {
        this.rows = rows;
    }
}

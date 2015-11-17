package com.commercehub.watershed.pump.model;

import java.util.List;
import java.util.Map;

public class JobPreview {
    private Integer count;
    private List<Map<String, Object>> rows;

    public JobPreview(Integer count, List<Map<String, Object>> rows) {
        this.count = count;
        this.rows = rows;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }

    public void setRows(List<Map<String, Object>> rows) {
        this.rows = rows;
    }
}

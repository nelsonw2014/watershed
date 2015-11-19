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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JobPreview that = (JobPreview) o;

        if (!count.equals(that.count)) return false;
        return rows.equals(that.rows);

    }

    @Override
    public int hashCode() {
        int result = count.hashCode();
        result = 31 * result + rows.hashCode();
        return result;
    }
}
